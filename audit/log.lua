local module_name = 'audit_log'

local system_log = require('log')

local clock = require('clock')
local errors = require('errors')
local fiber = require('fiber')

local cartridge = require('cartridge')

local account = require('common.admin.account')
local defaults = require('common.defaults')
local bounded_queue = require('common.bounded_queue')
local request_context = require('common.request_context')
local tenant = require('common.tenant')
local vshard_utils = require('common.vshard_utils')

local config_error = errors.new_class('Invalid audit log config')
local vshard_error = errors.new_class("Vshard call failed")

local config_checks = require('common.config_checks').new(config_error)

local severities = require('audit.severities')
local config_filter = require('common.config_filter')

local QUEUE_SIZE = 256
local NANOSECONDS_IN_SECOND = 1e9

local BASE_SPACE_NAME = 'tdg_audit_log_repair'

local TIMESTAMP_FIELD = 1
local BUCKET_FIELD = 2
local ENTRIES_FIELD = 3

local vars = require('common.vars').new(module_name)

local DEFAULT_ENABLED = true
local DEFAULT_SEVERITY = severities.INFO

vars:new('enabled', DEFAULT_ENABLED)
vars:new('severity', DEFAULT_SEVERITY)
vars:new('to_send', nil)
vars:new('need_to_send_condition', nil)
vars:new('send_fiber', nil)
vars:new('send_time_sec', 1)
vars:new('resend_time_sec', 5)

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function init_space()
    if type(box.cfg) == 'function' then
        return
    end

    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    local space = box.space[space_name]
    if space ~= nil then
        return
    end

    box.begin()

    space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format({
        { name = 'timestamp', type = 'number' },
        { name = 'bucket', type = 'number' },
        { name = 'entries', type = 'array' },
    })

    space:create_index('timestamp', {
        parts = {{ field = 'timestamp' }, { field = 'bucket' }},
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })

    box.commit()
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    assert(space ~= nil)
    return space
end

local function get_bucket(entry)
    -- type(entry[TIMESTAMP_FIELD]) == cdata
    local sec = entry[TIMESTAMP_FIELD] / NANOSECONDS_IN_SECOND
    return vshard.router.bucket_id_strcrc32(sec)
end

local function send(bucket, entries)
    local timeout = cartridge.config_get_readonly('vshard-timeout')
        or defaults.VSHARD_TIMEOUT

    local res, err = vshard_error:pcall(vshard.router.call, bucket, 'write',
        'vshard_audit_log.save', { bucket, entries }, { timeout = timeout })

    if err ~= nil then
        system_log.error(err)
    end

    return res, err
end

local function resend_entries()
    local space = get_space()
    for _, tuple in space.index.timestamp:pairs() do
        local _, err = send(tuple[BUCKET_FIELD], tuple[ENTRIES_FIELD])
        if err == nil then
            space:delete({ tuple[TIMESTAMP_FIELD], tuple[BUCKET_FIELD] })
        else
            -- In case of some problems with network
            -- send may not yield.
            fiber.sleep(0.1)
        end
    end
end

local function send_entries()
    local buckets = {}

    while not vars.to_send:is_empty() do
        local entry = vars.to_send:pop()
        local n = get_bucket(entry)
        local bucket = buckets[n]
        if bucket == nil then
            buckets[n] = { entry }
        else
            table.insert(bucket, entry)
        end
    end

    local workers = {}

    for bucket, entries in pairs(buckets) do
        local w = tenant.fiber_new(function()
            local _, err = send(bucket, entries)
            if err ~= nil then
                local space = get_space()
                local timestamp = entries[1][TIMESTAMP_FIELD]
                local ok, err = pcall(space.put, space, { timestamp, bucket, entries })
                if not ok then
                    system_log.error(err)
                end
            end
        end)

        w:name('send_audit_to_' .. bucket)
        w:set_joinable(true)

        table.insert(workers, w)
    end

    for _, w in ipairs(workers) do
        w:join()
    end
end

local function init(send_time_sec, resend_time_sec)
    init_space()

    if send_time_sec ~= nil then
        vars.send_time_sec = send_time_sec
    end

    if send_time_sec ~= nil then
        vars.resend_time_sec = resend_time_sec
    end

    vars.to_send = vars.to_send or bounded_queue.new(QUEUE_SIZE)
    vars.need_to_send_condition = vars.need_to_send_condition or fiber.cond()

    if vars.send_fiber == nil or vars.send_fiber:status() == 'dead' then
        vars.send_fiber = tenant.fiber_new(function()
            while true do
                vars.need_to_send_condition:wait(vars.send_time_sec)
                if vshard_utils.vshard_is_bootstrapped() then
                    send_entries()
                end
            end
        end)
        vars.send_fiber:name('audit_log')
    end
    vars.need_to_send_condition:signal()

    if vars.resend_fiber == nil or vars.resend_fiber:status() == 'dead' then
        vars.resend_fiber = tenant.fiber_new(function()
            box.ctl.wait_rw()
            while true do
                while not vshard_utils.vshard_is_bootstrapped() do
                    fiber.sleep(vars.resend_time_sec)
                end
                if box.info.ro == false then
                    resend_entries()
                end
                fiber.sleep(vars.resend_time_sec)
            end
        end)
        vars.resend_fiber:name('audit_log_resend')
    end
end

local function say(output, module_name, severity, fmt, ...)
    local text = string.format(fmt, ...)

    local request_id
    if request_context.is_empty() then
        request_id = '*' .. fiber.self().id()
    else
        request_id = request_context.get().id
    end

    local account_id
    local subject
    if account.is_empty() then
        account_id = 'system'
        subject = 'system'
    else
        account_id = account.id()
        subject = account.tostring()
    end

    local entry = {
        clock.time64(),             -- timestamp
        request_id,                 -- request_id
        severity,                   -- severity
        subject,                    -- subject
        account_id,                 -- subject_id
        module_name,                -- module
        text                        -- message
    }

    vars.to_send:push(entry)
    if vars.to_send:is_full() then
        vars.need_to_send_condition:signal()
    end

    local to_log
    if request_id then
        to_log = string.format('[%s] A> subj: %q, msg: %q', request_id, subject, text)
    else
        to_log = string.format('A> subj: %q, msg: %q', subject, text)
    end

    if severity == severities.VERBOSE then
        output.verbose(to_log)
    elseif severity == severities.INFO then
        output.info(to_log)
    elseif severity == severities.WARN then
        output.warn(to_log)
    elseif severity == severities.ALARM then
        output.error(to_log)
    else
        assert(false, 'Invalid severity value ' .. severity)
    end
end

local function say_closure(output, module_name, severity)
    return function(fmt, ...)
        if vars.enabled and severity >= vars.severity then
            say(output, module_name, severity, fmt, ...)
        end
    end
end

local function new(module_name)
    local output = require('log')
    return {
        verbose = say_closure(output, module_name, severities.VERBOSE),
        info = say_closure(output, module_name, severities.INFO),
        warn = say_closure(output, module_name, severities.WARN),
        alarm = say_closure(output, module_name, severities.ALARM)
    }
end

local function validate_config(cfg)
    local conf = config_filter.compare_and_get(cfg, 'audit_log', module_name)
    if conf == nil then
        return true
    end

    config_checks:check_luatype('audit_log', conf, 'table')

    config_checks:check_optional_luatype('audit_log.enabled', conf.enabled, 'boolean')

    config_checks:check_optional_luatype('audit_log.severity', conf.severity, 'string')
    if conf.severity ~= nil then
        config_checks:assert(
            severities.is_valid_value(severities.from_string(conf.severity)),
            'invalid audit log severity %q', conf.severity)
    end

    config_checks:check_optional_luatype(
        'audit_log.remove_older_than_n_hours', conf.remove_older_than_n_hours, 'number')
    if conf.remove_older_than_n_hours ~= nil then
        config_checks:assert(conf.remove_older_than_n_hours > 0,
            'audit_log.remove_older_than_n_hours must be a positive number')
    end

    return true
end

local function apply_config(cfg)
    local _, err = config_filter.compare_and_set(cfg, 'audit_log', module_name)
    if err ~= nil then
        return true
    end

    local enabled = DEFAULT_ENABLED
    if cfg.audit_log ~= nil and cfg.audit_log.enabled ~= nil then
        enabled = cfg.audit_log.enabled
    end

    local severity = DEFAULT_SEVERITY
    if cfg.audit_log ~= nil and cfg.audit_log.severity ~= nil then
        severity = severities.from_string(cfg.audit_log.severity)
    end

    config_error:assert(severities.is_valid_value(severity))
    if severity ~= vars.severity then
        vars.severity = severity
        system_log.info('Audit log severity changed to %s', severities.to_string(severity))
    end

    if enabled ~= vars.enabled then
        vars.enabled = enabled
        if enabled then
            system_log.info('Audit log enabled')
        else
            system_log.warn('Audit log disabled')
        end
    end

    return true
end

return {
    apply_config = apply_config,
    validate_config = validate_config,
    init = init,
    new = new,
}
