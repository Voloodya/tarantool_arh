local module_name = 'common_log'

local system_log = require('log')

local vshard = require('vshard')
local clock = require('clock')
local errors = require('errors')
local fiber = require('fiber')

local cartridge = require('cartridge')

local defaults = require('common.defaults')
local tenant = require('common.tenant')
local bounded_queue = require('common.bounded_queue')
local request_context = require('common.request_context')
local vshard_utils = require('common.vshard_utils')

local severities = require('log.severities')
local log_config = require('log.config')

local vshard_error = errors.new_class("Vshard call failed")
local config_error = errors.new_class("common_log_config_error")

local QUEUE_SIZE = 256
local NANOSECONDS_IN_SECOND = 1e9

local BASE_SPACE_NAME = 'tdg_log_repair'

local TIMESTAMP_FIELD = 1
local BUCKET_FIELD = 2
local ENTRIES_FIELD = 3

local vars = require('common.vars').new(module_name)

vars:new('enabled', false)
vars:new('severity', severities.INFO)
vars:new('to_send', nil)
vars:new('need_to_send_condition', nil)
vars:new('send_fiber', nil)
vars:new('host', '')
vars:new('send_time_sec', 1)
vars:new('resend_time_sec', 5)

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function init_space()
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
    return vshard.router.bucket_id_mpcrc32(sec)
end

local function send(bucket, entries)
    local timeout = cartridge.config_get_readonly('vshard-timeout')
        or defaults.VSHARD_TIMEOUT

    local res, err = vshard.router.call(bucket, 'write', 'vshard_common_log.save',
        { bucket, entries }, { timeout = timeout })

    if err ~= nil then
        system_log.error(vshard_error:new(err))
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

        w:name('send_common_log_to_' .. bucket)
        w:set_joinable(true)

        table.insert(workers, w)
    end

    for _, w in ipairs(workers) do
        w:join()
    end
end

local function init(host, send_time_sec, resend_time_sec)
    vars.host = host or ''

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
        vars.send_fiber:name('common_log')
    end
    vars.need_to_send_condition:signal()

    if vars.resend_fiber == nil or vars.resend_fiber:status() == 'dead' then
        vars.resend_fiber = tenant.fiber_new(function()
            while not vshard_utils.vshard_is_bootstrapped() do
                fiber.sleep(vars.resend_time_sec)
            end

            box.ctl.wait_rw()
            while true do
                if box.info.ro == false then
                    resend_entries()
                end
                fiber.sleep(vars.resend_time_sec)
            end
        end)
        vars.resend_fiber:name('common_log_resend')
    end
end

local function say(output, module_name, severity, fmt, ...)
    local text = type(fmt) == 'string' and fmt:format(...) or tostring(fmt)

    local request_id = not request_context.is_empty() and request_context.get().id or nil

    local msg = request_id and ("[%s] %s"):format(request_id, text) or text

    if severity == severities.VERBOSE then
        output.verbose(msg)
    elseif severity == severities.INFO then
        output.info(msg)
    elseif severity == severities.WARNING then
        output.warn(msg)
    elseif severity == severities.ERROR then
        output.error(msg)
    elseif severity == severities.DEBUG then
        output.debug(msg)
    else
        assert(false, 'Invalid severity value ' .. severity)
    end

    if not vars.enabled then
        return
    end

    local entry = {
        clock.time64(),             -- timestamp
        request_id,                 -- request_id
        severity,                   -- severity
        module_name,                -- module
        vars.host,                  -- host name
        text                        -- message
    }

    vars.to_send:push(entry)
    if vars.to_send:is_full() then
        vars.need_to_send_condition:signal()
    end
end

local function say_closure(output, module_name, severity)
    return function(fmt, ...)
        if severity <= vars.severity then
            say(output, module_name, severity, fmt, ...)
        end
    end
end

local function new(module_name)
    local output = system_log
    return {
        error = say_closure(output, module_name, severities.ERROR),
        warn = say_closure(output, module_name, severities.WARNING),
        info = say_closure(output, module_name, severities.INFO),
        verbose = say_closure(output, module_name, severities.VERBOSE),
        debug = say_closure(output, module_name, severities.DEBUG),
    }
end

local function apply_config(cfg)
    init_space()

    cfg = cfg['logger'] ~= box.NULL and cfg['logger'] or {}

    local toggle = vars.enabled ~= cfg.enabled
    if toggle then
        if cfg.enabled == box.NULL then
            vars.enabled = false -- disabled by default
        else
            config_error:assert(type(cfg.enabled) == 'boolean', 'logger.enabled must be a boolean value')
            vars.enabled = cfg.enabled
        end

        if vars.enabled then
            system_log.info('Logging enabled')
        else
            system_log.info('Logging disabled')
        end
    end

    local severity = cfg.severity and severities.from_string(cfg.severity) or severities.INFO
    if severities.is_valid_value(severity) then
        vars.severity = severity
    end

    return true
end

return {
    apply_config = apply_config,
    validate_config = log_config.validate,
    init = init,
    new = new,
}
