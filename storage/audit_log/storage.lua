local module_name = 'storage.audit.storage'

local checks = require('checks')
local system_log = require('log')
local audit_log = require('audit.log').new(module_name)

local clock = require('clock')
local digest = require('digest')
local fiber = require('fiber')
local buffer = require('buffer')
local uuid = require('uuid')
local msgpack = require('msgpack')
local merger_lib = require('merger')
local key_def_lib = require('key_def')
local utils = require('common.utils')

local cartridge = require('cartridge')
local errors = require('errors')

local defaults = require('common.defaults')
local tenant = require('common.tenant')
local vshard_utils = require('common.vshard_utils')

local vshard_error = errors.new_class('vshard call error')

local vars = require('common.vars').new(module_name)
local config_filter = require('common.config_filter')

vars:new('remove_older_nanoseconds')

local NANOSECONDS_IN_SECOND = 1e9
local NANOSECONDS_IN_HOUR = 3600 * NANOSECONDS_IN_SECOND
local DEFAULT_LIMIT = 25

local BASE_SPACE_NAME = 'tdg_audit_log'
local BASE_SEQUENCE_NAME = 'tdg_audit_log_order'

local FIELDS = {
    { name = 'timestamp',   type = 'unsigned' },
    { name = 'request_id',  type = 'uuid' },
    { name = 'severity',    type = 'number' },
    { name = 'subject',     type = 'string' },
    { name = 'subject_id',  type = 'string', is_nullable = true },
    { name = 'module',      type = 'string' },
    { name = 'message',     type = 'string' },
    { name = 'order',       type = 'unsigned' },
    { name = 'bucket_id',   type = 'unsigned' },
}

local TIMESTAMP_FIELD = 1
local REQUEST_ID_FIELD = 2
local SEVERITY_FIELD = 3
local SUBJECT_FIELD = 4
local SUBJECT_ID_FIELD = 5
local MODULE_FIELD = 6
local MESSAGE_FIELD = 7
local ORDER_FIELD = 8

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_sequence_name()
    return tenant.get_sequence_name(BASE_SEQUENCE_NAME)
end

local function init()
    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        return
    end

    system_log.info('Initialization of audit log storage')

    local sequence_name = get_sequence_name()
    box.begin()
    local space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format(FIELDS)

    box.schema.sequence.create(sequence_name, {if_not_exists = true})

    space:create_index('by_time', {
        parts = {{field = 'timestamp'}, {field = 'order'}},
        type = 'TREE',
        unique = true,
        if_not_exists = true,
        sequence = {id = sequence_name, field = 'order'},
    })

    space:create_index('bucket_id', {
        parts = {{field = 'bucket_id'}},
        type = 'TREE',
        unique = false,
        if_not_exists = true,
    })

    box.commit()
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    assert(space, ('audit log storage %q is not initialized'):format(space_name))
    return space
end

local function cleanup()
    local space = get_space()
    local n = 0

    local remove_before = clock.time64() - vars.remove_older_nanoseconds

    box.begin()

    for _, entry in space.index.by_time:pairs(
        { remove_before, nil }, { iterator = box.index.LE }) do

        space:delete({ entry.timestamp, entry.order })
        n = n + 1
    end

    box.commit()

    if n > 0 then
        system_log.info('Deleted %d audit log entries', n)
    end
end

local function save(bucket, entries)
    local space = get_space()

    box.begin()
    for _, entry in ipairs(entries) do
        if entry[REQUEST_ID_FIELD] ~= box.NULL then
            entry[REQUEST_ID_FIELD] = uuid.fromstr(entry[REQUEST_ID_FIELD])
        end

        table.insert(entry, box.NULL)
        table.insert(entry, bucket)

        space:replace(entry)
    end
    box.commit()

    if vars.remove_older_nanoseconds ~= nil then
        cleanup()
    end

    return true
end

local function encode_cursor(timestamp)
    return digest.base64_encode(tostring(timestamp))
end

local function decode_cursor(cursor)
    local timestamp = digest.base64_decode(cursor)
    return tonumber64(timestamp)
end

local function to_map(entry)
    return {
        time = entry[TIMESTAMP_FIELD],
        request_id = entry[REQUEST_ID_FIELD],
        severity = entry[SEVERITY_FIELD],
        subject = entry[SUBJECT_FIELD],
        subject_id = entry[SUBJECT_ID_FIELD],
        module = entry[MODULE_FIELD],
        message = entry[MESSAGE_FIELD],
        cursor = encode_cursor(entry[TIMESTAMP_FIELD])
    }
end

local function filter_impl(options)
    local space = get_space()

    local after
    if options.cursor ~= nil then
        after = decode_cursor(options.cursor)
    end

    local limit = DEFAULT_LIMIT
    local iter = 'GT'
    if options.limit ~= nil then
        limit = options.limit
        if limit < 0 then
            iter = 'LT'
            limit = -limit
        end
    end

    local request_id
    if options.request_id ~= nil then
        request_id = uuid.fromstr(options.request_id)
        if request_id == nil then
            -- Incorrect UUID -> no data will be returned
            return {}
        end
    end

    local n = 0
    local res = {}
    for _, entry in space.index.by_time:pairs(after, iter) do
        if n >= limit then
            break
        end

        repeat
            if options.subject ~= nil and entry[SUBJECT_FIELD] ~= options.subject then
                break
            end

            if options.module ~= nil and entry[MODULE_FIELD] ~= options.module then
                break
            end

            if request_id ~= nil and (entry.request_id == nil or entry.request_id ~= request_id) then
                break
            end

            if options.subject_id ~= nil and entry[SUBJECT_ID_FIELD] ~= options.subject_id then
                break
            end

            if options.severity ~= nil and entry[SEVERITY_FIELD] < options.severity then
                break
            end

            if options.from ~= nil and entry[TIMESTAMP_FIELD] < options.from then
                break
            end

            if options.to ~= nil and entry[TIMESTAMP_FIELD] > options.to then
                break
            end

            if options.text ~= nil and not string.match(entry[MESSAGE_FIELD], options.text) then
                break
            end

            n = n + 1
            table.insert(res, entry)
        until true
    end

    return res
end

local function validate_config(cfg) -- luacheck: ignore
    -- checks in audit/log.lua
    return true
end

local function apply_config(cfg)
    system_log.info('Apply config to audit log storage')

    config_filter.compare_and_set(cfg, 'audit_log', module_name)
    local conf = cfg.audit_log

    if conf == nil then
        return true
    end

    if conf.remove_older_than_n_hours == nil then
        vars.remove_older_nanoseconds = nil
    else
        vars.remove_older_nanoseconds =
            tonumber64(conf.remove_older_than_n_hours * NANOSECONDS_IN_HOUR)
    end

    return true
end

local audit_log_key_def = key_def_lib.new({
    {field = TIMESTAMP_FIELD, type = 'unsigned'},
    {field = ORDER_FIELD, type = 'unsigned'},
})

local function filter(options)
    options = options or {}

    checks({
        cursor = '?string',
        limit = '?number',
        from = '?number|cdata',
        to = '?number|cdata',
        subject = '?string',
        module = '?string',
        request_id = '?string',
        subject_id = '?string',
        severity = '?number',
        text = '?string',
    })

    local limit = DEFAULT_LIMIT
    local reverse = false
    if options.limit ~= nil then
        limit = options.limit
        if limit < 0 then
            limit = -limit
            reverse = true
        end
    end

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end
    local replicaset, err = vshard.router.routeall()
    if not replicaset then
        return nil, err
    end

    local replica_num = 0

    local results = fiber.channel()

    local timeout =
        cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT

    for _, replica in pairs(replicaset) do
        replica_num = replica_num + 1

        local buf = buffer.ibuf()
        local w = tenant.fiber_new(
            function ()
                local _, err = vshard_error:pcall(replica.callro, replica,
                    'vshard_audit_log.filter_impl',
                    { options },
                    { timeout = timeout, buffer = buf, skip_header = true })

                results:put({ buf = buf, err = err })
            end)

        w:name('get_audit_log')
    end

    if replica_num == 0 then
        return {}
    end

    local sources = {}
    for _ = 1, replica_num do
        local res = results:get()
        if res.err ~= nil then
            return nil, res.err
        end

        local buf = res.buf

        local len
        len, buf.rpos = msgpack.decode_array_header(buf.rpos, buf:size())
        if len == 2 then -- len([nil, err]) == 2
            local _, err_rpos = msgpack.decode_unchecked(buf.rpos)
            local err = msgpack.decode_unchecked(err_rpos)
            return nil, err
        end
        table.insert(sources, merger_lib.new_source_frombuffer(buf))
    end

    local merger = merger_lib.new(audit_log_key_def, sources, {reverse = reverse})

    local entries = {}

    local count = 0
    for _, tuple in merger:pairs() do
        if count >= limit then
            break
        end
        table.insert(entries, to_map(tuple))
        count = count + 1
    end

    if reverse then
        utils.reverse_table(entries)
    end

    return entries
end

local function clear_impl()
    local space = get_space()
    space:truncate()
    return true
end

local function clear()
    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local _, err = vshard.router.map_callrw('vshard_audit_log.clear_impl', {}, {timeout = timeout})
    if err ~= nil then
        return nil, err
    end

    system_log.warn('Audit log spaces were truncated')
    audit_log.alarm('Audit log spaces were truncated')

    return true
end

_G.vshard_audit_log = {
    save = save,
    filter_impl = filter_impl,
    clear_impl = clear_impl,
}

return {
    init = init,

    validate_config = validate_config,
    apply_config = apply_config,

    filter = filter,
    clear = clear,
}
