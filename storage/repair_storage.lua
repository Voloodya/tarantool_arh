local cartridge = require('cartridge')
local clock = require('clock')
local defaults = require('common.defaults')
local digest = require('digest')
local errors = require('errors')
local fiber = require('fiber')
local msgpack = require('msgpack')
local buffer = require('buffer')
local key_def_lib = require('key_def')
local merger_lib = require('merger')
local checks = require('checks')
local tenant = require('common.tenant')
local vshard_utils = require('common.vshard_utils')
local vshard_error = errors.new_class('vshard call error')

local FIELDS = {
    { name = 'id',          type = 'string' },
    { name = 'time',        type = 'number' },
    { name = 'status',      type = 'number' },
    { name = 'object',      type = 'map'    },
    { name = 'reason',      type = 'string' },
    { name = 'context',     type = 'map'    },
    { name = 'bucket_id',   type = 'number' },
}

local ID_FIELD      = 1
local TIME_FIELD    = 2
local STATUS_FIELD = 3
local OBJECT_FIELD = 4
local REASON_FIELD = 5

local TIMEOUT_SEC = 10

local repair_storage_error = errors.new_class("repair_storage_error")

local repair_storage = {}

local function get_space_name(base_name)
    return tenant.get_space_name(base_name)
end

local function get_space(base_name)
    local space_name = get_space_name(base_name)

    local space = box.space[space_name]
    assert(space, 'repair storage is not initialized')
    return space
end

local function get_bucket_id(key)
    checks("?string")
    local c = digest.murmur(key)
    return digest.guava(c, vshard.router.bucket_count() - 1) + 1
end

local function save_impl(base_name, id, time, status, object, reason, context, bucket_id)
    local space = get_space(base_name)
    space:replace({ id, time, status, object, reason, context, bucket_id })
    return true
end

function repair_storage:save(object, status, reason, context, time)
    checks('table', 'table', 'number', 'string', 'table', '?number|cdata')

    if time == nil then
        time = clock.time64()
    end

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local bucket_id = get_bucket_id(context.id)

    local _, err = vshard.router.call(bucket_id, 'write', 'vshard_repair.save_impl',
        { self.base_name, context.id, time, status, object, reason, context, bucket_id })

    if err ~= nil then
        return nil, err
    end

    return time
end

local function unflat(flat)
    local obj = {}
    for i, val in ipairs(flat) do
        obj[FIELDS[i].name] = val
    end
    return obj
end

local function decode_cursor(cursor)
    checks("?string")
    if cursor == nil then
        return nil
    end

    local ok, raw = pcall(digest.base64_decode, cursor)

    if not ok then
        return nil, repair_storage_error:new(
                "Failed to decode cursor: '%s'", tostring(cursor))
    end

    local ok, decoded = pcall(msgpack.decode, raw)

    if not ok then
        return nil, repair_storage_error:new(
                "Failed to decode cursor: '%s'", tostring(cursor))
    end

    if type(decoded) ~= 'table' then
        return nil, repair_storage_error:new(
                "Failed to decode cursor: '%s'", tostring(cursor))
    end

    return decoded
end

local function encode_cursor(cursor)
    checks("?table")
    if cursor == nil then
        return nil
    end

    local raw = msgpack.encode(cursor)
    local encoded = digest.base64_encode(raw)

    return encoded
end

local function extract_cursor(object)
    return {object[TIME_FIELD], object[ID_FIELD]}
end

local function get_impl(base_name, id)
    local space = get_space(base_name)
    return space:get(id)
end

function repair_storage:get(id)
    checks('table', 'string')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local bucket_id = get_bucket_id(id)

    local flat, err = vshard.router.call(
        bucket_id, 'read', 'vshard_repair.get_impl', { self.base_name, id })

    if err ~= nil then
        return nil, err
    end

    if flat == nil then
        return nil
    end

    local obj = unflat(flat)
    obj.cursor = encode_cursor(extract_cursor(flat))
    return obj
end

local function update_status_impl(base_name, id, status, reason)
    local space = get_space(base_name)

    local update_list = {{'=', STATUS_FIELD, status}}
    if reason ~= nil then
        table.insert(update_list, {'=', REASON_FIELD, reason})
    end
    space:update(id, update_list)
    return true
end

function repair_storage:update_status(id, status, reason)
    checks('table', 'string', 'number', '?string')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local bucket_id = get_bucket_id(id)

    local res, err = vshard.router.call(
        bucket_id, 'write', 'vshard_repair.update_status_impl',
        { self.base_name, id, status, reason })

    if err ~= nil then
        return nil, err
    end

    return res
end

local function update_object_impl(base_name, id, object, status, reason)
    local space = get_space(base_name)
    space:update(id, {
        { '=', STATUS_FIELD, status },
        { '=', OBJECT_FIELD, object },
        { '=', REASON_FIELD, reason }})
    return true
end

function repair_storage:update_object(id, object, status, reason)
    checks('table', 'string', 'table', 'number', 'string')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local bucket_id = get_bucket_id(id)

    local res, err = vshard.router.call(
        bucket_id, 'write', 'vshard_repair.update_object_impl',
        { self.base_name, id, object, status, reason })

    if err ~= nil then
        return nil, err
    end

    return res
end

local function update_reason_impl(base_name, id, reason, status)
    local space = get_space(base_name)

    local update = {
        { '=', STATUS_FIELD, status },
        { '=', REASON_FIELD, reason }}

    space:update(id, update)
    return true
end

function repair_storage:update_reason(id, reason, status)
    checks('table', 'string', 'string', 'number')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local bucket_id = get_bucket_id(id)

    local res, err = vshard.router.call(
        bucket_id, 'write', 'vshard_repair.update_reason_impl',
        { self.base_name, id, reason, status })

    if err ~= nil then
        return nil, err
    end

    return res
end

local function delete_impl(base_name, id)
    local space = get_space(base_name)
    space:delete(id)
    return true
end

function repair_storage:delete(id)
    checks('table', 'string')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local bucket_id = get_bucket_id(id)

    local res, err = vshard.router.call(
        bucket_id, 'write', 'vshard_repair.delete_impl',
        { self.base_name, id })

    if err ~= nil then
        return nil, err
    end

    return res
end

local function clear_impl(base_name)
    local space = get_space(base_name)
    space:truncate()
    return true
end

function repair_storage:clear()
    checks('table')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local _, err = vshard.router.map_callrw('vshard_repair.clear_impl', {self.base_name}, {timeout = timeout})
    if err ~= nil then
        return nil, err
    end

    return true
end

local function filter_impl(base_name, from, to, reason, first, after)
    local space = get_space(base_name)

    local scan_key, iterator

    if after ~= nil then
        local err
        scan_key, err = decode_cursor(after)
        if err ~= nil then
            return nil, err
        end
        iterator = box.index.GT
    elseif from ~= nil then
        scan_key = {from}
        iterator = box.index.GE
    else
        scan_key = box.NULL
        iterator = box.index.GE
    end

    local result = {}
    local count = 0
    for _, tuple in space.index.by_time:pairs(scan_key, {iterator = iterator}) do
        if to ~= nil and tuple.time > to then
            break
        end

        if reason == nil or reason ~= nil and string.match(tuple.reason, reason) then
            table.insert(result, tuple)
            count = count + 1
        end

        if count >= first then
            break
        end
    end

    return result
end

local repair_key_def = key_def_lib.new({
    {field = TIME_FIELD, type = 'number'},
    {field = ID_FIELD, type = 'string'},
})

function repair_storage:filter(from, to, reason, first, after)
    checks('table', '?cdata', '?cdata', '?string', 'number', '?string')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, vshard_error:new("Cluster isn't bootstrapped yet")
    end

    local replicaset, err = vshard.router.routeall()
    if not replicaset then
        return nil, err
    end

    local total = 0
    for _, _ in pairs(replicaset) do
        total = total + 1
    end

    if total == 0 then
        return {}
    end

    local results = fiber.channel(total)

    for _, replica in pairs(replicaset) do
        local buf = buffer.ibuf()
        tenant.fiber_new(
            function ()
                local _, err = replica:callro(
                    'vshard_repair.filter_impl',
                    { self.base_name, from, to, reason, first, after },
                    { buffer = buf, skip_header = true })
                results:put({ buf = buf, err = err })
            end)
    end

    local sources = {}

    for _ = 1, total do
        local res = results:get(TIMEOUT_SEC)

        if not res then
            results:close()
            return nil, repair_storage_error:new("Timeout expired")
        end

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

    local merger = merger_lib.new(repair_key_def, sources, {reverse = false})

    local result = {}
    local count = 0
    for _, tuple in merger:pairs() do
        if first ~= nil and count >= first then
            break
        end

        local obj = unflat(tuple)
        obj.cursor = encode_cursor(extract_cursor(tuple))
        table.insert(result, obj)
        count = count + 1
    end

    return result
end

function repair_storage:init()
    checks('table')
    if box.info.ro then
        return
    end

    local space_name = get_space_name(self.base_name)

    if box.space[space_name] ~= nil then
        return
    end

    box.begin()
    local space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format(FIELDS, { if_not_exists = true })

    space:create_index('id', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {
            {field = 'id', type = 'string'},
        },
    })

    space:create_index('by_time', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {
            {field = 'time', type = 'number'},
            {field = 'id', type = 'string'},
        },
    })

    space:create_index('bucket_id', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {
            {field = 'bucket_id', type = 'number'},
        },
    })

    box.commit()
end

function repair_storage.new(base_name)
    checks("?string")

    local instance = {
        base_name = base_name,

        init = repair_storage.init,
        save = repair_storage.save,
        get = repair_storage.get,
        update_status = repair_storage.update_status,
        update_object = repair_storage.update_object,
        update_reason = repair_storage.update_reason,
        delete = repair_storage.delete,
        clear = repair_storage.clear,
        filter = repair_storage.filter
    }

    return instance
end

_G.vshard_repair = {
    save_impl = save_impl,
    get_impl = get_impl,
    update_status_impl = update_status_impl,
    update_object_impl = update_object_impl,
    update_reason_impl = update_reason_impl,
    delete_impl = delete_impl,
    clear_impl = clear_impl,
    filter_impl = filter_impl
}

return {
    new = repair_storage.new
}
