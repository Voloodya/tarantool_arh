local clock = require('clock')
local digest = require('digest')
local tenant = require('common.tenant')

local FIELDS = {
    { name = 'id',          type = 'string' },
    { name = 'finished',    type = 'number' },
    { name = 'status',      type = 'number' },
    { name = 'result',      type = 'string' },
    { name = 'bucket_id',   type = 'number' },
}

local BASE_SPACE_NAME = 'tdg_task_list'

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function init()
    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        return
    end

    box.begin()
    local space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format(FIELDS, { if_not_exists = true })

    space:create_index('id',
        { parts = {{field = 'id', type = 'string' }}, type = 'HASH', unique = true, if_not_exists = true })
    space:create_index('bucket_id',
        { parts = {{field = 'bucket_id', type = 'number' }}, type = 'TREE', unique = false, if_not_exists = true})

    box.schema.func.create('task_storage.set_result_impl', { if_not_exists = true })
    box.schema.func.create('task_storage.delete_impl', { if_not_exists = true })
    box.schema.func.create('task_storage.get_impl', { if_not_exists = true })
    box.commit()
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    assert(space, 'task storage is not initialized')
    return space
end

local function get_bucket_id(id)
    checks('string')
    local c = digest.murmur(id)
    return digest.guava(c, vshard.router.bucket_count() - 1) + 1
end

local function set_result_impl(id, status, result, bucket_id)
    local space = get_space()
    local finished = clock.time64()
    return space:replace({ id, finished, status, result, bucket_id })
end

local function set_result(id, status, result)
    checks('string', 'number', 'string')
    local bucket_id = get_bucket_id(id)
    return vshard.router.call(
        bucket_id, 'write', 'vshard_tasks.set_result_impl', { id, status, result, bucket_id })
end

local function delete_impl(id)
    local space = get_space()
    space:delete(id)
    return true
end

local function delete(id)
    checks('string')
    local bucket_id = get_bucket_id(id)
    return vshard.router.call(bucket_id, 'write', 'vshard_tasks.delete_impl', { id })
end

local function get_impl(id)
    local space = get_space()
    return space:get(id)
end

local function unflat(flat)
    local obj = {}
    for i, val in ipairs(flat) do
        obj[FIELDS[i].name] = val
    end
    return obj
end

local function get(id)
    checks('string')
    local bucket_id = get_bucket_id(id)

    local flat = vshard.router.call(bucket_id, 'read', 'vshard_tasks.get_impl', { id })
    if flat == nil then
        return nil
    end

    return unflat(flat)
end

_G.vshard_tasks = {
    set_result_impl = set_result_impl,
    delete_impl = delete_impl,
    get_impl = get_impl
}

return {
    init = init,

    set_result = set_result,
    delete = delete,
    get = get
}
