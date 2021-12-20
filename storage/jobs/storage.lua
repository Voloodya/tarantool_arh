local clock = require('clock')
local checks = require('checks')
local cartridge = require('cartridge')
local task_statuses = require('tasks.statuses')
local tenant = require('common.tenant')

local FIELDS = {
    { name = 'id',          type = 'string' },
    { name = 'name',        type = 'string' },
    { name = 'args',        type = 'array'  },
    { name = 'context',     type = 'map'    },
    { name = 'added',       type = 'number' },
    { name = 'runner',      type = 'string',    is_nullable = true },
    { name = 'status',      type = 'number' },
    { name = 'result',      type = 'any',    is_nullable = true },
}

local BASE_SPACE_NAME = 'tdg_job_list'

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function init()
    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        box.space[space_name]:format(FIELDS, { if_not_exists = true })
        return box.space[space_name]
    end

    box.begin()
    local space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format(FIELDS, { if_not_exists = true })

    space:create_index('id',
        { parts = {{ 'id', 'string' }}, type = 'HASH', unique = true, if_not_exists = true })
    space:create_index('added',
        { parts = {{ 'added', 'number' }}, type = 'TREE', if_not_exists = true })
    box.commit()
    return space
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    assert(space, 'jobs storage is not initialized')
    return space
end

local function save(id, name, args, context)
    local space = get_space()
    space:replace({ id,
                    name,
                    args,
                    context,
                    clock.time64(),
                    box.NULL,
                    task_statuses.DID_NOT_START,
                    box.NULL })
    return true
end

local function delete(id)
    local space = get_space()
    space:delete(id)
end

local function get(id)
    local space = get_space()
    return space:get(id)
end

local function get_iter()
    local space = get_space()
    return space.index.added:pairs()
end

local function set_runner(id, url)
    local space = get_space()
    space:update(id, { { '=', 'runner', url or box.NULL } })
    return true
end

local function set_result(id, status, result)
    local space = get_space()
    return space:update(id, {
        { '=', 'status', status },
        { '=', 'result', result }
    })
end

local function push_result(uri, id, status, result)
    checks('string', 'string', 'number', '?')
    return cartridge.rpc_call('storage', 'set_job_result',
        { id, status, result },
        { uri = uri })
end

return {
    init = init,
    save = save,
    delete = delete,
    get_iter = get_iter,
    get = get,
    set_runner = set_runner,
    push_result = push_result,
    set_result = set_result
}
