local account = require('common.admin.account')

local SPACE_NAME = 'tdg_account_settings'
local ANONYMOUS_UID = 'anonymous'
local MAX_SIZE = 1024 * 100

local function get_space()
    local space = box.space[SPACE_NAME]
    assert(space, 'account settings storage is not initialized')
    return space
end

local function apply_config()
    if box.info.ro then
        return
    end

    if box.space[SPACE_NAME] ~= nil then
        return
    end

    box.begin()

    local space = box.schema.space.create(SPACE_NAME, { if_not_exists = true })
    space:format({
        { name = 'account_id', type = 'string' },
        { name = 'key', type = 'string' },
        { name = 'value', type = 'any' },
    })
    space:create_index('id', {
        parts = {
            { field = 'account_id', type = 'string' },
            { field = 'key', type = 'string' },
        },
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })

    box.commit()
end

local function space_call(func_name, ...)
    local space = get_space()
    local tuple = space[func_name](space, ...)
    if tuple == nil then
        return nil
    end
    return tuple.value
end

local function get_account_id()
    return account.id() or ANONYMOUS_UID
end

local function get_data_size(account_id)
    local space = get_space()
    local size = 0
    for _, tuple in space:pairs(account_id) do
        size = size + tuple:bsize()
    end
    return size
end

local function get(key)
    local account_id = get_account_id()
    return space_call('get', { account_id, key })
end

local function put(key, value)
    local account_id = get_account_id()
    local tuple = box.tuple.new({ account_id, key, value })

    local total_size = get_data_size(account_id) + tuple:bsize()
    if total_size > MAX_SIZE then
        return error((
            "Impossible to put data. Total size of settings data is %s KB. It should be less than %s KB."
        ):format(
            math.floor(total_size / 1024), math.floor(MAX_SIZE / 1024)
        ))
    end

    return space_call('replace', tuple)
end

local function delete(key)
    local account_id = get_account_id()
    return space_call('delete', { account_id, key })
end

return {
    apply_config = apply_config,

    get = get,
    put = put,
    delete = delete,
}
