local tenant = require('common.tenant')
local BASE_SPACE_NAME = 'tdg_blob_storage'

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    assert(space, 'blob storage is not initialized')
    return space
end

local function put_element(namespace, key, index, value, bucket_id)
    local space = get_space()
    space:replace({ namespace, key, index, value, bucket_id })
end

local function get_element(namespace, key, index)
    local space = get_space()
    local tuple = space:get({ namespace, key, index })
    if tuple == nil then
        return nil
    end
    return tuple.value
end

local function append_element(namespace, key, value, bucket_id)
    local space = get_space()

    box.begin()

    local index = 1
    local tuple = space.index.id:max({ namespace, key })
    if tuple ~= nil then
        index = tuple.index + 1
    end
    put_element(namespace, key, index, value, bucket_id)

    box.commit()
end

local function delete_element(namespace, key, index)
    local space = get_space()

    box.begin()

    local last_index = index
    for _, tuple in space:pairs({ namespace, key, index }, { iterator = box.index.GT }) do
        if tuple.key ~= key or tuple.namespace ~= namespace then
            break
        end
        last_index = tuple.index
        space:replace(tuple:update({ { '-', 'index', 1 } }))
    end
    space:delete({ namespace, key, last_index })

    box.commit()
end

local function put(namespace, key, value, bucket_id)
    put_element(namespace, key, 0, value, bucket_id)
end

local function get(namespace, key)
    return get_element(namespace, key, 0)
end

local function delete(namespace, key)
    local space = get_space()
    space:delete({ namespace, key, 0 })
end

_G.vshard_blob_storage = {
    put_element = put_element,
    append_element = append_element,
    get_element = get_element,
    delete_element = delete_element,

    put = put,
    get = get,
    delete = delete,
}

local function apply_config()
    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        return
    end

    box.begin()

    local space = box.schema.space.create(space_name, { engine = 'vinyl', if_not_exists = true })
    space:format({
        { name = 'namespace', type = 'string' },
        { name = 'key', type = 'string' },
        { name = 'index', type = 'unsigned' },
        { name = 'value', type = 'any' },
        { name = 'bucket_id', type = 'unsigned' },
    })
    space:create_index('id', {
        parts = {
            { field = 'namespace', type = 'string' },
            { field = 'key', type = 'string' },
            { field = 'index', type = 'unsigned' },
        },
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })
    space:create_index('bucket_id', {
        parts = { { field = 'bucket_id', type = 'number' } },
        type = 'TREE',
        unique = false,
        if_not_exists = true,
    })

    box.schema.func.create('vshard_blob_storage.put_element', { if_not_exists = true })
    box.schema.func.create('vshard_blob_storage.append_element', { if_not_exists = true })
    box.schema.func.create('vshard_blob_storage.get_element', { if_not_exists = true })
    box.schema.func.create('vshard_blob_storage.delete_element', { if_not_exists = true })

    box.schema.func.create('vshard_blob_storage.put', { if_not_exists = true })
    box.schema.func.create('vshard_blob_storage.get', { if_not_exists = true })
    box.schema.func.create('vshard_blob_storage.delete', { if_not_exists = true })

    box.commit()
end

return {
    apply_config = apply_config,
}
