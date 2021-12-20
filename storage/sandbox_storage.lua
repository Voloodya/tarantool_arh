local module_name = 'storage.sandbox_storage' -- luacheck: ignore

local fiber = require('fiber')
local cartridge_pool = require('cartridge.pool')
local tenant = require('common.tenant')

local BASE_SPACE_NAME = 'tdg_sandbox_storage'

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function add_to_subscribers(subscribers, uri)
    subscribers[uri] = true
    return subscribers
end

local function create_sandbox_storage_space()
    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        return box.space[space_name]
    end

    box.begin()
    local space = box.schema.space.create(space_name,
        { if_not_exists = true })

    space:format({
        { name = 'namespace', type = 'string' },
        { name = 'key', type = 'string' },
        { name = 'value', type = 'any', is_nullable = true },
        { name = 'generation', type = 'unsigned' },
        { name = 'subscribers', type = 'map' },
        { name = 'bucket_id', type = 'unsigned' },
    })

    space:create_index('id', {
        parts = {
            {field = 'namespace', type = 'string', is_nullable = false},
            {field = 'key', type = 'string', is_nullable = false},
        },
        type = 'HASH',
        unique = true,
        if_not_exists = true,
    })

    space:create_index('generation', {
        parts = {
            {field = 'namespace', type = 'string', is_nullable = false},
            {field = 'generation', type = 'unsigned', is_nullable = false},
        },
        type = 'TREE',
        unique = false,
        if_not_exists = true,
    })

    space:create_index('bucket_id', {
        parts = {
            {field = 'bucket_id', type = 'unsigned', is_nullable = false},
        },
        type = 'TREE',
        unique = false,
        if_not_exists = true,
    })
    box.commit()
    return space
end

local function get_space()
    local space_name = get_space_name()
    return box.space[space_name]
end

local function broadcast_update(namespace, key, source)
    local space = get_space()
    local tuple = space:get({namespace, key})
    local generation = tuple.generation or 0
    local subscribers = tuple.subscribers

    for uri, _ in pairs(subscribers) do
        if uri ~= source then
            local conn = cartridge_pool.connect(uri)
            local _, err = conn:call('sandbox_proxy.set', {namespace, key, generation})
            if err ~= nil then
                subscribers[uri] = nil
            end
        end
    end

    -- Ignore subscribers update if key's generation was changed
    tuple = space:get({namespace, key})
    if tuple.generation ~= generation then
        return true
    end
    space:update({namespace, key}, {{'=', 'subscribers', subscribers}})
    return true
end

local function get_generation(space)
    local tuple = space.index.generation:max()
    local generation = tuple and tuple.generation or 0
    return generation
end

_G.vshard_sandbox = {
    get = function(namespace, key, source)
        local space = get_space()

        local tuple = space:get({namespace, key})
        if tuple == nil then
            return {nil, 0}
        end

        local subscribers = add_to_subscribers(tuple.subscribers, source)
        space:update({namespace, key}, {{'=', 'subscribers', subscribers}})

        return {tuple.value, tuple.generation}
    end,
    set = function(namespace, key, value, source, bucket_id)
        local space = get_space()

        local generation = get_generation(space)
        generation = generation + 1ULL

        local tuple = space:get({namespace, key})

        local subscribers
        if tuple == nil then
            subscribers = { [source] = { last_update = fiber.time() } }
        else
            subscribers = add_to_subscribers(tuple.subscribers, source)
        end
        space:replace({namespace, key, value, generation, subscribers, bucket_id})

        tenant.fiber_new(broadcast_update, namespace, key, source)
        return {value, generation}
    end,
    sync = function(namespace, generation)
        local space = get_space()
        if space == nil then
            return {{}, 0}
        end

        local result = {}
        local max_generation = get_generation(space)
        for _, tuple in space.index.generation:pairs({namespace, generation}, {iterator = box.index.GT}) do
            table.insert(result, {tuple.key, tuple.generation})
        end
        return {result, max_generation}
    end,
}

local function init()
    if box.info.ro then
        return
    end

    local space = get_space()
    if space ~= nil then
        space:truncate()
    end
end

local function apply_config()
    if box.info.ro then
        return
    end

    create_sandbox_storage_space()

    box.schema.func.create('vshard_sandbox.get', {if_not_exists = true})
    box.schema.func.create('vshard_sandbox.set', {if_not_exists = true})
    box.schema.func.create('vshard_sandbox.sync', {if_not_exists = true})
end

return {
    init = init,
    apply_config = apply_config,
}
