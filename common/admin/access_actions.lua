local graphql = require('common.graphql')
local types = require('graphql.types')
local account_provider = require('account_provider.account_provider')

local permissions_schema = types.object({
    name = 'AccessActionAggregatePermissions',
    description = 'Aggregate permissions',
    fields = {
        name = types.string.nonNull,
        read = types.boolean.nonNull,
        write = types.boolean.nonNull,
    },
    schema = 'admin',
})

local access_action_schema = types.object({
    name = 'DataAccessAction',
    description = 'Access action model',
    fields = {
        id = types.string.nonNull,
        description = types.string.nonNull,
        aggregates = types.list(permissions_schema),
    },
    schema = 'admin',
})

local permission_input = types.inputObject({
    name = 'AccessActionAggregatePermissionsInput',
    description = 'Aggregate',
    fields = {
        name = types.string.nonNull,
        read = types.boolean.nonNull,
        write = types.boolean.nonNull,
    },
    schema = 'admin',
})

local function create(_, args)
    local description = args['description']
    local aggregates = args['aggregates']
    local data, err = account_provider.data_action_create(description, aggregates)
    if err ~= nil then
        return nil, err
    end
    return data
end

local function update(_, args)
    local id = args['id']
    local description = args['description']
    local aggregates = args['aggregates']
    local data, err = account_provider.data_action_update(id, description, aggregates)
    if err ~= nil then
        return nil, err
    end
    return data
end

local function delete(_, args)
    local id = args['id']
    local data, err = account_provider.data_action_delete(id)
    if err ~= nil then
        return nil, err
    end
    return data
end

local function get(_, args)
    local id = args['id']
    local data, err = account_provider.data_action_get(id)
    if err ~= nil then
        return nil, err
    end
    return data
end

local function list(_, _)
    local data, err = account_provider.data_action_list()
    if err ~= nil then
        return nil, err
    end
    return data
end

local function init()
    graphql.add_callback_prefix('admin', 'data_access_action', 'Data access actions management')
    graphql.add_mutation_prefix('admin', 'data_access_action', 'Data access actions management')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'data_access_action',
        name = 'get',
        callback = 'common.admin.access_actions.get',
        kind = access_action_schema,
        args = {
            id = types.string.nonNull,
        },
        doc = "Get an access action"
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'data_access_action',
        name = 'list',
        callback = 'common.admin.access_actions.list',
        kind = types.list(access_action_schema).nonNull,
        args = {},
        doc = "Get an access action list"
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'data_access_action',
        name = 'create',
        callback = 'common.admin.access_actions.create',
        kind = access_action_schema,
        args = {
            description = types.string.nonNull,
            aggregates = types.list(permission_input).nonNull,
        },
        doc = "Add a new access action"
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'data_access_action',
        name = 'update',
        callback = 'common.admin.access_actions.update',
        kind = access_action_schema,
        args = {
            id = types.string.nonNull,
            description = types.string,
            aggregates = types.list(permission_input),
        },
        doc = "Update an access action"
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'data_access_action',
        name = 'delete',
        callback = 'common.admin.access_actions.delete',
        kind = access_action_schema,
        args = {
            id = types.string.nonNull,
        },
        doc = "Delete an access action"
    })
end

return {
    init = init,
    get = get,
    list = list,
    create = create,
    update = update,
    delete = delete,
}
