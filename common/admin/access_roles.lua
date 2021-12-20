local module_name = 'common.admin.access_roles'

local errors = require('errors')
local log = require('log.log').new(module_name)

local account_provider = require('account_provider.account_provider')
local access_actions_list = require('account_manager.access_actions_list')
local graphql = require('common.graphql')
local types = require('graphql.types')
local sections_list = access_actions_list.get_sections()

local access_error = errors.new_class('access_error')

local action_schema = types.object {
    name = 'AccessAction',
    description = 'Access action model',
    fields = {
        id = types.string.nonNull,
        description = types.string.nonNull,
        section = types.string.nonNull,
        type = types.string.nonNull,
    },
    schema = 'admin',
}

local role_action_schema = types.object {
    name = 'RoleAccessAction',
    description = 'Role access action model',
    fields = {
        id = types.string.nonNull,
        description = types.string.nonNull,
        section = types.string.nonNull,
        type = types.string.nonNull,
        allowed = types.boolean.nonNull,
    },
    schema = 'admin',
}

local role_action_input_schema = types.inputObject {
    name = 'RoleAccessActionInput',
    description = 'Role access action input model',
    fields = {
        id = types.string.nonNull,
        allowed = types.boolean.nonNull,
    },
    schema = 'admin',
}

local role_schema = types.object {
    name = 'AccessRole',
    description = 'Access role model',
    fields = {
        id = types.long.nonNull,
        name = types.string.nonNull,
        description = types.string.nonNull,
        created_at = types.long,
        authority = types.long,
    },
    schema = 'admin',
}

local function role_to_graphql(role)
    return {
        id = role.id,
        name = role.name,
        description = role.description,
        created_at = role.created_at,
        authority = role.authority,
    }
end

local function get_access_roles_list(_, args)
    log.verbose('Get roles list')

    local tenant_uid = args.tenant
    local roles, err = account_provider.access_roles_list(tenant_uid)
    if err ~= nil then
        return nil, access_error:new(err)
    end

    for i, role in ipairs(roles) do
        roles[i] = role_to_graphql(role)

        local authority, err = account_provider.access_roles_get_authority(role.id, tenant_uid)
        if err ~= nil then
            return nil, access_error:new(err)
        end
        roles[i].authority = authority
    end
    return roles
end

local function get_access_role(_, args)
    local id = args['id']
    local tenant_uid = args['tenant']
    local role, err = account_provider.access_role_get(id, tenant_uid)
    if err ~= nil then
        return nil, access_error:new(err)
    end

    local authority, err = account_provider.access_roles_get_authority(id, tenant_uid)
    if err ~= nil then
        return nil, access_error:new(err)
    end
    role.authority = authority

    return role_to_graphql(role)
end

local function create_access_role(_, args)
    log.info('Create new role')

    local role, err = account_provider.access_role_create(args)

    if err ~= nil then
        return nil, access_error:new(err)
    end

    return role_to_graphql(role)
end

local function update_access_role(_, args)
    log.info('Update role %s', args['id'])

    local id = args['id']
    local updates = args
    local role, err = account_provider.access_role_update(id, updates)
    if err ~= nil then
        return nil, access_error:new(err)
    end

    return role_to_graphql(role)
end

local function delete_access_role(_, args)
    log.info('Delete role %s', args['id'])

    local role, err = account_provider.access_role_delete(args['id'])

    if err ~= nil then
        return nil, access_error:new(err)
    end

    return role_to_graphql(role)
end

local function get_access_action_list()
    log.verbose('Get access actions list')
    local access_actions = access_actions_list.get()
    local result = {}
    for v in pairs(access_actions) do
        table.insert(result, {
            id = v,
            description = access_actions_list.get_description(v),
            section = access_actions_list.get_section(v),
            type = 'action'
        })
    end

    local data_actions, err = account_provider.data_action_list()
    if err ~= nil then
        return nil, err
    end

    for _, data_access_action in ipairs(data_actions) do
        table.insert(result, {
            id = data_access_action.id,
            description = data_access_action.description,
            section = sections_list.data_actions,
            type = 'data',
        })
    end

    return result
end

local function get_access_role_actions(_, args)
    log.verbose('Get access actions list of role %s', args['id'])
    local actions, err = account_provider.access_role_get_actions(args['id'])
    if err ~= nil then
        return nil, err
    end
    return actions
end

local function update_access_role_actions(_, args)
    log.verbose('Update access actions list of role %s', args['id'])
    local actions, err = account_provider.access_role_update_actions(args['id'], args['actions'])
    if err ~= nil then
        return nil, err
    end
    return actions
end

local function init()
    graphql.add_callback_prefix('admin', 'access_role', 'Roles management')
    graphql.add_mutation_prefix('admin', 'access_role', 'Roles management')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'access_role',
        name = 'get',
        callback = 'common.admin.access_roles.get_access_role',
        kind = role_schema,
        args = {
            id = types.long.nonNull,
            tenant = types.string,
        },
        doc = 'Returns role',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'access_role',
        name = 'create',
        callback = 'common.admin.access_roles.create_access_role',
        kind = role_schema,
        args = {
            name = types.string.nonNull,
            description = types.string,
        },
        doc = 'Add new role',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'access_role',
        name = 'update',
        callback = 'common.admin.access_roles.update_access_role',
        kind = role_schema,
        args = {
            id = types.long.nonNull,
            name = types.string.nonNull,
            description = types.string,
        },
        doc = 'Modify role',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'access_role',
        name = 'delete',
        callback = 'common.admin.access_roles.delete_access_role',
        kind = role_schema,
        args = {id = types.long.nonNull},
        doc='Delete role',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'access_role',
        name = 'list',
        callback = 'common.admin.access_roles.get_access_roles_list',
        kind = types.list(role_schema),
        args = {tenant = types.string},
        doc = 'Returns roles list',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'access_role',
        name = 'actions_list',
        callback = 'common.admin.access_roles.get_access_action_list',
        kind = types.list(action_schema),
        args = {},
        doc = 'Returns access action list',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'access_role',
        name = 'get_access_role_actions',
        callback = 'common.admin.access_roles.get_access_role_actions',
        kind = types.list(role_action_schema),
        args = {id = types.long.nonNull},
        doc = 'Returns access actions of role',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'access_role',
        name = 'update_access_role_actions',
        callback = 'common.admin.access_roles.update_access_role_actions',
        kind = types.list(role_action_schema),
        args = {id = types.long.nonNull, actions = types.list(role_action_input_schema).nonNull},
        doc = 'Update access action list',
    })
end

return {
    get_access_roles_list = get_access_roles_list,
    get_access_role = get_access_role,
    create_access_role = create_access_role,
    update_access_role = update_access_role,
    delete_access_role = delete_access_role,

    get_access_action_list = get_access_action_list,
    get_access_role_actions = get_access_role_actions,
    update_access_role_actions = update_access_role_actions,
    init = init,
}
