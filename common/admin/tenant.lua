local cartridge = require('cartridge')
local types = require('graphql.types')
local graphql = require('common.graphql')
local account_states = require('account_manager.states')

local tenant_schema = types.object{
    name = 'TenantSchema',
    fields = {
        uid = types.string.nonNull,
        name = types.string.nonNull,
        description = types.string,
        created_at = types.long.nonNull,
        state = types.string.nonNull,
        state_reason = types.string,
    },
    description = 'Tenant info',
    schema = 'admin',
}

local tenant_details_schema = types.object{
    name = 'TenantDetailsSchema',
    fields = {
        uid = types.string.nonNull,
        port = types.long,
    },
    description = 'Detailed tenants info',
    schema = 'admin',
}

local function to_graphql(tenant)
    return {
        uid = tenant.uid,
        name = tenant.name,
        description = tenant.description,
        created_at = tenant.created_at,
        state = account_states.to_string(tenant.state),
        state_reason = tenant.state_reason,
    }
end

local function create(_, args)
    local name = args['name']
    local description = args['description']

    local tenant, err = cartridge.rpc_call('core', 'tenant_create', {name, description}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end
    return to_graphql(tenant)
end

local function list(_, _)
    local tenant_list, err = cartridge.rpc_call('core', 'tenant_list', {})
    if err ~= nil then
        return nil, err
    end

    for i, tenant in ipairs(tenant_list) do
        tenant_list[i] = to_graphql(tenant)
    end
    return tenant_list
end

local function details(_, args)
    local details, err = cartridge.rpc_call('core', 'tenant_details', {args.uid})
    if err ~= nil then
        return nil, err
    end
    return details
end

local function details_list(_, _)
    local tenant_details, err = cartridge.rpc_call('core', 'tenant_details_list', {})
    if err ~= nil then
        return nil, err
    end
    return tenant_details
end

local function get(_, args)
    local uid = args['uid']
    local tenant, err = cartridge.rpc_call('core', 'tenant_get', {uid})
    if err ~= nil then
        return nil, err
    end
    return to_graphql(tenant)
end

local function delete(_, args)
    local uid = args['uid']
    local tenant, err = cartridge.rpc_call('core', 'tenant_delete', {uid}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end
    return to_graphql(tenant)
end

local function update(_, args)
    local uid = args['uid']
    local name = args['name']
    local description = args['description']
    local tenant, err = cartridge.rpc_call('core', 'tenant_update', {uid, name, description}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end
    return to_graphql(tenant)
end

local function set_state(_, args)
    local uid = args['uid']
    local state = args['state']
    local state_reason = args['state_reason']
    local tenant, err = cartridge.rpc_call('core', 'tenant_set_state', {uid, state, state_reason}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end
    return to_graphql(tenant)
end

local function init()
    graphql.add_mutation_prefix('admin', 'tenant', 'Tenant management')
    graphql.add_callback_prefix('admin', 'tenant', 'Tenant management')

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'tenant',
        name = 'create',
        callback = 'common.admin.tenant.create',
        kind = tenant_schema,
        args = {
            name = types.string.nonNull,
            description = types.string,
        },
        doc = 'Create new tenant',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'tenant',
        name = 'update',
        callback = 'common.admin.tenant.update',
        kind = tenant_schema,
        args = {
            uid = types.string.nonNull,
            name = types.string,
            description = types.string,
        },
        doc = 'Update tenant',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'tenant',
        name = 'set_state',
        callback = 'common.admin.tenant.set_state',
        kind = tenant_schema,
        args = {
            uid = types.string.nonNull,
            state = types.string.nonNull,
            state_reason = types.string,
        },
        doc = 'Set tenant\'s state',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'tenant',
        name = 'delete',
        callback = 'common.admin.tenant.delete',
        kind = tenant_schema,
        args = {
            uid = types.string.nonNull,
        },
        doc = 'Delete tenant',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'tenant',
        name = 'list',
        callback = 'common.admin.tenant.list',
        kind = types.list(tenant_schema),
        args = {},
        doc = 'Enumerate tenants',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'tenant',
        name = 'details',
        callback = 'common.admin.tenant.details',
        kind = tenant_details_schema,
        args = {
            uid = types.string
        },
        doc = 'Get current tenant details',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'tenant',
        name = 'details_list',
        callback = 'common.admin.tenant.details_list',
        kind = types.list(tenant_details_schema),
        args = {},
        doc = 'Get list of tenant details',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'tenant',
        name = 'get',
        callback = 'common.admin.tenant.get',
        kind = tenant_schema,
        args = {
            uid = types.string.nonNull,
        },
        doc = 'Get tenant',
    })
end

return {
    -- Tenant DDL
    get = get,
    list = list,
    details = details,
    details_list = details_list,
    create = create,
    update = update,
    set_state = set_state,
    delete = delete,

    init = init,
}
