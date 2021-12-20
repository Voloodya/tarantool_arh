local graphql = require('common.graphql')
local types = require('graphql.types')
local account_provider = require('account_provider.account_provider')
local password_digest = require('account_manager.password_digest')
local account_states = require('account_manager.states')

local token_schema = types.object {
    name = 'TokenSchema',
    description = 'Authentication token for external connected app using',
    fields = {
        uid = types.string,
        token = types.string,
        name = types.string.nonNull,
        created_at = types.long.nonNull,
        last_login = types.long,
        state = types.string.nonNull,
        state_reason = types.string,
        role = types.string.nonNull,
        expires_in = types.long.nonNull,
        unblocked_at = types.long,
        tenant = types.string,
    },
    schema = 'admin',
}

local function token_to_graphql(token)
    local role, err = account_provider.access_role_get(token.role_id)
    if err ~= nil then
        return nil, err
    end
    return {
        uid = token.uid,
        name = token.name,
        created_at = token.created_at,
        last_login = token.last_login,
        state = account_states.to_string(token.state),
        state_reason = token.state_reason,
        role = role.name,
        expires_in = token.expires_in,
        unblocked_at = token.unblocked_at,
        tenant = token.tenant,
    }
end

local function role_id_by_name(name, tenant_uid)
    if name == nil then
        return nil
    end

    local role, err = account_provider.access_role_get_by_name(name, tenant_uid)
    if err ~= nil then
        error(err)
    end

    if role == nil then
        error(('Role "%s" is not found'):format(name))
    end

    return role.id
end

local function token_create(_, args)
    local expires_in = args['expires_in']
    if expires_in ~= nil and expires_in < 0 then
        return nil, 'expires_in value should be positive'
    end

    local tenant_uid = args['tenant']
    local role = args['role']
    local role_id = role_id_by_name(role, tenant_uid)

    local data = {
        name = args['name'],
        role_id = role_id,
        expires_in = expires_in,
        tenant = tenant_uid,
    }

    local token, err = account_provider.token_create(data)
    if err ~= nil then
        return nil, err
    end

    -- The only one single place to view token
    token.role = role
    token.state = account_states.to_string(token.state)
    return token
end

local function token_import(_, args)
    local token = {}

    token.uid = args['uid']
    token.name = args['name']
    token.created_at = args['created_at']
    token.last_login = args['last_login']
    token.state = account_states.from_string(args['state'])
    token.state_reason = args['state_reason']
    token.unblocked_at = args['unblocked_at']
    token.role_id = role_id_by_name(args['role'])
    token.tenant = args['tenant']
    token.expires_in = args['expires_in']
    if token.expires_in ~= nil and token.expires_in < 0 then
        return nil, 'expires_in value should be positive'
    end

    local token, err = account_provider.token_import(token)
    if err ~= nil then
        return nil, err
    end

    token.state = account_states.to_string(token.state)

    return token
end

local function token_get(_, args)
    local name = args['name']

    local token, err = account_provider.token_get_by_name(name)
    if err ~= nil then
        return nil, err
    end
    if token == nil then
        error('Token ' .. name .. ' is not found')
    end

    return token_to_graphql(token)
end

local function token_update(_, args)
    local name = args['name']
    local role = args['role']
    local role_id = role_id_by_name(role)
    local expires_in = args['expires_in']
    if expires_in ~= nil and expires_in < 0 then
        return nil, 'expires_in value should be positive'
    end

    local updates = {
        role_id = role_id,
        expires_in = expires_in,
    }

    local token, err = account_provider.token_update(name, updates)
    if err ~= nil then
        return nil, err
    end

    return token_to_graphql(token)
end

local function token_remove(_, args)
    local name = args['name']

    local token, err = account_provider.token_remove(name)

    if err ~= nil then
        return nil, err
    end

    return token_to_graphql(token)
end

local function token_set_state(_, args)
    local state = account_states.from_string(args['state'])
    if state == nil then
        return nil, ("Unknown '%s' state"):format(args['state'])
    end

    local name = args['name']
    local reason = args['reason']

    local token, err = account_provider.token_set_state(name, state, reason)
    if err ~= nil then
        return nil, err
    end
    return token_to_graphql(token)
end

local function token_list()
    local result, err = account_provider.token_list()

    if err ~= nil then
        return nil, err
    end

    for i, token in ipairs(result) do
        result[i] = token_to_graphql(token)
    end
    return result
end

local function get_token_from_request(request)
    local auth_header = request.headers['authorization']
    if auth_header and auth_header:startswith('Bearer ') then
        return auth_header:sub(8, -1)
    end
end

local function get_token_info(token)
    local digest = password_digest.password_digest(token)

    local info, err = account_provider.token_get(digest)
    if info == nil then
        return nil, err
    end

    return {
        uid = digest,
        name = info.name,
        role_id = info.role_id,
        state = info.state,
        tenant = info.tenant,
    }
end

local function init()
    graphql.add_callback_prefix('admin', 'token', 'Token management')
    graphql.add_mutation_prefix('admin', 'token', 'Token management')

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'token',
        name = 'add',
        callback = 'common.admin.token_acl.token_create',
        kind = token_schema,
        args = {
            name = types.string.nonNull,
            role = types.string.nonNull,
            expires_in = types.long,
            tenant = types.string,
        },
        doc = 'Add new token'
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'token',
        name = 'import',
        callback = 'common.admin.token_acl.token_import',
        kind = token_schema,
        args = {
            uid = types.string.nonNull,
            name = types.string.nonNull,
            created_at = types.long.nonNull,
            last_login = types.long,
            state = types.string.nonNull,
            state_reason = types.string,
            role = types.string.nonNull,
            expires_in = types.long.nonNull,
            unblocked_at = types.long,
            tenant = types.string,
        },
        doc = 'Import token',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'token',
        name = 'get',
        callback = 'common.admin.token_acl.token_get',
        kind = token_schema,
        args = {name = types.string.nonNull},
        doc = 'Get token',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'token',
        name = 'update',
        callback = 'common.admin.token_acl.token_update',
        kind = token_schema,
        args = {
            name = types.string.nonNull,
            role = types.string,
            expires_in = types.long,
        },
        doc = 'Update token',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'token',
        name = 'remove',
        callback = 'common.admin.token_acl.token_remove',
        kind = token_schema,
        args = {name = types.string.nonNull},
        doc = 'Remove token',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'token',
        name = 'set_state',
        callback = 'common.admin.token_acl.token_set_state',
        kind = token_schema,
        args = {name=types.string.nonNull, state=types.string.nonNull, reason=types.string},
        doc = "Set token state",
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'token',
        name = 'list',
        callback = 'common.admin.token_acl.token_list',
        kind = types.list(token_schema),
        args = {},
        doc = 'Enumerate all tokens',
    })
end

return {
    token_create = token_create,
    token_import = token_import,
    token_get = token_get,
    token_update = token_update,
    token_remove = token_remove,
    token_list = token_list,
    token_set_state = token_set_state,

    get_token_from_request = get_token_from_request,
    get_token_info = get_token_info,

    token_schema = token_schema,

    init = init,
}
