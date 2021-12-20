local graphql = require('common.graphql')

local cartridge = require('cartridge')
local cartridge_auth = require('cartridge.auth')
local account_provider = require('account_provider.account_provider')
local types = require('graphql.types')

local auth = require('common.admin.auth')
local account = require('common.admin.account')
local account_states = require('account_manager.states')

local user_schema = types.object{
    name = 'UserSchema',
    fields = {
        uid = types.string.nonNull,
        email = types.string.nonNull,
        login = types.string.nonNull,
        username = types.string.nonNull,
        created_at = types.long.nonNull,
        last_login = types.long,
        state = types.string.nonNull,
        state_reason = types.string,
        role = types.string.nonNull,
        expires_in = types.long.nonNull,
        last_password_update_time = types.long,
        failed_login_attempts = types.long.nonNull,
        unblocked_at = types.long,
        tenant = types.string,
    },
    description = 'User info',
    schema = 'admin',
}

local user_with_password_schema = types.object{
    name = 'UserWithPasswordSchema',
    fields = {
        uid = types.string.nonNull,
        email = types.string.nonNull,
        login = types.string.nonNull,
        username = types.string.nonNull,
        created_at = types.long.nonNull,
        last_login = types.long,
        state = types.string.nonNull,
        state_reason = types.string,
        role = types.string.nonNull,
        expires_in = types.long.nonNull,
        last_password_update_time = types.long,
        failed_login_attempts = types.long.nonNull,
        unblocked_at = types.long,
        password = types.string.nonNull,
        tenant = types.string,
    },
    description = 'User info with password',
    schema = 'admin',
}

local function user_to_graphql(user)
    local role, err = account_provider.access_role_get(user.role_id)
    if err ~= nil then
        error(err)
    end

    return {
        uid = user.uid,
        email = user.email,
        login = user.login,
        username = user.username,
        created_at = user.created_at,
        last_login = user.last_login,
        state = account_states.to_string(user.state),
        state_reason = user.state_reason,
        role = role.name,
        expires_in = user.expires_in,
        last_password_update_time = user.last_password_update_time,
        failed_login_attempts = user.failed_login_attempts,
        unblocked_at = user.unblocked_at,
        tenant = user.tenant,
    }
end

local function user_with_password_to_graphql(user)
    local graphql_user = user_to_graphql(user)
    graphql_user.password = user.password
    return graphql_user
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

local function user_add(_, args)
    local role = args['role']
    local expires_in = args['expires_in']
    if expires_in ~= nil and expires_in < 0 then
        return nil, 'expires_in value should be positive'
    end

    local tenant_uid = args['tenant']
    local user_data = {
        email = args['email'],
        password = args['password'],
        role_id = role_id_by_name(role, tenant_uid),
        username = args['username'],
        expires_in = expires_in,
        tenant = tenant_uid,
    }

    local user, err = account_provider.create_user(user_data)
    if err ~= nil then
        error(err)
    end

    return user_with_password_to_graphql(user)
end

local function user_import(_, args)
    local user_data = {
        generate_password = args['generate'],
        use_mail = args['use_mail'],
        uid = args['uid'],
        email = args['email'],
        password = args['password'],
        login = args['login'],
        username = args['username'],
        created_at = args['created_at'],
        last_login = args['last_login'],
        state = account_states.from_string(args['state']),
        state_reason = args['state_reason'],
        role_id = role_id_by_name(args['role']),
        expires_in = args['expires_in'],
        last_password_update_time = args['last_password_update_time'],
        failed_login_attempts = args['failed_login_attempts'],
        unblocked_at = args['unblocked_at'],
        tenant = args['tenant'],
    }

    if user_data.expires_in ~= nil and user_data.expires_in < 0 then
        return nil, 'expires_in value should be positive'
    end

    local user, err = account_provider.import_user(user_data)
    if err ~= nil then
        error(err)
    end

    return user_with_password_to_graphql(user)
end

local function user_list(_, _)
    local result, err = account_provider.get_user_list()
    if err ~= nil then
        error(err)
    end

    local err
    for i, user in ipairs(result) do
        result[i], err = user_to_graphql(user)
        if err ~= nil then
            error(err)
        end
    end
    return result
end

local function user_remove(_, args)
    local uid = args['uid']

    local user, err = account_provider.delete_user(uid)
    if err ~= nil then
        error(err)
    end

    return user_to_graphql(user)
end

local function user_set_state(_, args)
    local state = account_states.from_string(args['state'])
    if state == nil then
        return nil, ("Unknown '%s' state"):format(args['state'])
    end

    local uid = args['uid']
    local reason = args['reason']

    local user, err = account_provider.set_user_state(uid, state, reason)
    if err ~= nil then
        return nil, err
    end

    return user_to_graphql(user)
end

local function user_modify(_, args)
    local uid = args['uid']
    local role = args['role']
    local expires_in = args['expires_in']
    if expires_in ~= nil and expires_in < 0 then
        return nil, 'expires_in value should be positive'
    end

    local user_data = {
        email = args['email'],
        password = args['password'],
        role_id = role_id_by_name(role),
        username = args['username'],
        expires_in = expires_in,
    }

    local user, err = account_provider.update_user(uid, user_data)
    if err ~= nil then
        return nil, err
    end

    if args['password'] ~= nil and account.id() == args['uid'] then
        cartridge_auth.set_lsid_cookie({
            username = user.login,
            version = user.last_password_update_time,
        })
    end


    return user_to_graphql(user)
end

local function user_reset_password(_, args)
    local uid = args['uid']

    local new_password, err = account_provider.reset_password_user(uid)
    if err ~= nil then
        error(err)
    end

    return new_password
end

local function is_anonymous_allowed(_, _)
    local rc, err = auth.is_anonymous_allowed()
    if rc == nil then
        error(err)
    end
    return rc
end

local function user_self(_, _)
    local login = cartridge.http_get_username()

    if login == nil then
        return nil
    end

    local user, err = account_provider.get_user_by_login(login, {without_cache = true})
    if err ~= nil then
        error(err)
    end
    if user == nil then
        error("Unknown user '" .. login .. "'")
    end

    return { user = user_to_graphql(user) }
end

local function user_self_modify(_, args)
    if account.is_empty() or not account.is_user() then
        error('Mutation is prohibited for not users')
    end

    local uid = account.id()
    local password = args['password']
    local username = args['username']

    local user, err = account_provider.update_user(uid, {
        password = password,
        username = username,
    })
    if err ~= nil then
        error(err)
    end

    return { user = user_to_graphql(user) }
end

local function init()
    graphql.add_mutation_prefix('admin', 'user', 'User management')
    graphql.add_callback_prefix('admin', 'user', 'User management')

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'user',
        name = 'add',
        callback = 'common.admin.users.user_add',
        kind = user_with_password_schema,
        args = {
            email = types.string.nonNull,
            password = types.string,
            role = types.string.nonNull,
            expires_in = types.long,
            username = types.string,
            tenant = types.string,
        },
        doc = "Add new user",
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'user',
        name = 'import',
        callback = 'common.admin.users.user_import',
        kind = user_with_password_schema,
        args = {
            generate = types.boolean,
            use_mail = types.boolean,
            uid = types.string.nonNull,
            email = types.string.nonNull,
            password = types.string,
            login = types.string.nonNull,
            username = types.string.nonNull,
            created_at = types.long.nonNull,
            last_login = types.long,
            state = types.string.nonNull,
            state_reason = types.string,
            role = types.string.nonNull,
            expires_in = types.long.nonNull,
            last_password_update_time = types.long,
            failed_login_attempts = types.long.nonNull,
            unblocked_at = types.long,
            tenant = types.string,
        },
        doc="Add new user",
    })

    graphql.add_mutation({
        schema='admin',
        prefix='user',
        name='remove',
        callback='common.admin.users.user_remove',
        kind=user_schema,
        args={uid=types.string},
        doc="Remove user",
    })

    graphql.add_mutation({
        schema='admin',
        prefix='user',
        name='set_state',
        callback='common.admin.users.user_set_state',
        kind=user_schema,
        args={uid=types.string.nonNull, state=types.string.nonNull, reason=types.string},
        doc="Set user state",
    })

    graphql.add_mutation({
        schema='admin',
        prefix='user',
        name='modify',
        callback='common.admin.users.user_modify',
        kind=user_schema,
        args = {
            uid = types.string.nonNull,
            email = types.string,
            password = types.string,
            username = types.string,
            expires_in = types.long,
            role = types.string,
        },
        doc="Modify user",
    })

    graphql.add_mutation({
        schema='admin',
        prefix='user',
        name='reset_password',
        callback='common.admin.users.user_reset_password',
        kind=types.string,
        args={uid=types.string},
        doc="Reset user's password",
    })

    graphql.add_callback({
        schema='admin',
        prefix='user',
        name='list',
        callback='common.admin.users.user_list',
        kind=types.list(user_schema),
        args={},
        doc='Enumerate users',
    })

    graphql.add_callback({
        schema='admin',
        prefix = 'user',
        name = 'is_anonymous_allowed',
        doc = 'Returns whether anonymous access allowed',
        args = {},
        kind = types.boolean,
        callback = 'common.admin.users.is_anonymous_allowed',
     })

    local self_user = types.object{
        name='GraphqlUser',
        description='Graphql authorization info',
        fields = {
            user = user_schema,
        },
    }
    graphql.add_callback({
        schema='admin',
        prefix = 'user',
        name = 'self',
        doc = 'Returns if access granted and current logged user. ' ..
            'If user is null than access granted by anonymous or token',
        args = {},
        kind = self_user,
        callback = 'common.admin.users.self',
    })

    graphql.add_mutation({
        schema='admin',
        prefix = 'user',
        name = 'self_modify',
        doc = 'Current user information modification',
        args = {password=types.string, username=types.string},
        kind = self_user,
        callback = 'common.admin.users.self_modify',
    })
end

return {
    -- graphql api
    is_anonymous_allowed = is_anonymous_allowed,
    user_add = user_add,
    user_import = user_import,
    user_list = user_list,
    user_remove = user_remove,
    user_modify = user_modify,
    user_set_state = user_set_state,
    user_reset_password = user_reset_password,
    self = user_self,
    self_modify = user_self_modify,

    init = init,
}
