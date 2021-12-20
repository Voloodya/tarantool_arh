local module_name = 'account_provider.account_provider'

local cartridge = require('cartridge')

local errors = require('errors')
local vars = require('common.vars').new(module_name)
local cache = require('account_provider.cache')
local ldap = require('account_provider.ldap')
local password_digest = require('account_manager.password_digest')
local account_states = require('account_manager.states')
local audit_log = require('audit.log').new(module_name)
local config_filter = require('common.config_filter')

vars:new('external_auth_plugin')

local config_error = errors.new_class('account_provider_config_error')
local external_auth_error = errors.new_class('external_auth_error')

-- Users
local function get_user_list()
    return cache.get_user_list()
end

local function create_user(user)
    return cartridge.rpc_call('core', 'create_user', {user}, {leader_only = true})
end

local function import_user(user)
    return cartridge.rpc_call('core', 'import_user', {user}, {leader_only = true})
end

local function delete_user(uid)
    return cartridge.rpc_call('core', 'delete_user', {uid}, {leader_only = true})
end

local function update_user(uid, updates)
    return cartridge.rpc_call('core', 'update_user', {uid, updates}, {leader_only = true})
end

local function set_user_state(uid, state, reason)
    return cartridge.rpc_call('core', 'set_user_state', {uid, state, reason}, {leader_only = true})
end

local function reset_password_user(uid)
    return cartridge.rpc_call('core', 'reset_password_user', {uid}, {leader_only = true})
end

local function get_user_by_login(login, opts)
    if ldap.ldap_is_enabled() then
        local user = ldap.get_user(login)
        if user ~= nil then
            return user
        end
    end

    return cache.get_user(login, opts)
end

local function reset_user_failed_login_attempts(uid)
    return cartridge.rpc_call('core', 'reset_user_failed_login_attempts', {uid}, {leader_only = true})
end

local function inc_user_failed_login_attempts(uid)
    return cartridge.rpc_call('core', 'inc_user_failed_login_attempts', {uid}, {leader_only = true})
end

local function is_password_correct(user, password)
    local password_is_correct = (user.password == password_digest.get_salted_password(password))
    if not password_is_correct then
        inc_user_failed_login_attempts(user.uid)
        audit_log.warn('Incorrect password for user %s', user.uid)
    else
        reset_user_failed_login_attempts(user.uid)
        audit_log.info('Correct password for user %s', user.uid)
    end

    return password_is_correct
end

local function check_password(login, password)
    if ldap.ldap_is_enabled() then
        local ok = ldap.authorize(login, password)
        if ok then
            return true
        end
    end

    local user, err  = cache.get_user(login)
    if err ~= nil then
        return nil, err
    end
    if user == nil then
        return nil, 'User not found'
    end

    if user.state == account_states.BLOCKED then
        return nil, 'User is blocked'
    end

    return is_password_correct(user, password)
end

-- Tokens
local function token_create(token)
    return cartridge.rpc_call('core', 'token_create',
        {token}, {leader_only = true})
end

local function token_import(token)
    return cartridge.rpc_call('core', 'token_import', {token}, {leader_only = true})
end

local function token_update(name, updates)
    return cartridge.rpc_call('core', 'token_update', {name, updates}, {leader_only = true})
end

local function token_remove(name)
    return cartridge.rpc_call('core', 'token_remove', {name}, {leader_only = true})
end

local function token_list()
    return cache.get_token_list()
end

local function token_get(uid)
    return cache.get_token(uid)
end

local function token_get_by_name(name)
    return cache.get_token_by_name(name)
end

local function token_set_state(name, state, reason)
    return cartridge.rpc_call('core', 'token_set_state', {name, state, reason}, {leader_only = true})
end

-- External auth
local function external_auth_enabled()
    return vars.external_auth_plugin ~= nil
end

local function check_external_auth(req)
    return external_auth_error:pcall(vars.external_auth_plugin.auth, req)
end

-- Roles
local function access_roles_list(tenant_uid)
    return cartridge.rpc_call('core', 'get_access_roles_list', {tenant_uid})
end

local function access_role_get(id, tenant_uid)
    return cartridge.rpc_call('core', 'get_access_role', {id, tenant_uid})
end

local function access_role_get_by_name(name, tenant_uid)
    return cartridge.rpc_call('core', 'get_access_role_by_name', {name, tenant_uid})
end

local function access_roles_get_authority(id, tenant_uid)
    return cartridge.rpc_call('core', 'get_access_roles_authority', {id, tenant_uid})
end

local function access_role_create(role)
    return cartridge.rpc_call('core', 'create_access_role', {role}, {leader_only = true})
end

local function access_role_update(id, updates)
    return cartridge.rpc_call('core', 'update_access_role',
        {id, updates}, {leader_only = true})
end

local function access_role_delete(id)
    return cartridge.rpc_call('core', 'delete_access_role', {id}, {leader_only = true})
end

local function access_role_get_actions(role_id)
    return cache.get_access_role_action_list(role_id)
end

local function access_role_update_actions(id, new_actions)
    return cartridge.rpc_call('core', 'update_access_role_actions', {id, new_actions}, {leader_only = true})
end

local function action_is_allowed_for_role(role_id, action)
    return cache.get_access_role_action(role_id, action)
end

local function data_action_list()
    return cartridge.rpc_call('core', 'get_data_action_list', {}, {})
end

local function data_action_get(uid)
    return cartridge.rpc_call('core', 'get_data_action', {uid}, {})
end

local function data_action_create(description, permissions)
    return cartridge.rpc_call('core', 'create_data_action',
        {description, permissions}, {leader_only = true})
end

local function data_action_update(uid, description, permissions)
    return cartridge.rpc_call('core', 'update_data_action',
        {uid, description, permissions}, {leader_only = true})
end

local function data_action_delete(uid)
    return cartridge.rpc_call('core', 'delete_data_action',
        {uid}, {leader_only = true})
end

local function check_data_action(role_id, aggregate, what)
    return cache.check_data_action(role_id, aggregate, what)
end

-- Config routines
local function validate_config(cfg)
    ldap.validate_config(cfg)

    if cfg['auth_external'] ~= nil then
        local plugin_module, err = config_error:pcall(load, cfg['auth_external'], 't')
        config_error:assert(err == nil, 'auth_external: invalid code: %s', err)

        local plugin, err = config_error:pcall(plugin_module)
        config_error:assert(err == nil, err)

        config_error:assert(type(plugin) == 'table', 'auth_external plugin should return a table')
        config_error:assert(type(plugin.auth) == 'function', 'auth_external file should contain "auth" method')
    end
    return true
end

local function apply_config(cfg)
    ldap.apply_config(cfg)

    local _, err = config_filter.compare_and_set(cfg, 'auth_external', module_name)
    if err == nil then
        if cfg['auth_external'] == nil then
            vars.external_auth_plugin = nil
        else
            local plugin = load(cfg['auth_external'], 't')
            vars.external_auth_plugin = plugin()
        end
    end

    return true
end

local function init()
    if not box.info.ro then
        box.schema.user.disable('guest')
    end
    cache.init()
    return true
end

return {
    init = init,
    apply_config = apply_config,
    validate_config = validate_config,
    check_external_auth = check_external_auth,
    external_auth_enabled = external_auth_enabled,

    -- Users
    create_user = create_user,
    import_user = import_user,
    get_user_list = get_user_list,
    delete_user = delete_user,
    update_user = update_user,
    set_user_state = set_user_state,
    reset_password_user = reset_password_user,
    get_user_by_login = get_user_by_login,
    check_password = check_password,

    -- Tokens
    token_create = token_create,
    token_import = token_import,
    token_get = token_get,
    token_get_by_name = token_get_by_name,
    token_update = token_update,
    token_remove = token_remove,
    token_set_state = token_set_state,
    token_list = token_list,

    -- Roles
    access_roles_list = access_roles_list,
    access_role_get = access_role_get,
    access_role_get_by_name = access_role_get_by_name,
    access_roles_get_authority = access_roles_get_authority,
    access_role_create = access_role_create,
    access_role_update = access_role_update,
    access_role_delete = access_role_delete,
    access_role_get_actions = access_role_get_actions,
    access_role_update_actions = access_role_update_actions,
    action_is_allowed_for_role = action_is_allowed_for_role,

    -- Access action permissions
    data_action_list = data_action_list,
    data_action_get = data_action_get,
    data_action_create = data_action_create,
    data_action_update = data_action_update,
    data_action_delete = data_action_delete,
    check_data_action = check_data_action,
}
