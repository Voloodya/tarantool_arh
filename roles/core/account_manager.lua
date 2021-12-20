local account_manager_user = require('account_manager.user')
local account_manager_token = require('account_manager.token')
local account_manager_server = require('account_manager.server')
local account_manager_access_roles = require('account_manager.access_role')
local account_manager_password_generator = require('account_manager.password_generator.server')
local account_manager_expiration = require('account_manager.expiration.server')
local account_manager_webui = require('account_manager.webui.server')
local account_manager_data_actions = require('account_manager.data_actions')
local account_manager_tenant = require('account_manager.tenant')

-- Users
local function create_user(user)
    return account_manager_user.create(user)
end

local function import_user(user)
    return account_manager_user.import(user)
end

local function update_user(uid, updates)
    return account_manager_user.update(uid, updates)
end

local function delete_user(uid)
    return account_manager_user.delete(uid)
end

local function set_user_state(uid, state, reason)
    return account_manager_user.set_state(uid, state, reason)
end

local function reset_password_user(uid)
    return account_manager_user.reset_password(uid)
end

local function get_user_by_login(login)
    return account_manager_user.get_by_login(login)
end

local function get_user_list()
    return account_manager_user.list()
end

local function inc_user_failed_login_attempts(uid)
    return account_manager_user.inc_failed_login_attempts(uid)
end

local function reset_user_failed_login_attempts(uid)
    return account_manager_user.reset_failed_login_attempts(uid)
end

local function update_users_activity(users)
    return account_manager_user.update_activity(users)
end

-- Password generator

local function generate_password(opts)
    return account_manager_password_generator.generate(opts)
end

local function validate_password(password, opts)
    return account_manager_password_generator.validate(password, opts)
end

-- Tokens
local function token_create(token)
    return account_manager_token.create(token)
end

local function token_import(token)
    return account_manager_token.import(token)
end

local function token_get(uid)
    return account_manager_token.get(uid)
end

local function token_get_by_name(name)
    return account_manager_token.get_by_name(name)
end

local function token_update(name, updates)
    return account_manager_token.update(name, updates)
end

local function token_remove(name)
    return account_manager_token.remove(name)
end

local function token_set_state(name, state, reason)
    return account_manager_token.set_state(name, state, reason)
end

local function token_list()
    return account_manager_token.list()
end

local function token_update_activity(tokens)
    return account_manager_token.update_activity(tokens)
end

-- Tenants

local function tenant_create(name, description)
    return account_manager_tenant.create(name, description)
end

local function tenant_delete(uid)
    return account_manager_tenant.delete(uid)
end

local function tenant_update(uid, name, description)
    return account_manager_tenant.update(uid, name, description)
end

local function tenant_get(uid)
    return account_manager_tenant.get(uid)
end

local function tenant_list()
    return account_manager_tenant.list()
end

local function tenant_details_list()
    return account_manager_tenant.details_list()
end

local function tenant_details(uid)
    return account_manager_tenant.details(uid)
end

-- Roles
local function create_access_role(role)
    return account_manager_access_roles.create(role)
end

local function update_access_role(id, updates)
    return account_manager_access_roles.update(id, updates)
end

local function delete_access_role(id)
    return account_manager_access_roles.delete(id)
end

local function get_access_role(id, tenant_uid)
    return account_manager_access_roles.get(id, tenant_uid)
end

local function get_access_role_by_name(name, tenant_uid)
    return account_manager_access_roles.get_by_name(name, tenant_uid)
end

local function get_access_roles_list(tenant_uid)
    return account_manager_access_roles.list(tenant_uid)
end

local function get_access_roles_authority(role_id, tenant_uid)
    return account_manager_access_roles.get_authority(role_id, tenant_uid)
end

local function get_access_role_actions(role_id)
    return account_manager_access_roles.get_access_actions(role_id)
end

local function update_access_role_actions(role_id, new_actions)
    return account_manager_access_roles.update_role_actions(role_id, new_actions)
end

local function get_webui_blacklist(role_id)
    return account_manager_webui.get_blacklist(role_id)
end

local function create_data_action(description, permissions)
    return account_manager_data_actions.create(description, permissions)
end

local function get_data_action(uid)
    return account_manager_data_actions.get(uid)
end

local function get_data_action_list()
    return account_manager_data_actions.list()
end

local function update_data_action(uid, description, permissions)
    return account_manager_data_actions.update(uid, description, permissions)
end

local function delete_data_action(uid)
    return account_manager_data_actions.delete(uid)
end

local function get_aggregate_access_list_for_role(role_id)
    return account_manager_data_actions.aggregate_access_list_for_role(role_id)
end

local function tenant_validate_config(cfg)
    local _, err = account_manager_user.tenant_validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_access_roles.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function tenant_apply_config(cfg, opts)
    local _, err = account_manager_server.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_user.tenant_apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_data_actions.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_access_roles.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function apply_config(cfg)
    local _, err = account_manager_user.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_password_generator.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_token.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_tenant.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_expiration.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function validate_config(cfg)
    local _, err = account_manager_expiration.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager_password_generator.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function stop()
    account_manager_server.stop()
end

return {
    validate_config = validate_config,
    apply_config = apply_config,
    tenant_validate_config = tenant_validate_config,
    tenant_apply_config = tenant_apply_config,
    stop = stop,

    -- rpc registry
    -- users
    create_user = create_user,
    import_user = import_user,
    update_user = update_user,
    delete_user = delete_user,
    reset_password_user = reset_password_user,
    get_user_by_login = get_user_by_login,
    get_user_list = get_user_list,
    set_user_state = set_user_state,
    inc_user_failed_login_attempts = inc_user_failed_login_attempts,
    reset_user_failed_login_attempts = reset_user_failed_login_attempts,
    update_users_activity = update_users_activity,

    -- password_generator
    generate_password = generate_password,
    validate_password = validate_password,

    -- tokens
    token_create = token_create,
    token_import = token_import,
    token_get_by_name = token_get_by_name,
    token_get = token_get,
    token_update = token_update,
    token_remove = token_remove,
    token_set_state = token_set_state,
    token_list = token_list,
    token_update_activity = token_update_activity,

    -- tenants
    tenant_create = tenant_create,
    tenant_update = tenant_update,
    tenant_delete = tenant_delete,
    tenant_list = tenant_list,
    tenant_details_list = tenant_details_list,
    tenant_details = tenant_details,
    tenant_get = tenant_get,

    -- roles
    create_access_role = create_access_role,
    delete_access_role = delete_access_role,
    update_access_role = update_access_role,
    get_access_role = get_access_role,
    get_access_role_by_name = get_access_role_by_name,
    get_access_roles_list = get_access_roles_list,
    get_access_roles_authority = get_access_roles_authority,
    get_access_role_actions = get_access_role_actions,
    update_access_role_actions = update_access_role_actions,
    get_webui_blacklist = get_webui_blacklist,

    -- access action permissions
    create_data_action = create_data_action,
    get_data_action = get_data_action,
    get_data_action_list = get_data_action_list,
    delete_data_action = delete_data_action,
    update_data_action = update_data_action,
    get_aggregate_access_list_for_role = get_aggregate_access_list_for_role,
}
