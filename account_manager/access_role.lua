local module_name = 'account_manager.role'
local clock = require('clock')
local cartridge = require('cartridge')
local vars = require('common.vars').new(module_name)
local tenant = require('common.tenant')
local access_actions_list = require('account_manager.access_actions_list')
local account_manager_server = require('account_manager.server')
local account_manager_tenant = require('account_manager.tenant')

vars:new_global('users')
vars:new_global('tokens')
vars:new_global('data_actions')

local base_role_space_name = 'tdg_roles'
local role_sequence_name = 'tdg_role_id'

local role_format = {
    {name = 'id', type = 'unsigned', is_nullable = false},
    {name = 'name', type = 'string', is_nullable = false},
    {name = 'description', type = 'string', is_nullable = false},
    {name = 'created_at', type = 'unsigned', is_nullable = true},
    {name = 'is_active', type = 'boolean', is_nullable = false},
}

local sections_list = access_actions_list.get_sections()

local function get_role_space_name(tenant_uid)
    if tenant_uid == nil then
        tenant_uid = tenant.uid()
    end

    return tenant.get_space_name(base_role_space_name, tenant_uid)
end

local function get_role_space(tenant_uid)
    local space_name = get_role_space_name(tenant_uid)
    return box.space[space_name]
end

local base_actions_space_name = 'tdg_access_actions'

local actions_format = {
    {name = 'role_id', type = 'unsigned', is_nullable = false},
    {name = 'action', type = 'string', is_nullable = false},
}

local function get_actions_space_name()
    local tenant_uid = tenant.uid()
    return tenant.get_space_name(base_actions_space_name, tenant_uid)
end

local function get_actions_space()
    local actions_space_name = get_actions_space_name()
    return box.space[actions_space_name]
end

local function action_is_allowed(role_id, action)
    local space = get_actions_space()
    return space:get({role_id, action}) ~= nil
end

local function flatten(role)
    return {
        box.NULL,
        role.name,
        role.description,
        role.created_at,
        role.is_active,
    }
end

local function unflatten(role)
    if box.tuple.is(role) then
        return role:tomap({names_only = true})
    end

    local result = {}
    for i, field in ipairs(role_format) do
        result[field.name] = role[i]
    end
    return result
end

local function check_tenant(tenant_uid)
    -- default tenant
    if tenant_uid == nil or tenant_uid == 'default' then
        return nil
    end

    local _, err = account_manager_tenant.get(tenant_uid)
    if err ~= nil then
        return nil, err
    end

    return tenant_uid
end

local function get_authority(role_id, tenant_uid)
    local tenant_uid, err = check_tenant(tenant_uid)
    if err ~= nil then
        return nil, err
    end

    local space = get_role_space(tenant_uid)

    local role = space:get({role_id})
    if role == nil then
        return nil, ('Role %s does not exist'):format(role_id)
    end

    local role_actions_count = 0
    local actions_count = 0

    local all_actions = access_actions_list.get()
    for _, action in pairs(all_actions) do
        actions_count = actions_count + 1
        if action_is_allowed(role.id, action) then
            role_actions_count = role_actions_count + 1
        end
    end

    local data_actions_list = vars.data_actions.list()
    for _, data_access_action in ipairs(data_actions_list) do
        actions_count = actions_count + 1
        if action_is_allowed(role.id, data_access_action.id) then
            role_actions_count = role_actions_count + 1
        end
    end

    local authority = 100
    if actions_count ~= 0 then
        authority = math.floor(role_actions_count * 100 / actions_count + 0.5)
    end
    return authority
end

local function is_available(role)
    if role == nil then
        return false
    end
    return role.is_active == true
end

local function get(id, tenant_uid)
    local tenant_uid, err = check_tenant(tenant_uid)
    if err ~= nil then
        return nil, err
    end

    local space = get_role_space(tenant_uid)
    local tuple = space:get({id})
    if not is_available(tuple) then
        return nil, ('Role with id %s is not found'):format(id)
    end
    return unflatten(tuple)
end

local function get_by_name_ci(name, tenant_uid)
    local tenant_uid, err = check_tenant(tenant_uid)
    if err ~= nil then
        return nil, err
    end

    local space = get_role_space(tenant_uid)
    name = name:strip()

    for _, tuple in space.index.name:pairs() do
        if utf8.casecmp(tuple.name, name) == 0 then
            return tuple
        end
    end
end

local function get_by_name(name, tenant_uid)
    local tenant_uid, err = check_tenant(tenant_uid)
    if err ~= nil then
        return nil, err
    end

    local space = get_role_space(tenant_uid)
    local tuple = space.index.name:get({name})

    if not is_available(tuple) then
        return nil, ('Role "%s" is not found'):format(name)
    end
    return unflatten(tuple)
end

local function get_access_actions_list(role_id)
    local space = get_actions_space()
    local result = {}
    for _, tuple in space:pairs({role_id}) do
        table.insert(result, tuple)
    end
    return result
end

local function get_roles_by_access_action(action)
    local result = {}
    local space = get_actions_space()
    for _, tuple in space.index.action:pairs({action}) do
        table.insert(result, tuple.role_id)
    end
    return result
end

local function list(tenant_uid)
    local tenant_uid, err = check_tenant(tenant_uid)
    if err ~= nil then
        return nil, err
    end

    local result = {}
    local space = get_role_space(tenant_uid)
    for _, tuple in space:pairs() do
        if is_available(tuple) then
            table.insert(result, unflatten(tuple))
        end
    end
    return result
end

local function is_system_role(role_name)
    if role_name == 'admin' or role_name == 'supervisor' or role_name == 'user' then
        return true
    end
    return false
end

local function validate_access_actions(actions)
    local available_actions = access_actions_list.get()
    for _, action in ipairs(actions) do
        local id = action.id
        if available_actions[id] == nil and vars.data_actions.get(id) == nil then
            return false, ('Action "%s" does not exist'):format(action.id)
        end
    end
    return true
end

local function get_access_actions(role_id)
    local role = get(role_id)
    if role == nil then
        return nil, ('Role %s does not exist'):format(role_id)
    end

    local all_actions = access_actions_list.get()
    local result = {}
    for _, action in pairs(all_actions) do
        local id = action
        table.insert(result, {
                id = id,
                description = access_actions_list.get_description(id),
                section = access_actions_list.get_section(id),
                type = 'action',
                allowed = action_is_allowed(role_id, action)
        })
    end

    local data_actions_list = vars.data_actions.list()
    for _, data_access_action in ipairs(data_actions_list) do
        local id = data_access_action.id
        table.insert(result, {
            id = id,
            description = data_access_action.description,
            section = sections_list.data_actions,
            type = 'data',
            allowed = action_is_allowed(role_id, id),
        })
    end

    return result
end

local function update_access_actions(role_id, new_actions)
    local role = get(role_id)
    if role == nil then
        return nil, ('Role %s does not exist'):format(role_id)
    end

    if is_system_role(role.name) then
        return nil, ('Unable to update system role %s'):format(role.name)
    end

    local ok, err = validate_access_actions(new_actions)
    if not ok then
        return nil, err
    end

    local space = get_actions_space()
    for _, action in ipairs(new_actions) do
        if action.allowed == true then
            space:replace({role_id, action.id})
        else
            space:delete({role_id, action.id})
        end
    end
    account_manager_server.notify_subscribers('access_actions', role_id)
    return get_access_actions(role_id)
end

local function create(role)
    local space = get_role_space()

    role.name = role.name:strip()
    if #role.name == 0 then
        return nil, 'Failed to create access role: name is required'
    end

    local tuple = get_by_name_ci(role.name)
    if tuple ~= nil and is_available(tuple) then
        return nil, ('Role with name %q already exists'):format(tuple.name)
    end

    role.created_at = clock.time64()
    role.is_active = true
    if role.description == nil then
        role.description = ''
    end

    role = space:insert(flatten(role))
    return unflatten(role)
end

local NAME_FIELDNO = 2
local DESCRIPTION_FIELDNO = 3

local function update(id, updates)
    local space = get_role_space()
    local role = get(id)

    if role == nil then
        return nil, ('Role %s does not exist'):format(id)
    end

    if is_system_role(role.name) == true then
        return nil, ('Unable to update system role %s'):format(role.name)
    end

    local new_name = updates.name:strip()
    if #new_name == 0 then
        return nil, 'Failed to update access role: name is required'
    end

    if new_name ~= nil then
        local test_name_role = get_by_name_ci(new_name)
        if is_available(test_name_role) and test_name_role.id ~= role.id then
            return nil, ('Role with name %q already exists'):format(new_name)
        end
    end

    local update_list = {}
    if new_name ~= nil then
        table.insert(update_list, {'=', NAME_FIELDNO, new_name})
    end

    local new_description = updates.description
    if new_description ~= nil then
        table.insert(update_list, {'=', DESCRIPTION_FIELDNO, new_description})
    end

    if #update_list == 0 then
        return unflatten(role)
    end

    role = space:update({role.id}, update_list)
    account_manager_server.notify_subscribers('access_role', role.id)
    return unflatten(role)
end

local function is_used_by_ldap(role)
    -- ldap doesn't support multitenancy
    if not tenant.is_default() then
        return false
    end

    local ldap = cartridge.config_get_readonly('ldap')
    if ldap == nil then
        return false
    end

    for _, ldap_cfg in ipairs(ldap) do
        for _, ldap_role in ipairs(ldap_cfg.roles) do
            if utf8.casecmp(role.name, ldap_role.role) == 0 then
                return true
            end
        end
    end
    return false
end

local function delete(id)
    local space = get_role_space()
    local role = get(id)

    if not is_available(role) then
        return nil, ('Role %s does not exist'):format(id)
    end

    if is_system_role(role.name) == true then
        return nil, ('Unable to delete system role %q'):format(role.name)
    end

    for _, user in vars.users.iterate() do
        if user.is_deleted == false and user.role_id == id then
            return nil, ('Role %q is used by user %q'):format(role.name, user.email)
        end
    end

    for _, token in vars.tokens.iterate() do
        if token.is_deleted == false and token.role_id == id then
            return nil, ('Role %q is used by token "%s"'):format(role.name, token.name)
        end
    end

    if is_used_by_ldap(role) then
        return nil, ('Role %q is used in LDAP configuration section'):format(role.name)
    end

    local role = space:delete({role.id})
    account_manager_server.notify_subscribers('access_role', role.id)
    return unflatten(role)
end

local SYSTEM_ROLES = {
    -- ONE_TIME_ACCESS - a virtual role for one time access to change password.
    -- It's selected automatically when state == new
    ONE_TIME_ACCESS = -1,
    ADMIN = 1,
    SUPERVISOR = 2,
    USER = 3,
}

local function init_default_roles()
    -- System roles
    local role_space = get_role_space()
    local default_roles = {
        {SYSTEM_ROLES.ADMIN, 'admin', 'Full access', nil, true, false},
        {SYSTEM_ROLES.SUPERVISOR, 'supervisor', 'Full read-only access', nil, true, false},
        {SYSTEM_ROLES.USER, 'user', 'Default access', nil, true, false},
    }

    for _, role in ipairs(default_roles) do
        if role_space.index.name:get({role[2]}) == nil then
            role_space:insert(role)
        end
    end

    -- System actions
    local actions_space = get_actions_space()
    local actions_list = access_actions_list.get()
    local default_actions = {
        -- Audit log
        {SYSTEM_ROLES.ADMIN, actions_list.audit_log_read},
        {SYSTEM_ROLES.ADMIN, actions_list.audit_log_set_severity},
        {SYSTEM_ROLES.ADMIN, actions_list.audit_log_set_enabled},
        {SYSTEM_ROLES.ADMIN, actions_list.audit_log_clear},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.audit_log_read},

        -- Log
        {SYSTEM_ROLES.ADMIN, actions_list.logs_read},
        {SYSTEM_ROLES.ADMIN, actions_list.logs_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.logs_set_config},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.logs_read},

        -- Access roles
        {SYSTEM_ROLES.ADMIN, actions_list.access_roles_get_list},
        {SYSTEM_ROLES.ADMIN, actions_list.access_roles_get},
        {SYSTEM_ROLES.ADMIN, actions_list.access_roles_create},
        {SYSTEM_ROLES.ADMIN, actions_list.access_roles_update},
        {SYSTEM_ROLES.ADMIN, actions_list.access_roles_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.access_role_actions_update},
        {SYSTEM_ROLES.ADMIN, actions_list.access_role_actions_list},
        {SYSTEM_ROLES.ADMIN, actions_list.access_actions_list},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.access_roles_get_list},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.access_roles_get},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.access_role_actions_list},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.access_actions_list},

        -- Cluster config graphql
        {SYSTEM_ROLES.ADMIN, actions_list.config_hard_limits_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_hard_limits_write},
        {SYSTEM_ROLES.ADMIN, actions_list.config_vshard_timeout_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_vshard_timeout_write},
        {SYSTEM_ROLES.ADMIN, actions_list.config_default_keep_version_count_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_default_keep_version_count_write},
        {SYSTEM_ROLES.ADMIN, actions_list.config_force_yield_limits_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_force_yield_limits_write},
        {SYSTEM_ROLES.ADMIN, actions_list.config_graphql_query_cache_size_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_graphql_query_cache_size_write},
        {SYSTEM_ROLES.ADMIN, actions_list.config_connector_inputs_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_connector_inputs_write},
        {SYSTEM_ROLES.ADMIN, actions_list.config_ban_inactive_more_seconds_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_ban_inactive_more_seconds_write},
        {SYSTEM_ROLES.ADMIN, actions_list.config_locked_sections_read},
        {SYSTEM_ROLES.ADMIN, actions_list.config_locked_sections_write},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_hard_limits_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_vshard_timeout_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_default_keep_version_count_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_force_yield_limits_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_graphql_query_cache_size_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_connector_inputs_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_ban_inactive_more_seconds_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.config_locked_sections_read},

        -- Password generator
        {SYSTEM_ROLES.ADMIN, actions_list.password_generator_get_config},
        {SYSTEM_ROLES.ADMIN, actions_list.password_generator_set_config},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.password_generator_get_config},

        -- Users
        {SYSTEM_ROLES.ADMIN, actions_list.user_list},
        {SYSTEM_ROLES.ADMIN, actions_list.user_create},
        {SYSTEM_ROLES.ADMIN, actions_list.user_import},
        {SYSTEM_ROLES.ADMIN, actions_list.user_update},
        {SYSTEM_ROLES.ADMIN, actions_list.user_set_state},
        {SYSTEM_ROLES.ADMIN, actions_list.user_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.user_reset_password},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.user_list},

        -- Tokens
        {SYSTEM_ROLES.ADMIN, actions_list.token_create},
        {SYSTEM_ROLES.ADMIN, actions_list.token_import},
        {SYSTEM_ROLES.ADMIN, actions_list.token_get},
        {SYSTEM_ROLES.ADMIN, actions_list.token_update},
        {SYSTEM_ROLES.ADMIN, actions_list.token_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.token_list},
        {SYSTEM_ROLES.ADMIN, actions_list.token_set_state},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.token_list},

        -- Tenants
        {SYSTEM_ROLES.ADMIN, actions_list.tenant_read},
        {SYSTEM_ROLES.ADMIN, actions_list.tenant_create},
        {SYSTEM_ROLES.ADMIN, actions_list.tenant_update},
        {SYSTEM_ROLES.ADMIN, actions_list.tenant_set_state},
        {SYSTEM_ROLES.ADMIN, actions_list.tenant_delete},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.tenant_read},

        -- TDG config

        {SYSTEM_ROLES.ADMIN, actions_list.tdg_config_load_example},
        {SYSTEM_ROLES.ADMIN, actions_list.tdg_config_read},
        {SYSTEM_ROLES.ADMIN, actions_list.tdg_config_save},
        {SYSTEM_ROLES.ADMIN, actions_list.tdg_config_apply},
        {SYSTEM_ROLES.ADMIN, actions_list.tdg_config_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.tdg_config_change_settings},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.tdg_config_read},

        -- Admin misc

        {SYSTEM_ROLES.ADMIN, actions_list.eval},

        -- Model and expiration

        {SYSTEM_ROLES.ADMIN, actions_list.data_type_write},
        {SYSTEM_ROLES.ADMIN, actions_list.data_type_read},
        {SYSTEM_ROLES.ADMIN, actions_list.expiration_cleanup},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.data_type_read},

        -- notifier
        {SYSTEM_ROLES.ADMIN, actions_list.notifier_user_list},
        {SYSTEM_ROLES.ADMIN, actions_list.notifier_user_set},
        {SYSTEM_ROLES.ADMIN, actions_list.notifier_user_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.notifier_mail_server_set},
        {SYSTEM_ROLES.ADMIN, actions_list.notifier_mail_server_get},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.notifier_user_list},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.notifier_mail_server_get},

        -- repair_queue
        {SYSTEM_ROLES.ADMIN, actions_list.repair_list},
        {SYSTEM_ROLES.ADMIN, actions_list.repair_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.repair_try_again},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.repair_list},

        {SYSTEM_ROLES.USER, actions_list.repair_list},

        -- output_processor
        {SYSTEM_ROLES.ADMIN, actions_list.output_processor_list},
        {SYSTEM_ROLES.ADMIN, actions_list.output_processor_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.output_processor_postprocess_again},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.output_processor_list},

        {SYSTEM_ROLES.USER, actions_list.output_processor_list},

        -- checks section
        {SYSTEM_ROLES.ADMIN, actions_list.cron_syntax},
        {SYSTEM_ROLES.ADMIN, actions_list.storage_dir_writable},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.cron_syntax},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.storage_dir_writable},

        -- storage_jobs
        {SYSTEM_ROLES.ADMIN, actions_list.job_list},
        {SYSTEM_ROLES.ADMIN, actions_list.job_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.job_try_again},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.job_list},

        {SYSTEM_ROLES.USER, actions_list.job_list},

        -- storage_maintenance
        {SYSTEM_ROLES.ADMIN, actions_list.storage_unlinked_spaces_list},
        {SYSTEM_ROLES.ADMIN, actions_list.storage_unlinked_spaces_drop},
        {SYSTEM_ROLES.ADMIN, actions_list.storage_unlinked_spaces_truncate},
        {SYSTEM_ROLES.ADMIN, actions_list.storage_aggregates_get},
        {SYSTEM_ROLES.ADMIN, actions_list.storage_clear},
        {SYSTEM_ROLES.ADMIN, actions_list.storage_spaces_len},
        {SYSTEM_ROLES.ADMIN, actions_list.storage_spaces_drop},


        {SYSTEM_ROLES.SUPERVISOR, actions_list.storage_unlinked_spaces_list},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.storage_aggregates_get},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.storage_spaces_len},

        -- tasks
        {SYSTEM_ROLES.ADMIN, actions_list.task_start},
        {SYSTEM_ROLES.ADMIN, actions_list.task_stop},
        {SYSTEM_ROLES.ADMIN, actions_list.task_list},
        {SYSTEM_ROLES.ADMIN, actions_list.task_result_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.task_config_read},
        {SYSTEM_ROLES.ADMIN, actions_list.task_config_write},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.task_list},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.task_config_read},

        {SYSTEM_ROLES.USER, actions_list.task_start},
        {SYSTEM_ROLES.USER, actions_list.task_stop},
        {SYSTEM_ROLES.USER, actions_list.task_list},
        {SYSTEM_ROLES.USER, actions_list.task_result_delete},

        -- services
        {SYSTEM_ROLES.ADMIN, actions_list.service_read},
        {SYSTEM_ROLES.ADMIN, actions_list.service_write},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.service_read},

        {SYSTEM_ROLES.USER, actions_list.service_read},
        {SYSTEM_ROLES.USER, actions_list.service_write},

        -- view pages
        {SYSTEM_ROLES.ADMIN, actions_list.view_cluster},
        {SYSTEM_ROLES.ADMIN, actions_list.view_test},
        {SYSTEM_ROLES.ADMIN, actions_list.view_graphql},
        {SYSTEM_ROLES.ADMIN, actions_list.view_model},
        {SYSTEM_ROLES.ADMIN, actions_list.view_repair},
        {SYSTEM_ROLES.ADMIN, actions_list.view_logger},
        {SYSTEM_ROLES.ADMIN, actions_list.view_audit_log},
        {SYSTEM_ROLES.ADMIN, actions_list.view_tasks},
        {SYSTEM_ROLES.ADMIN, actions_list.view_settings},
        {SYSTEM_ROLES.ADMIN, actions_list.view_connectors_config},
        {SYSTEM_ROLES.ADMIN, actions_list.view_doc},
        {SYSTEM_ROLES.ADMIN, actions_list.view_config},
        {SYSTEM_ROLES.ADMIN, actions_list.view_data_types},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_cluster},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_test},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_graphql},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_model},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_repair},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_logger},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_audit_log},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_tasks},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_settings},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_connectors_config},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_doc},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_config},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.view_data_types},

        {SYSTEM_ROLES.USER, actions_list.view_test},
        {SYSTEM_ROLES.USER, actions_list.view_graphql},
        {SYSTEM_ROLES.USER, actions_list.view_repair},
        {SYSTEM_ROLES.USER, actions_list.view_tasks},
        {SYSTEM_ROLES.USER, actions_list.view_doc},

        -- cartridge api
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_bootstrap_vshard},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_restart_replication},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_auth_params_write},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_probe_server},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_edit_topology},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_failover_write},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_failover_promote},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_servers_read},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_replicasets_read},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_issues_read},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_suggestions_read},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_config_read},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_config_write},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_config_force_reapply},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_edit_vshard_options},
        {SYSTEM_ROLES.ADMIN, actions_list.cartridge_disable_servers},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.cartridge_servers_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.cartridge_replicasets_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.cartridge_issues_read},
        {SYSTEM_ROLES.SUPERVISOR, actions_list.cartridge_suggestions_read},

        -- data actions
        {SYSTEM_ROLES.ADMIN, actions_list.data_action_read},
        {SYSTEM_ROLES.ADMIN, actions_list.data_action_create},
        {SYSTEM_ROLES.ADMIN, actions_list.data_action_delete},
        {SYSTEM_ROLES.ADMIN, actions_list.data_action_update},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.data_action_read},

        -- account provider
        {SYSTEM_ROLES.ADMIN, actions_list.ldap_config_read},
        {SYSTEM_ROLES.ADMIN, actions_list.ldap_config_write},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.ldap_config_read},

        -- metrics
        {SYSTEM_ROLES.ADMIN, actions_list.metrics_config_read},
        {SYSTEM_ROLES.ADMIN, actions_list.metrics_config_write},

        {SYSTEM_ROLES.SUPERVISOR, actions_list.metrics_config_read},
    }

    for _, action in ipairs(default_actions) do
        if action[2] ~= nil and actions_space:get(action) == nil then
            actions_space:insert(action)
        end
    end
end

local function apply_config()
    vars.users = require('account_manager.user')
    vars.tokens = require('account_manager.token')
    vars.data_actions = require('account_manager.data_actions')
    if box.info.ro then
        return
    end

    local role_space_name = get_role_space_name()

    if box.space[role_space_name] == nil then
        box.begin()
    end

    local role_space = box.schema.space.create(role_space_name, {
        format = role_format,
        if_not_exists = true,
    })

    box.schema.sequence.create(role_sequence_name, {if_not_exists = true})

    role_space:create_index('id', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        sequence = {field = 'id', id = role_sequence_name},
        parts = {{field = 'id', type = 'unsigned'}},
    })

    role_space:create_index('name', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'name', type = 'string'}},
    })

    local actions_space_name = get_actions_space_name()
    local actions_space = box.schema.space.create(actions_space_name, {
        format = actions_format,
        if_not_exists = true,
    })

    actions_space:create_index('role_action', {
        type = 'TREE',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'role_id', type = 'unsigned'}, {field = 'action', type = 'string'}},
    })

    actions_space:create_index('action', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {{field = 'action', type = 'string'}},
    })

    if box.is_in_txn() == false then
        box.begin()
    end

    init_default_roles()

    box.commit()
end

local function validate_config(cfg)
    if type(box.cfg) == 'function' then
        return true
    end

    -- ldap doesn't support multitenancy
    if not tenant.is_default() then
        return true
    end

    if cfg.ldap == nil then
        return true
    end

    -- Cartridge call validate config on all instances.
    -- If instance doesn't have enabled "core" role following check fail.
    -- See https://github.com/tarantool/cartridge/issues/859
    if get_role_space() == nil then
        return true
    end

    for _, ldap_cfg in ipairs(cfg.ldap) do
        for _, ldap_role in ipairs(ldap_cfg.roles) do
            local _, err = get_by_name(ldap_role.role)
            if err ~= nil then
                error(string.format('LDAP configuration validation failed: %s', err))
            end
        end
    end

    return true
end

return {
    validate_config = validate_config,
    apply_config = apply_config,
    SYSTEM_ROLES = SYSTEM_ROLES,

    -- Roles management
    create = create,
    delete = delete,
    update = update,
    get = get,
    get_by_name = get_by_name,
    list = list,
    get_authority = get_authority,

    -- Role actions management
    get_access_actions = get_access_actions,
    update_role_actions = update_access_actions,

    get_access_actions_list = get_access_actions_list,
    get_roles_by_access_action = get_roles_by_access_action,
}
