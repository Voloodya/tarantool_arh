local env = require('env')

local sections = {
    admin = 'Admin',
    configuration = 'Configuration',
    logs = 'Logs',
    audit_logs = 'Audit Logs',
    notifier = 'Notifier',
    roles = 'Roles',
    users = 'Users',
    tokens = 'Tokens',
    tenants = 'Tenants',
    data_action_management = 'Data Actions Management',
    data_type_management = 'Data Type Management',
    model = 'Model',
    storage = 'Storage',
    checks = 'Checks',
    repair_queue = 'Repair Queue',
    output_processor_list = 'Output Processor List',
    jobs_list = 'Jobs List',
    tasks = 'Tasks',
    services = 'Services',
    view_pages = 'Pages Access',
    cartridge_actions = 'Cartridge Actions',
    data_actions = 'Data Actions',
    account_provider = 'Access provider',
}

local actions = {
    -- audit_log
    audit_log_read = {
        section = sections.audit_logs,
        description = "Show the audit log"
    },
    audit_log_clear = {
        section = sections.audit_logs,
        description = "Clear the audit log"
    },
    audit_log_set_enabled = {
        section = sections.audit_logs,
        description = "Enable the audit log"
    },
    audit_log_set_severity = {
        section = sections.audit_logs,
        description = "Set the lowest severity level for audit log messages"
    },

    -- admin.access_roles
    access_roles_get_list = {
        section = sections.roles,
        description = "Show all access roles"
    },
    access_roles_get = {
        section = sections.roles,
        description = "Read the role information"
    },
    access_roles_create = {
        section = sections.roles,
        description = "Create access-related roles"
    },
    access_roles_update = {
        section = sections.roles,
        description = "Change access-related roles"
    },
    access_roles_delete = {
        section = sections.roles,
        description = "Delete access-related roles"
    },
    access_role_actions_update = {
        section = sections.roles,
        description = "Change settings for access-related roles"
    },
    access_role_actions_list = {
        section = sections.roles,
        description = "Show all actions allowed for the access-related role"
    },
    access_actions_list = {
        section = sections.roles,
        description = "Show access-related actions"
    },

    -- cluster.config
    config_hard_limits_read = {
        section = sections.configuration,
        description = "Read the hard limits"
    },
    config_hard_limits_write = {
        section = sections.configuration,
        description = "Set the hard limits"
    },
    config_vshard_timeout_read = {
        section = sections.configuration,
        description = "Read the timeout for vshard requests"
    },
    config_vshard_timeout_write = {
        section = sections.configuration,
        description = "Set the timeout for vshard requests"
    },
    config_default_keep_version_count_read = {
        section = sections.configuration,
        description = "Read default keep_version_count parameter for versioning"
    },
    config_default_keep_version_count_write = {
        section = sections.configuration,
        description = "Set default keep_version_count parameter for versioning"
    },
    config_force_yield_limits_read = {
        section = sections.configuration,
        description = "Read the force yield limit"
    },
    config_force_yield_limits_write = {
        section = sections.configuration,
        description = "Set the force yield limit"
    },
    config_graphql_query_cache_size_read = {
        section = sections.configuration,
        description = "Read the cache size for GraphQL queries"
    },
    config_graphql_query_cache_size_write = {
        section = sections.configuration,
        description = "Set the cache size for GraphQL queries"
    },
    config_connector_inputs_read = {
        section = sections.configuration,
        description = "Read the input section in config"
    },
    config_connector_inputs_write = {
        section = sections.configuration,
        description = "Update the input section in config"
    },
    config_ban_inactive_more_seconds_read = {
        section = sections.configuration,
        description = "Read time limit for inactive users and tokens in config"
    },
    config_ban_inactive_more_seconds_write = {
        section = sections.configuration,
        description = "Update time limit for inactive users and tokens in config"
    },
    config_locked_sections_read = {
        section = sections.configuration,
        description = "Read the locked sections in config"
    },
    config_locked_sections_write = {
        section = sections.configuration,
        description = "Write the locked sections in config"
    },

    -- password generator
    password_generator_get_config = {
        section = sections.configuration,
        description = "Show the settings of the password generator"
    },
    password_generator_set_config = {
        section = sections.configuration,
        description = "Change the settings of the password generator"
    },

    -- users
    user_create = {
        section = sections.users,
        description = "Create a new user"
    },
    user_import = {
        section = sections.users,
        description = "Import a user"
    },
    user_update = {
        section = sections.users,
        description = "Update user information"
    },
    user_delete = {
        section = sections.users,
        description = "Delete the user"
    },
    user_list = {
        section = sections.users,
        description = "Show all users"
    },
    user_set_state = {
        section = sections.users,
        description = "Change the user's state"
    },
    user_reset_password = {
        section = sections.users,
        description = "Reset user password"
    },

    -- tokens
    token_create = {
        section = sections.tokens,
        description = "Create a new token"
    },
    token_import = {
        section = sections.tokens,
        description = "Import a token"
    },
    token_get = {
        section = sections.tokens,
        description = "Get the token"
    },
    token_update = {
        section = sections.tokens,
        description = "Update the token"
    },
    token_delete = {
        section = sections.tokens,
        description = "Delete the token"
    },
    token_list = {
        section = sections.tokens,
        description = "Show all tokens"
    },
    token_set_state = {
        section = sections.tokens,
        description = "Change the token state"
    },

    -- tenants
    tenant_read = {
        section = sections.tenants,
        description = "Show tenants information"
    },
    tenant_create = {
        section = sections.tenants,
        description = "Create new tenants"
    },
    tenant_update = {
        section = sections.tenants,
        description = "Update tenants"
    },
    tenant_set_state = {
        section = sections.tenants,
        description = "Set tenant's state"
    },
    tenant_delete = {
        section = sections.tenants,
        description = "Delete tenants"
    },

    -- tdg config
    tdg_config_load_example = {
        section = sections.configuration,
        description = "Load sample configuration"
    },
    tdg_config_read = {
        section = sections.configuration,
        description = "Read TDG configuration"
    },
    tdg_config_save = {
        section = sections.configuration,
        description = "Save TDG configuration"
    },
    tdg_config_delete = {
        section = sections.configuration,
        description = "Delete stored configurations from space"
    },
    tdg_config_apply = {
        section = sections.configuration,
        description = "Apply configuration"
    },
    tdg_config_change_settings = {
        section = sections.configuration,
        description = "Change current configuration"
    },

    -- logger
    logs_read = {
        section = sections.logs,
        description = "Show logs"
    },
    logs_delete = {
        section = sections.logs,
        description = "Clear logs"
    },
    logs_set_config = {
        section = sections.logs,
        description = "Change configuration of logging"
    },

    -- notifier
    notifier_user_list = {
        section = sections.notifier,
        description = "Show all users subscribed to notifications"
    },
    notifier_user_set = {
        section = sections.notifier,
        description = "Specify a user subscribed to notifications"
    },
    notifier_user_delete = {
        section = sections.notifier,
        description = "Delete users subscribed to notifications"
    },
    notifier_mail_server_get = {
        section = sections.notifier,
        description = "Show the mail servers for sending notifications"
    },
    notifier_mail_server_set = {
        section = sections.notifier,
        description = "Specify the mail server for sending notifications"
    },

    -- repair_queue
    repair_list = {
        section = sections.repair_queue,
        description = "Show all objects in the repair queue"
    },
    repair_delete = {
        section = sections.repair_queue,
        description = "Delete objects in the repair queue"
    },
    repair_try_again = {
        section = sections.repair_queue,
        description = "Retry processing objects in the repair queue"
    },

    -- output_processor
    output_processor_list = {
        section = sections.output_processor_list,
        description = "Show all tasks sent to the output processor (state + history)"
    },
    output_processor_delete = {
        section = sections.output_processor_list,
        description = "Delete tasks in the output processor"
    },
    output_processor_postprocess_again = {
        section = sections.output_processor_list,
        description = "Restart post processing in the output processor"
    },

    -- checks section
    cron_syntax = {
        section = sections.checks,
        description = "Check syntax of cron expression"
    },
    storage_dir_writable = {
        section = sections.checks,
        description = "Check write permissions for directory"
    },

    -- storage_jobs
    job_list = {
        section = sections.jobs_list,
        description = "Show all jobs"
    },
    job_delete = {
        section = sections.jobs_list,
        description = "Delete the job"
    },
    job_try_again = {
        section = sections.jobs_list,
        description = "Try executing the job again"
    },

    -- storage_maintenance
    storage_unlinked_spaces_list = {
        section = sections.storage,
        description = "Show all spaces not linked from anywhere"
    },
    storage_unlinked_spaces_drop = {
        section = sections.storage,
        description = "Delete spaces not linked from anywhere"
    },
    storage_unlinked_spaces_truncate = {
        section = sections.storage,
        description = "Remove data from spaces not linked from anywhere"
    },
    storage_aggregates_get = {
        section = sections.storage,
        description = "Show all aggregates"
    },
    storage_spaces_len = {
        section = sections.storage,
        description = "Show length of all storage spaces",
    },
    storage_spaces_drop = {
        section = sections.storage,
        description = "Drop all spaces"
    },

    -- tasks
    task_start = {
        section = sections.tasks,
        description = "Run the task",
    },
    task_stop = {
        section = sections.tasks,
        description = "Stop the task",
    },
    task_list = {
        section = sections.tasks,
        description = "Show the tasks with statuses",
    },
    task_result_delete = {
        section = sections.tasks,
        description = "Delete the result of a task",
    },
    task_config_read = {
        section = sections.tasks,
        description = "Show tasks config",
    },
    task_config_write = {
        section = sections.tasks,
        description = "Edit tasks config",
    },

    -- services
    service_read = {
        section = sections.services,
        description = "Allow to run read services (query)"
    },
    service_write = {
        section = sections.services,
        description = "Allow to run write services (mutation)"
    },

    -- view pages
    view_cluster = {
        section = sections.view_pages,
        description = "Show cluster page"
    },
    view_test = {
        section = sections.view_pages,
        description = "Show test page"
    },
    view_graphql = {
        section = sections.view_pages,
        description = "Show graphql page"
    },
    view_model = {
        section = sections.view_pages,
        description = "Show model page"
    },
    view_repair = {
        section = sections.view_pages,
        description = "Show repair pages"
    },
    view_logger = {
        section = sections.view_pages,
        description = "Show logger page"
    },
    view_audit_log = {
        section = sections.view_pages,
        description = "Show audit_log page"
    },
    view_tasks = {
        section = sections.view_pages,
        description = "Show tasks page"
    },
    view_settings = {
        section = sections.view_pages,
        description = "Show settings page"
    },
    view_connectors_config = {
        section = sections.view_pages,
        description = "Show connectors configuration page"
    },
    view_doc = {
        section = sections.view_pages,
        description = "Show doc page"
    },
    view_config = {
        section = sections.view_pages,
        description = "Show configuration page"
    },
    view_data_types = {
        section = sections.view_pages,
        description = "Show data types page"
    },

    -- cartridge api
    cartridge_bootstrap_vshard = {
        section = sections.cartridge_actions,
        description = "Allow to bootstrap vshard"
    },
    cartridge_restart_replication = {
        section = sections.cartridge_actions,
        description = "Allow to restart replication"
    },
    cartridge_auth_params_write = {
        section = sections.cartridge_actions,
        description = "Allow to modify auth parameters"
    },
    cartridge_probe_server = {
        section = sections.cartridge_actions,
        description = "Allow to probe server"
    },
    cartridge_edit_topology = {
        section = sections.cartridge_actions,
        description = "Allow to edit cluster topology server"
    },
    cartridge_failover_write = {
        section = sections.cartridge_actions,
        description = "Allow to modify failover parameters"
    },
    cartridge_failover_promote = {
        section = sections.cartridge_actions,
        description = "Allow to promote instance to the leader of replicaset"
    },
    cartridge_servers_read = {
        section = sections.cartridge_actions,
        description = "Show cluster servers"
    },
    cartridge_replicasets_read = {
        section = sections.cartridge_actions,
        description = "Show replicasets information"
    },
    cartridge_issues_read = {
        section = sections.cartridge_actions,
        description = "Show cluster problems"
    },
    cartridge_suggestions_read = {
        section = sections.cartridge_actions,
        description = "Show cluster management suggestions"
    },
    cartridge_config_force_reapply = {
        section = sections.cartridge_actions,
        description = "Allow to reapply config on the specified nodes"
    },
    cartridge_edit_vshard_options = {
        section = sections.cartridge_actions,
        description = "Allow to edit vshard options"
    },
    cartridge_disable_servers = {
        section = sections.cartridge_actions,
        description = "Allow to disable listed servers"
    },

    -- data access actions
    data_action_create = {
        section = sections.data_action_management,
        description = "Allow to create new data action"
    },
    data_action_update = {
        section = sections.data_action_management,
        description = "Allow to update data action"
    },
    data_action_delete = {
        section = sections.data_action_management,
        description = "Allow to delete data action"
    },
    data_action_read = {
        section = sections.data_action_management,
        description = "Allow to read data actions"
    },

    -- Model and expiration
    data_type_read = {
        section = sections.data_type_management,
        description = "Allow to read model and versioning configuration"
    },
    data_type_write = {
        section = sections.data_type_management,
        description = "Allow to modify model and versioning configuration"
    },
    expiration_cleanup = {
        section = sections.data_type_management,
        description = "Clean up expired data"
    },

    -- account_provider
    ldap_config_read = {
        section = sections.account_provider,
        description = "Show LDAP configuration",
    },
    ldap_config_write = {
        section = sections.account_provider,
        description = "Update LDAP configuration",
    },

    -- metrics
    metrics_config_read = {
        section = sections.configuration,
        description = "Show metrics configuration",
    },
    metrics_config_write = {
        section = sections.configuration,
        description = "Update metrics configuration",
    },
}

if env.dev_mode == true then
    local dev_mode_actions = {
        -- admin misc
        eval = {
            section = sections.admin,
            description = "Execute the console commands"
        },

        -- storage_maintenance
        storage_clear = {
            section = sections.storage,
            description = "Clean up all data"
        },

        -- cartridge api
        cartridge_config_read = {
            section = sections.cartridge_actions,
            description = "Show cluster config"
        },
        cartridge_config_write = {
            section = sections.cartridge_actions,
            description = "Modify cluster config"
        },
    }

    for k, v in pairs(dev_mode_actions) do
        actions[k] = v
    end
end

local action_ids = {}
for k in pairs(actions) do
    action_ids[k] = k
end

local function get()
    return action_ids
end

local function get_description(id)
    return actions[id].description
end

local function get_section(id)
    return actions[id].section
end

local function get_sections()
    return sections
end

return {
    get = get,
    get_description = get_description,
    get_section = get_section,
    get_sections = get_sections,
}
