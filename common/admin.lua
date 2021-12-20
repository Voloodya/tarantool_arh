local module_name = 'common.admin'

local log = require('log.log').new(module_name)

local audit_log = require('audit.log').new(module_name)

local json = require('json')
local yaml = require('yaml').new()
local fio = require('fio')
local env = require('env')
local errors = require('errors')
local membership = require('membership')
local http = require('common.http')
local utils = require('common.utils')
local config_utils = require('common.config_utils')
local zip = require('common.zip')
local cartridge = require('cartridge')
local cartridge_utils = require('cartridge.utils')
local cartridge_vshard_utils = require('cartridge.vshard-utils')
local cartridge_api_config = require('cartridge.webui.api-config')
local cartridge_graphql = require('cartridge.graphql')
local account_provider_graphql = require('account_provider.graphql')
local pool = require('cartridge.pool')
local types = require('graphql.types')
local tenant = require('common.tenant')
local graphql = require('common.graphql')
local account = require('common.admin.account')
local users = require('common.admin.users')
local token_acl = require('common.admin.token_acl')
local tenant_graphql = require('common.admin.tenant')
local access_role = require('account_manager.access_role')
local access_roles = require('common.admin.access_roles')
local access_actions = require('common.admin.access_actions')
local cluster_config = require('common.admin.cluster_config')
local audit_log_graphql = require('audit.graphql')
local logger_graphql = require('log.graphql')
local notifier_graphql = require('notifier.graphql')
local connector_graphql = require('connector.graphql')
local repair_queue_graphql = require('input_processor.repair_queue.graphql')
local output_processor_graphql = require('output_processor.graphql')
local storage_expiration_graphql = require('storage.expiration.graphql')
local storage_jobs_graphql = require('storage.jobs.graphql')
local account_settings_graphql = require('roles.core.account_settings.graphql')
local tenant_settings_graphql = require('roles.core.tenant_settings.graphql')
local maintenance_graphql = require('roles.permanent.maintenance.graphql')
local storage_maintenance_graphql = require('storage.maintenance.graphql')
local storage_migrations_graphql = require('storage.migrations.graphql')
local tasks_graphql = require('tasks.graphql')
local admin_auth = require('common.admin.auth')
local actions_list = require('account_manager.access_actions_list').get()
local password_generator_graphql = require('account_manager.password_generator.graphql')
local config_backup_graphql = require('roles.core.configuration_archive.backup.graphql')
local cartridge_webui = require('cartridge.webui')
local cartridge_webui_api_config = require('cartridge.webui.api-config')
local cartridge_twophase = require('cartridge.twophase')
local account_manager_webui = require('account_manager.webui.server')
local auth = require('common.admin.auth')
local request_context = require('common.request_context')
local data_type_graphql = require('common.data_type')
local metrics_graphql = require('common.metrics.graphql')

local cartridge_graphql_access_error = errors.new_class('cartridge graphql access error')
local graphql_access_error = errors.new_class('graphql access error')
local upload_error = errors.new_class('Config upload failed')
local download_error = errors.new_class('Config upload failed')

yaml.cfg {
    encode_use_tostring = true
}

local function render_doc_file(req)
    local relpath = req.path:match('^/admin/tdg/docs/(.+)$')
    local fullpath = fio.pathjoin(env.binarydir, '_doc_output', relpath)
    return http.render_file(fullpath)
end


local function finalize_error(http_code, err)
    log.error(tostring(err))
    return {
        status = http_code,
        body = json.encode(err),
    }
end

-- https://github.com/tarantool/cartridge/blob/e887629/cartridge/webui/api-config.lua#L127
local function upload_config(clusterwide_config, opts)
    local patch = {}

    for k, _ in pairs(tenant.get_cwcfg():get_plaintext()) do
        if not config_utils.is_system_section(k) and not config_utils.is_locked_section(k) then
            patch[k] = box.NULL
        end
    end

    for k, v in pairs(clusterwide_config:get_plaintext()) do
        if config_utils.is_system_section(k) then
            local err = upload_error:new(
                "uploading system section %q is forbidden", k
            )
            return nil, err
        elseif config_utils.is_locked_section(k) then
            local err = upload_error:new(
                "uploading locked section %q is forbidden", k
            )
            return nil, err
        else
            patch[k] = v
        end
    end

    return tenant.patch_config(patch, opts)
end

local function load_config(path, opts, load_extensions)
    local cw_config, err = config_utils.load_clusterwide_config(path, {load_extensions = load_extensions})
    if err ~= nil then
        return false, err
    end

    local _, err = cartridge_api_config.upload_config(cw_config, opts)
    if err ~= nil then
        return false, err
    end
    return true
end

local function apply_config(dir, comment, opts)
    opts = opts or {}

    local _, err = load_config(dir, opts, tenant.is_default())
    if err ~= nil then
        log.info('TDG config apply failed: %s', err)
        return false, err
    end

    log.info('TDG config applied')
    audit_log.info('TDG config applied')

    local _, err = cartridge.rpc_call('core', 'backup_config_save_current', {comment}, {leader_only = true})
    if err ~= nil then
        log.warn('Configuration was successfully applied but config was not saved to backup: %s', err)
        return false, err
    end

    return true
end

local function upload_config_api(dir, opts)
    local is_initialized_by_api = false
    if request_context.is_empty() then
        request_context.init({})
        is_initialized_by_api = true
    end

    opts = opts or {}
    if opts.token_name ~= nil then
        local ok = auth.authorize_with_token_name(opts.token_name)
        if not ok then
            if is_initialized_by_api then
                request_context.clear()
            end

            return false, upload_error:new("Unknown token name %q", opts.token_name)
        end
    end

    local ok, err = apply_config(dir, opts.comment, opts)

    if is_initialized_by_api then
        request_context.clear()
    end

    if not ok then
        log.error(tostring(err))
        return false, err
    end

    return true
end

local function upload_cartridge_config(body, comment, opts)
    local tempdir, err = config_utils.unzip_data_to_tmpdir(body)
    if err ~= nil then
        return nil, upload_error:new(err)
    end

    local ok, err = apply_config(tempdir, comment, opts)

    fio.rmtree(tempdir)

    if not ok then
        log.error(tostring(err))
        return nil, err
    end
end

local function http_upload_cartridge_config(req)
    if cartridge.config_get_readonly() == nil then
        local err = upload_error:new("Cluster isn't bootstrapped yet")
        return finalize_error(409, err)
    end

    log.info('Try to upload cartridge config')
    audit_log.info('Try to upload cartridge config')

    local body = cartridge_utils.http_read_body(req)
    if body == nil then
        local err = upload_error:new('Invalid request body')
        return finalize_error(400, err)
    end

    local _, err = upload_cartridge_config(body, req.headers['config-comment'])
    if err ~= nil then
        return finalize_error(400, err)
    end

    return {
        status = 200,
        headers = {
            ['content-type'] = "text/html; charset=utf-8"
        },
        body = json.encode('Config applied'),
    }
end

local function pack_code(section, code, dst)
    local ok, err = fio.mktree(fio.pathjoin(dst, fio.dirname(section)))
    if not ok then
        return nil, err
    end

    ok, err = utils.write_file(fio.pathjoin(dst, section), code)
    if not ok then
        return nil, err
    end
end

local function download_tdg_config(_)
    local cfg = tenant.get_cfg_deepcopy()
    if cfg == nil then
        return finalize_error(404, 'Config is not uploaded')
    end

    cfg = config_utils.strip_system_config_sections(cfg)

    local tempdir = fio.tempdir()

    if cfg['types'] then
        local ok, err = utils.write_file(tempdir..'/model.avsc', cfg['types'])
        if not ok then
            fio.rmtree(tempdir)
            return finalize_error(500, err)
        end
        cfg.types = {__file = 'model.avsc'}
    end

    for section, content in pairs(cfg) do
        if section:startswith('src/') then
            local _, err = pack_code(section, content, tempdir)
            if err ~= nil then
                fio.rmtree(tempdir)
                return finalize_error(500, err)
            end

            cfg[section] = nil
        end
    end

    local connector_cfg = {}
    if cfg['connector'] ~= box.NULL then
        connector_cfg = cfg['connector']
    end

    local connector_cfg_input = {}
    if connector_cfg.input ~= box.NULL then
        connector_cfg_input = connector_cfg.input
    end
    for _, input in pairs(connector_cfg_input) do
        if input.wsdl then
            local ok, err = utils.write_file(tempdir..'/WSIBConnect.wsdl', input.wsdl)
            if not ok then
                fio.rmtree(tempdir)
                return finalize_error(500, err)
            end
            input.wsdl = {__file = 'WSIBConnect.wsdl'}
        end
        if input.success_response_body then
            local ok, err = utils.write_file(tempdir..'/success_response_body.xml', input.success_response_body)
            if not ok then
                fio.rmtree(tempdir)
                return finalize_error(500, err)
            end
            input.success_response_body = {__file = 'success_response_body.xml'}
        end
        if input.error_response_body then
            local ok, err = utils.write_file(tempdir..'/error_response_body.xml', input.error_response_body)
            if not ok then
                fio.rmtree(tempdir)
                return finalize_error(500, err)
            end
            input.error_response_body = {__file = 'error_response_body.xml'}
        end
    end

    local ok, err = utils.write_file(tempdir..'/config.yml', yaml.encode(cfg))
    if not ok then
        fio.rmtree(tempdir)
        return finalize_error(500, err)
    end

    local tempzip = fio.pathjoin(tempdir, 'config.zip')
    local _, err = zip.zip(tempzip, tempdir)
    if err ~= nil then
        fio.rmtree(tempdir)
        return finalize_error(500, err)
    end

    log.info('Config zipped')

    local raw, err = utils.read_file(tempzip)
    fio.rmtree(tempdir)
    if not raw then
        return finalize_error(500, err)
    end

    audit_log.info('TDG config downloaded')
    return {
        status = 200,
        headers = {
            ['content-type'] = "application/zip",
            ['content-disposition'] = 'attachment; filename="config.zip"',
        },
        body = raw,
    }
end

local function download_config(_, cfg)
    if cfg == nil then
        return finalize_error(404, 'Config is not uploaded')
    end

    if not tenant.is_default() then
        return download_tdg_config()
    end

    cfg = config_utils.strip_system_config_sections(cfg)

    local tempdir = fio.tempdir()

    for section, content in pairs(cfg) do
        if section:startswith('src/') or section:startswith('extensions/') then
            local _, err = pack_code(section, content, tempdir)
            if err ~= nil then
                fio.rmtree(tempdir)
                return finalize_error(500, err)
            end

            cfg[section] = nil
        end
    end

    local ok, err = utils.write_file(tempdir..'/config.yml', yaml.encode(cfg))
    if not ok then
        fio.rmtree(tempdir)
        return finalize_error(500, err)
    end

    local tempzip = fio.pathjoin(tempdir, 'config.zip')
    local _, err = zip.zip(tempzip, tempdir)
    if err ~= nil then
        fio.rmtree(tempdir)
        return finalize_error(500, err)
    end

    log.info('Config zipped')

    local raw, err = utils.read_file(tempzip)
    fio.rmtree(tempdir)
    if not raw then
        return finalize_error(500, err)
    end

    audit_log.info('Config downloaded')
    return {
        status = 200,
        headers = {
            ['content-type'] = "application/zip",
            ['content-disposition'] = 'attachment; filename="config.zip"',
        },
        body = raw,
    }
end

-- https://github.com/tarantool/cartridge/blob/e887629/cartridge/webui/api-config.lua#L209
local function get_sections(_, args)
    local cwcfg = tenant.get_cwcfg()
    if cwcfg == nil then
        local err = download_error:new("Cluster isn't bootstrapped yet")
        return nil, err
    end

    local ret = {}
    for section, content in pairs(cwcfg:get_plaintext()) do
        if (args.sections == nil or cartridge_utils.table_find(args.sections, section))
            and not config_utils.is_system_section(section)
            and not config_utils.is_locked_section(section) then
            table.insert(ret, {
                filename = section,
                content = content,
            })
        end
    end

    return ret
end

-- https://github.com/tarantool/cartridge/blob/e887629/cartridge/webui/api-config.lua#L233
local function set_sections(_, args)
    if cartridge.config_get_readonly() == nil then
        return nil, upload_error:new("Cluster isn't bootstrapped yet")
    end

    if args.sections == nil then
        args.sections = {}
    end

    local patch = {}
    local query_sections = {}

    for _, input in ipairs(args.sections) do
        if config_utils.is_system_section(input.filename) then
            return nil, upload_error:new(
                "uploading system section %q is forbidden",
                input.filename
            )
        elseif config_utils.is_locked_section(input.filename) then
            return nil, upload_error:new(
                "uploading locked section %q is forbidden",
                input.filename
            )
        end

        patch[input.filename] = input.content or box.NULL
        table.insert(query_sections, input.filename)
    end

    local _, err = tenant.patch_config(patch)
    if err ~= nil then
        return nil, err
    end

    return get_sections(nil, {sections = query_sections})
end

local function load_config_example(_, _)
    local _, err = admin_auth.check_role_has_access(actions_list.tdg_config_load_example)
    if err ~= nil then
        log.error(tostring(err))
        return nil, err
    end

    if not cartridge.is_healthy() then
        log.info('Bootstrapping from first time example config')

        local all = {
            ['vshard-storage'] = true,
            ['vshard-router'] = true,
            ['failover-coordinator'] = true,
            ['connector'] = true,
            ['storage'] = true,
            ['runner'] = true,
            ['core'] = true,
        }

        local _, err = cartridge.admin_join_server({
            uri = membership.myself().uri,
            roles = all,
        })
        if err ~= nil then
            log.error(tostring(err))
            return nil, err
        end

        if cartridge_vshard_utils.can_bootstrap() then
            local _, err = cartridge.admin_bootstrap_vshard()
            if err ~= nil then
                log.error(tostring(err))
                return nil, err
            end
        end
    end

    local config_path =  fio.pathjoin(env.binarydir, 'example', 'config')
    local _, err = apply_config(config_path)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function test_soap_data(_, _)
    return cartridge.config_get_readonly('test-soap-data') or ''
end

local function get_welcome_message(req)
    if req.method ~= 'GET' then
        return { status = 405 }
    end
    return {
        status = 200,
        body = cartridge.config_get_readonly('welcome-message') or ''
    }
end

local function get_webui_blacklist()
    local role_id = account.role_id()

    if role_id == nil then
        -- Cartridge auth is enabled, because anonymous access with authorization enabled is not possible
        return account_manager_webui.default_webui_blacklist
    end

    if role_id == access_role.SYSTEM_ROLES.ONE_TIME_ACCESS then
        return account_manager_webui.one_time_access_role_blacklist
    end

    return cartridge.rpc_call('core', 'get_webui_blacklist', { role_id })
end

local public_cartridge_graphql_actions = {
    ['mutation'] = {},
    ['query'] = {
        ['cluster.webui_blacklist'] = true,
        -- Need for frontend
        ['cluster.self'] = true,
        ['cluster.failover'] = true,
        ['cluster.failover_params'] = true,
        ['cluster.known_roles'] = true,
        ['cluster.vshard_bucket_count'] = true,
        ['cluster.vshard_groups'] = true,
        ['cluster.can_bootstrap_vshard'] = true,
        ['cluster.auth_params'] = true,
        ['cluster.validate_config'] = true,
        ['cluster.vshard_known_groups'] = true, -- Actually it's not supported by TDG
    },
}

local private_cartridge_graphql_actions = {
    ['mutation'] = {
        ['bootstrap_vshard'] = actions_list.cartridge_bootstrap_vshard,
        ['cluster.config'] = actions_list.cartridge_config_write,
        ['cluster.auth_params'] = actions_list.cartridge_auth_params_write,
        ['cluster.failover'] = actions_list.cartridge_failover_write,
        ['cluster.restart_replication'] = actions_list.cartridge_restart_replication,

        ['cluster.edit_topology'] = actions_list.cartridge_edit_topology,
        ['cluster.failover_promote'] = actions_list.cartridge_failover_promote,
        ['cluster.failover_params'] = actions_list.cartridge_failover_write,
        ['cluster.config_force_reapply'] = actions_list.cartridge_config_force_reapply,
        ['cluster.edit_vshard_options'] = actions_list.cartridge_edit_vshard_options,
        ['cluster.disable_servers'] = actions_list.cartridge_disable_servers,
        ['probe_server'] = actions_list.cartridge_probe_server,
        -- deprecated variants of "edit_topology"
        ['join_server'] = actions_list.cartridge_edit_topology,
        ['edit_replicaset'] = actions_list.cartridge_edit_topology,
        ['edit_server'] = actions_list.cartridge_edit_topology,
        ['expel_server'] = actions_list.cartridge_edit_topology,
    },
    ['query'] = {
        ['cluster.config'] = actions_list.cartridge_config_read,
        ['cluster.issues'] = actions_list.cartridge_issues_read,
        ['cluster.suggestions'] = actions_list.cartridge_suggestions_read,
        ['servers'] = actions_list.cartridge_servers_read,
        ['replicasets'] = actions_list.cartridge_replicasets_read,
    }
}

local cartridge_mutation_available_for_non_default_tenant = {
    ['cluster.config'] = true,
}

local function check_cartridge_graphql_access(operation, field)
    local private_operations = private_cartridge_graphql_actions[operation]
    if private_operations == nil then
        local message = string.format('Unknown operation %s', operation)
        local err_obj = cartridge_graphql_access_error:new(message)
        err_obj.graphql_extensions = {code = 400, message = message}
        error(err_obj)
    end

    local is_public_action = public_cartridge_graphql_actions[operation][field]
    if is_public_action == true then
        return
    end

    if operation == 'mutation' and not tenant.is_default() then
        if cartridge_mutation_available_for_non_default_tenant[field] ~= true then
            local message = string.format('%s %q is not supported for non-default tenant', operation, field)
            local err_obj = cartridge_graphql_access_error:new(message)
            err_obj.graphql_extensions = {code = 400, message = message}
            error(err_obj)
        end
    end

    local private_action = private_operations[field]
    if private_action == nil then
        local message = string.format('Unsupported %s "%s"', operation, field)
        local err_obj = cartridge_graphql_access_error:new(message)
        err_obj.graphql_extensions = {code = 400, message = message}
        error(err_obj)
    end

    local _, err = auth.check_role_has_access(private_action)
    if err ~= nil then
        local err_obj = cartridge_graphql_access_error:new(err)
        err_obj.graphql_extensions = {code = 403, message = 'Unauthorized'}
        error(err_obj)
    end
end

local private_graphql_actions = {
    ['mutation'] = {
        ['token.add'] = actions_list.token_create,
        ['token.import'] = actions_list.token_import,
        ['token.update'] = actions_list.token_update,
        ['token.remove'] = actions_list.token_delete,
        ['token.set_state'] = actions_list.token_set_state,

        ['user.add'] = actions_list.user_create,
        ['user.import'] = actions_list.user_import,
        ['user.remove'] = actions_list.user_delete,
        ['user.set_state'] = actions_list.user_set_state,
        ['user.modify'] = actions_list.user_update,
        ['user.reset_password'] = actions_list.user_reset_password,

        ['tenant.delete'] = actions_list.tenant_delete,
        ['tenant.set_state'] = actions_list.tenant_set_state,
        ['tenant.update'] = actions_list.tenant_update,
        ['tenant.create'] = actions_list.tenant_create,

        ['access_role.create'] = actions_list.access_roles_create,
        ['access_role.update'] = actions_list.access_roles_update,
        ['access_role.delete'] = actions_list.access_roles_delete,
        ['access_role.update_access_role_actions'] = actions_list.access_role_actions_update,

        ['data_access_action.create'] = actions_list.data_action_create,
        ['data_access_action.update'] = actions_list.data_action_update,
        ['data_access_action.delete'] = actions_list.data_action_delete,

        ['data_type'] = actions_list.data_type_write,

        ['account_provider.ldap'] = actions_list.ldap_config_write,

        ['cartridge.load_config_example'] = actions_list.tdg_config_load_example,
        ['cartridge.evaluate'] = actions_list.eval,

        ['config.vshard_timeout'] = actions_list.config_vshard_timeout_write,
        ['config.default_keep_version_count'] = actions_list.config_default_keep_version_count_write,
        ['config.hard_limits'] = actions_list.config_hard_limits_write,
        ['config.force_yield_limits'] = actions_list.config_force_yield_limits_write,
        ['config.graphql_query_cache_size'] = actions_list.config_graphql_query_cache_size_write,
        ['config.input_update'] = actions_list.config_connector_inputs_write,
        ['config.input_delete'] = actions_list.config_connector_inputs_write,
        ['config.input_add'] = actions_list.config_connector_inputs_write,
        ['config.ban_inactive_more_seconds'] = actions_list.config_ban_inactive_more_seconds_write,
        ['config.locked_sections_add'] = actions_list.config_locked_sections_write,
        ['config.locked_sections_delete'] = actions_list.config_locked_sections_write,

        ['output_processor.delete_from_list'] = actions_list.output_processor_delete,
        ['output_processor.clear_list'] = actions_list.output_processor_delete,
        ['output_processor.try_again'] = actions_list.output_processor_postprocess_again,
        ['output_processor.try_again_all'] = actions_list.output_processor_postprocess_again,

        ['delete_from_repair_queue'] = actions_list.repair_delete,
        ['clear_repair_queue'] = actions_list.repair_delete,
        ['repair'] = actions_list.repair_try_again,
        ['repair_all'] = actions_list.repair_try_again,

        ['notifier_upsert_user'] = actions_list.notifier_user_set,
        ['notifier_delete_user'] = actions_list.notifier_user_delete,
        ['set_mail_server'] = actions_list.notifier_mail_server_set,

        ['logs.config'] = actions_list.logs_set_config,
        ['logs.truncate'] = actions_list.logs_delete,

        ['audit_log.enabled'] = actions_list.audit_log_set_enabled,
        ['audit_log.severity'] = actions_list.audit_log_set_severity,
        ['audit_log.clear'] = actions_list.audit_log_clear,

        ['tasks.start'] = actions_list.task_start,
        ['tasks.stop'] = actions_list.task_stop,
        ['tasks.forget'] = actions_list.task_result_delete,
        ['tasks.config'] = actions_list.task_config_write,

        ['maintenance.drop_unlinked_spaces'] = actions_list.storage_unlinked_spaces_drop,
        ['maintenance.truncate_unlinked_spaces'] = actions_list.storage_unlinked_spaces_truncate,
        ['maintenance.clear_data'] = actions_list.storage_clear,
        ['maintenance.drop_spaces'] = actions_list.storage_spaces_drop,

        ['jobs.delete_job'] = actions_list.job_delete,
        ['jobs.delete_all_jobs'] = actions_list.job_delete,
        ['jobs.try_again'] = actions_list.job_try_again,
        ['jobs.try_again_all'] = actions_list.job_try_again,

        ['password_generator.config'] = actions_list.password_generator_set_config,

        ['expiration_cleanup'] = actions_list.expiration_cleanup,

        ['backup.config_delete'] = actions_list.tdg_config_delete,
        ['backup.config_save_current'] = actions_list.tdg_config_save,
        ['backup.config_apply'] = actions_list.tdg_config_apply,
        ['backup.settings'] = actions_list.tdg_config_change_settings,

        ['metrics.config'] = actions_list.metrics_config_write,
    },
    ['query'] = {
        ['token.get'] = actions_list.token_get,
        ['token.list'] = actions_list.token_list,

        ['user.list'] = actions_list.user_list,

        ['tenant.get'] = actions_list.tenant_read,
        ['tenant.list'] = actions_list.tenant_read,
        ['tenant.details'] = actions_list.tenant_read,
        ['tenant.details_list'] = actions_list.tenant_read,

        ['access_role.get'] = actions_list.access_roles_get,
        ['access_role.list'] = actions_list.access_roles_get_list,
        ['access_role.actions_list'] = actions_list.access_actions_list,
        ['access_role.get_access_role_actions'] = actions_list.access_role_actions_list,

        ['data_access_action.get'] = actions_list.data_action_read,
        ['data_access_action.list'] = actions_list.data_action_read,
        ['data_type'] = actions_list.data_type_read,

        ['account_provider.ldap'] = actions_list.ldap_config_read,

        ['config.vshard_timeout'] = actions_list.config_vshard_timeout_read,
        ['config.default_keep_version_count'] = actions_list.config_default_keep_version_count_read,
        ['config.hard_limits'] = actions_list.config_hard_limits_read,
        ['config.force_yield_limits'] = actions_list.config_force_yield_limits_read,
        ['config.graphql_query_cache_size'] = actions_list.config_graphql_query_cache_size_read,
        ['config.inputs'] = actions_list.config_connector_inputs_read,
        ['config.ban_inactive_more_seconds'] = actions_list.config_ban_inactive_more_seconds_read,
        ['config.locked_sections_list'] = actions_list.config_locked_sections_read,

        ['output_processor.get_list'] = actions_list.output_processor_list,

        ['repair_list'] = actions_list.repair_list,

        ['notifier_get_users'] = actions_list.notifier_user_list,
        ['get_mail_server'] = actions_list.notifier_mail_server_get,

        ['logs.get'] = actions_list.logs_read,

        ['audit_log.get'] = actions_list.audit_log_read,

        ['tasks.get_list'] = actions_list.task_list,
        ['tasks.config'] = actions_list.task_config_read,

        ['maintenance.unlinked_space_list'] = actions_list.storage_unlinked_spaces_list,
        ['maintenance.get_aggregates'] = actions_list.storage_aggregates_get,
        ['maintenance.spaces_len'] = actions_list.storage_spaces_len,

        ['jobs.get_list'] = actions_list.job_list,

        ['password_generator.config'] = actions_list.password_generator_get_config,

        ['checks.cron_syntax'] = actions_list.cron_syntax,
        ['checks.storage_dir_writable'] = actions_list.storage_dir_writable,

        ['backup.config_list'] = actions_list.tdg_config_read,
        ['backup.config_get'] = actions_list.tdg_config_read,

        ['metrics.config'] = actions_list.metrics_config_read,
    },
}

local public_graphql_actions = {
    ['mutation'] = {
        ['settings.put'] = true,
        ['settings.delete'] = true,
        ['connector.soap_request'] = true,
        ['connector.http_request'] = true,
        ['user.self_modify'] = true,

        -- FIXME: introduce access actions for
        ['migration.apply'] = true,
        ['migration.dry_run'] = true,
        ['migration.stats'] = true,
    },
    ['query'] = {
        ['user.self'] = true,
        ['user.is_anonymous_allowed'] = true,
        ['cartridge.test_soap_data'] = true,
        ['audit_log.enabled'] = true,
        ['logs.config'] = true,
        ['password_generator.generate'] = true,
        ['password_generator.validate'] = true,
        ['settings.get'] = true,
        ['maintenance.current_tdg_version'] = true,
        ['maintenance.clock_delta'] = true,
        ['tenant_settings.get'] = true,

        -- FIXME: introduce access actions for
        ['migration.stats'] = true,
    },
}

local mutation_available_for_non_default_tenant = {
    ['token.add'] = true,
    ['token.import'] = true,
    ['token.update'] = true,
    ['token.remove'] = true,
    ['token.set_state'] = true,

    ['user.add'] = true,
    ['user.import'] = true,
    ['user.remove'] = true,
    ['user.set_state'] = true,
    ['user.modify'] = true,
    ['user.reset_password'] = true,

    ['access_role.create'] = true,
    ['access_role.update'] = true,
    ['access_role.delete'] = true,
    ['access_role.update_access_role_actions'] = true,

    ['data_access_action.create'] = true,
    ['data_access_action.update'] = true,
    ['data_access_action.delete'] = true,

    ['data_type'] = true,

    ['cartridge.load_config_example'] = true,

    ['config.input_update'] = true,
    ['config.input_delete'] = true,
    ['config.input_add'] = true,

    ['output_processor.delete_from_list'] = true,
    ['output_processor.clear_list'] = true,
    ['output_processor.try_again'] = true,
    ['output_processor.try_again_all'] = true,

    ['delete_from_repair_queue'] = true,
    ['clear_repair_queue'] = true,
    ['repair'] = true,
    ['repair_all'] = true,

    ['notifier_upsert_user'] = true,
    ['notifier_delete_user'] = true,
    ['set_mail_server'] = true,

    ['logs.config'] = true,
    ['logs.truncate'] = true,

    ['audit_log.enabled'] = true,
    ['audit_log.severity'] = true,
    ['audit_log.clear'] = true,

    ['tasks.start'] = true,
    ['tasks.stop'] = true,
    ['tasks.forget'] = true,
    ['tasks.config'] = true,

    ['maintenance.drop_unlinked_spaces'] = true,
    ['maintenance.truncate_unlinked_spaces'] = true,
    ['maintenance.clear_data'] = true,
    ['maintenance.drop_spaces'] = true,

    ['jobs.delete_job'] = true,
    ['jobs.delete_all_jobs'] = true,
    ['jobs.try_again'] = true,
    ['jobs.try_again_all'] = true,

    ['backup.config_delete'] = true,
    ['backup.config_save_current'] = true,
    ['backup.config_apply'] = true,
    ['backup.settings'] = true,
}

local function check_graphql_access(operation, field, schema)
    if schema ~= 'admin' then
        return
    end

    local private_operations = private_graphql_actions[operation]
    if private_operations == nil then
        local message = string.format('Unknown operation %s', operation)
        local err_obj = graphql_access_error:new(message)
        err_obj.message = message
        err_obj.code = 400
        error(err_obj)
    end

    local is_public_action = public_graphql_actions[operation][field]
    if is_public_action == true then
        return
    end

    if operation == 'mutation' and not tenant.is_default() then
        if mutation_available_for_non_default_tenant[field] ~= true then
            local message = string.format('%s %q is not supported for non-default tenant', operation, field)
            local err_obj = graphql_access_error:new(message)
            err_obj.message = message
            err_obj.code = 400
            error(err_obj)
        end
    end

    local private_action = private_operations[field]
    if private_action == nil then
        local message = string.format('Unsupported %s "%s"', operation, field)
        local err_obj = graphql_access_error:new(message)
        err_obj.message = message
        err_obj.code = 400
        error(err_obj)
    end

    local _, err = auth.check_role_has_access(private_action)
    if err ~= nil then
        local err_obj = graphql_access_error:new(err)
        err_obj.message = 'Unauthorized'
        err_obj.code = 403
        error(err_obj)
    end
end

local function init()
    users.init()
    token_acl.init()
    access_roles.init()
    access_actions.init()
    cluster_config.init()

    local httpd = cartridge.service_get('httpd')
    http.add_route(httpd, { public = true,
                     path = '/admin/tdg/docs/.*' }, 'common.admin', 'render_doc_file')
    http.add_route(httpd, { public = true,
                     path = '/welcome' }, 'common.admin', 'get_welcome_message')

    cartridge_graphql.on_resolve(check_cartridge_graphql_access)
    cartridge_twophase.on_patch(tenant.patch_clusterwide_config)
    graphql.on_resolve(check_graphql_access)

    graphql.add_mutation_prefix('admin', 'cartridge', 'Cartridge api')
    graphql.add_callback_prefix('admin', 'cartridge', 'Cartridge api')

    graphql.add_mutation(
        {schema='admin',
         prefix='cartridge',
         name='load_config_example',
         callback='common.admin.load_config_example',
         kind=types.boolean,
         args={},
         doc='Loads example config'})

    graphql.add_callback({
            schema='admin',
            prefix = 'cartridge',
            name = 'test_soap_data',
            doc = 'Returns test soap data',
            args = {},
            kind = types.string,
            callback = 'common.admin.test_soap_data',
    })

    if env.dev_mode then
        graphql.add_mutation({
                schema = 'admin',
                prefix = 'cartridge',
                name = 'evaluate',
                doc = 'Returns evaluated string on local or remote node',
                args = {eval=types.string, uri=types.string},
                kind = types.string,
                callback = 'common.admin.graphql_evaluate',
        })
    end

    audit_log_graphql.init()
    logger_graphql.init()
    notifier_graphql.init()
    connector_graphql.init()
    repair_queue_graphql.init()
    output_processor_graphql.init()
    storage_expiration_graphql.init()
    storage_jobs_graphql.init()
    maintenance_graphql.init()
    storage_maintenance_graphql.init()
    storage_migrations_graphql.init()
    tasks_graphql.init()
    password_generator_graphql.init()
    config_backup_graphql.init()
    tenant_graphql.init()
    account_settings_graphql.init()
    tenant_settings_graphql.init()
    account_provider_graphql.init()
    data_type_graphql.init()
    metrics_graphql.init()

    -- @monkeypatch
    -- Define our webui blacklist rules
    cartridge_webui.get_blacklist = get_webui_blacklist

    -- @monkeypatch
    -- To support multitenancy for "Code" page
    cartridge_webui_api_config.upload_config = upload_config
    cartridge_webui_api_config.set_sections = set_sections
    cartridge_webui_api_config.get_sections = get_sections

    --[[
        Redirect cartridge config routes to input_processor
        Remove it when cartridge config management includes missed functionality
          - tar.gz
          - multifile
    ]]

    local http_instance = cartridge.service_get('httpd')
    for _, route in ipairs(http_instance.routes) do
        if route.path == '/admin/config' then
            if route.method == 'GET' then
                route.public = false
                route.sub = function(req)
                    local _, err = admin_auth.check_role_has_access(actions_list.tdg_config_read)
                    if err ~= nil then
                        return {
                            status = 403,
                            body = json.encode(err),
                        }
                    end

                    local resp = download_config(req, cartridge.config_get_deepcopy())
                    return cartridge.http_render_response(resp)
                end
            elseif route.method == 'PUT' then
                route.public = false
                route.sub = function(req)
                    local _, err = admin_auth.check_role_has_access(actions_list.tdg_config_apply)
                    if err ~= nil then
                        return {
                            status = 403,
                            body = json.encode(err),
                        }
                    end

                    local resp = http_upload_cartridge_config(req)
                    return cartridge.http_render_response(resp)
                end
            else
                return {
                    status = 405,
                }
            end
            -- API
        elseif route.path == '/admin/api' then
            route.public = false
             -- Frontend
        elseif route.path == '/admin' then
            route.public = true
        end
    end
end

local function eval_format(status, ...)
    local err
    if status then
        -- serializer can raise an exception
        local args_rest = {...}
        if #args_rest == 0 then
            return '---\n...\n'
        end
        status, err = pcall(yaml.encode, {...})
        if status then
            return err
        else
            err = 'console: an exception occurred when formatting the output: '..
                tostring(err)
        end
    else
        err = ...
        if err == nil then
            err = box.NULL
        end
    end
    return yaml.encode({ error = err })
end


local function eval(account_name, line)
    log.info('%s evaluate the line %q', account_name, line)

    local funcall, errmsg = load("return "..line)
    if not funcall then
        funcall, errmsg = load(line)
    end

    local result
    if funcall then
        result = eval_format(pcall(funcall))
    else
        result = eval_format(false, errmsg)
    end

    log.info('%s got the eval result:\n%s', account_name, result)
    audit_log.info('Result of eval %q:\n%s', line, result)

    return result
end

local function graphql_evaluate(_, args)
    local account_name = account.tostring()
    if account.id() ~= nil then
        account_name = ('%s (id: %q)'):format(account_name, account.id())
    end

    -- Local call
    if args.uri == nil then
        return eval(account_name, args.eval)
    end

    -- Or remote call
    log.info('%s remotely evaluate the line %q', account_name, args.eval)
    local result
    local conn, err = pool.connect(args.uri)
    if conn == nil then
        result = eval_format(false, err)
        log.info('%s remotely got the eval result:\n%s', account_name, result)
        return result
    end
    local rc, err = conn:call('admin.eval', {account_name, args.eval})
    if rc == nil then
        result = eval_format(false, err)
        log.info('%s remotely got the eval result:\n%s', account_name, result)
        return result
    end

    log.info('%s remotely got the eval result:\n%s', account_name, rc)
    return rc
end

_G.admin = {
    eval = eval,
    upload_config_api = upload_config_api,
}

return {
    init = init,
    render_doc_file = render_doc_file,

    http_upload_config = http_upload_cartridge_config,
    upload_config = upload_cartridge_config,
    download_config = download_config,

    load_config_example = load_config_example,
    test_soap_data = test_soap_data,

    eval = eval,
    graphql_evaluate = graphql_evaluate,
    get_welcome_message = get_welcome_message,

    -- for tests
    private_cartridge_graphql_actions = private_cartridge_graphql_actions,
    public_cartridge_graphql_actions = public_cartridge_graphql_actions,
}
