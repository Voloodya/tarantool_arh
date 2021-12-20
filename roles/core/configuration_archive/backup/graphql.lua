local auth = require('common.admin.auth')
local actions_list = require('account_manager.access_actions_list').get()
local http = require('common.http')
local json = require('json')
local cartridge = require('cartridge')
local cartridge_clusterwide_config = require('cartridge.clusterwide-config')
local types = require('graphql.types')
local graphql = require('common.graphql')
local tenant = require('common.tenant')

local config_backup_section_schema = types.object{
    name = 'Config_backup_section_schema',
    fields = {
        section = types.string.nonNull,
        content = types.string.nonNull,
    },
    description = 'Config section in yaml format',
}

local config_backup_version_schema = types.object{
    name = 'Config_backup_version_schema',
    fields = {
        version = types.long.nonNull,
        timestamp = types.string.nonNull,
        comment = types.string,
        active = types.boolean,
        uploaded_by = types.string,
    },
    description = 'Config metadata (version and timestamp)',
}

local configuration_settings_schema = types.object{
    name = 'Config_backup_settings_schema',
    fields = {
        keep_config_count = types.long,
    },
    description = 'Config backup settings schema',
}

local function format_result(result)
    local list = {}
    for section, content in pairs(result) do
        table.insert(list, {
            section = section,
            content = content,
        })
    end
    return list
end

local function config_get(_, arg)
    local version = tonumber64(arg['version'])
    if version == nil then
        return nil, 'version expected to be a number'
    end

    local result, err = cartridge.rpc_call('core', 'backup_config_get', {version})
    if err ~= nil then
        return nil, err
    end

    return format_result(result)
end

local function config_delete(_, arg)
    local version = tonumber64(arg['version'])
    if version == nil then
        return nil, 'version expected to be a number'
    end

    local result, err = cartridge.rpc_call('core', 'backup_config_delete', {version}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end

    return format_result(result)
end

local function config_list()
    local result, err = cartridge.rpc_call('core', 'backup_config_list', {})
    if err ~= nil then
        return nil, err
    end

    -- Workaround for https://github.com/tarantool/cartridge/issues/1090
    for _, data in ipairs(result) do
        data.timestamp = tostring(data.timestamp):gsub('U*LL', '')
    end

    return result
end

local function config_apply(_, arg)
    local version = tonumber64(arg['version'])
    if version == nil then
        return nil, 'version expected to be a number'
    end

    local result, err = cartridge.rpc_call('core', 'backup_config_apply', {version}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end

    return format_result(result)
end

local function config_save_current(_, arg)
    local comment = arg.comment
    local result, err = cartridge.rpc_call('core', 'backup_config_save_current', {comment}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end

    return format_result(result)
end

local function settings(_, arg)
    local section = tenant.get_cfg_deepcopy('backup') or {}

    -- Check type directly here:
    --   * nil - an argument is not specified - skip
    --   * null - setup value of an argument to box.NULL
    if type(arg['keep_config_count']) ~= 'nil' then
        section.keep_config_count = arg['keep_config_count']
    end

    local _, err = tenant.patch_config({backup = section})
    if err ~= nil then
        return nil, err
    end

    return tenant.get_cfg_deepcopy('backup') or {}
end

local function download_config_http(req)
    local _, err = auth.check_role_has_access(actions_list.tdg_config_read)
    if err ~= nil then
        return {
            status = 403,
            body = json.encode(err),
        }
    end

    local admin = require('common.admin')

    local version = req:stash('version')
    version = tonumber(version)
    if version == nil then
        return {
            status = 400,
            body = json.encode('"version" expected to be a number'),
        }
    end

    local sections, err = cartridge.rpc_call('core', 'backup_config_get', {version})
    if err ~= nil then
        return {
            status = 400,
            body = json.encode(err),
        }
    end

    local cfg, err = cartridge_clusterwide_config.new(sections)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode(err),
        }
    end

    local resp = admin.download_config(req, cfg:get_deepcopy())
    return cartridge.http_render_response(resp)
end

local function init()
    graphql.add_mutation_prefix('admin', 'backup', 'Backup management')
    graphql.add_callback_prefix('admin', 'backup', 'Backup management')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'backup',
        name = 'config_list',
        doc = 'Returns list of versions of applied configuration',
        args = {},
        kind = types.list(config_backup_version_schema),
        callback = 'roles.core.configuration_archive.backup.graphql.config_list',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'backup',
        name = 'config_get',
        doc = 'Returns config of applied configuration with specified version',
        args = {version = types.long.nonNull},
        kind = types.list(config_backup_section_schema),
        callback = 'roles.core.configuration_archive.backup.graphql.config_get',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'backup',
        name = 'config_delete',
        doc = 'Delete config of applied configuration with specified version',
        args = {version = types.long.nonNull},
        kind = types.list(config_backup_section_schema),
        callback = 'roles.core.configuration_archive.backup.graphql.config_delete',
    })

    graphql.add_mutation({
        schema='admin',
        prefix = 'backup',
        name = 'config_save_current',
        doc = 'Save config to backup. Promote backup to the normal one',
        args = {comment = types.string},
        kind = types.list(config_backup_section_schema),
        callback = 'roles.core.configuration_archive.backup.graphql.config_save_current',
    })

    graphql.add_mutation({
        schema='admin',
        prefix = 'backup',
        name = 'config_apply',
        doc = 'Apply specified config version',
        args = {version=types.long.nonNull},
        kind = types.list(config_backup_section_schema),
        callback = 'roles.core.configuration_archive.backup.graphql.config_apply',
    })

    graphql.add_mutation({
        schema='admin',
        prefix = 'backup',
        name = 'settings',
        doc = 'Change backup configuration',
        args = {keep_config_count=types.long},
        kind = configuration_settings_schema,
        callback = 'roles.core.configuration_archive.backup.graphql.settings',
    })

    local httpd = cartridge.service_get('httpd')

    http.add_route(httpd, {public = false, path = '/backup/config/version/:version', method = 'GET'},
        'roles.core.configuration_archive.backup.graphql', 'download_config_http')
end

return {
    init = init,

    config_list = config_list,
    config_get = config_get,
    config_apply = config_apply,
    config_delete = config_delete,
    settings = settings,
    download_config_http = download_config_http,
    config_save_current = config_save_current,
}
