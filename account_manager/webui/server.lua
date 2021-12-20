local env = require('env')

local auth = require('common.admin.auth')
local access_role = require('account_manager.access_role')

-- List of all available pages
local pages = {
    view_cluster = '/cluster/dashboard',
    view_cartridge_config = '/cluster/configuration',
    view_code = '/cluster/code',
    view_test = '/tdg/test',
    view_graphql = '/tdg/repl',
    view_console = '/tdg/console',
    view_model = '/tdg/model',
    view_repair = '/tdg/repair',
    view_logger = '/tdg/logger',
    view_audit_log = '/tdg/audit-log',
    view_data_types = '/tdg/data-types',
    view_config = '/tdg/files-config',
    view_tasks = '/tdg/tasks',
    view_change_password = '/tdg/change-password',
    view_settings = '/tdg/settings',
    view_connectors_config = '/tdg/connectors',
    view_doc = '/tdg/doc',
}

local default_webui_blacklist = {
    pages.view_change_password,
    pages.view_cartridge_config,
}

if env.dev_mode ~= true then
    table.insert(default_webui_blacklist, pages.view_console)
    table.insert(default_webui_blacklist, pages.view_code)
end

local one_time_access_role_blacklist = {}
for _, page in pairs(pages) do
    if page ~= pages.view_change_password then
        table.insert(one_time_access_role_blacklist, page)
    end
end

local function get_blacklist(role_id)
    if role_id == nil and not auth.is_anonymous_allowed() then
        return {}
    end
    role_id = role_id or access_role.SYSTEM_ROLES.ADMIN

    local actions, err = access_role.get_access_actions(role_id)
    if err ~= nil then
        return nil, err
    end

    if actions == nil then
        return {}
    end

    local blacklist = table.deepcopy(default_webui_blacklist)
    for _, action in pairs(actions) do
        if not action.allowed then
            local url = pages[action.id]
            if url ~= nil then
                table.insert(blacklist, url)
            end
        end
    end
    -- In not dev mode it will be from default_webui_blacklist
    if env.dev_mode and role_id ~= access_role.SYSTEM_ROLES.ADMIN then
        table.insert(blacklist, pages.view_console)
        table.insert(blacklist, pages.view_code)
    end
    return blacklist
end

return {
    get_blacklist = get_blacklist,
    default_webui_blacklist = default_webui_blacklist,
    one_time_access_role_blacklist = one_time_access_role_blacklist,
}
