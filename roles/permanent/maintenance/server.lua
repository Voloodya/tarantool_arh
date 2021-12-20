local module_name = 'maintenance.server'

local cartridge = require('cartridge')

local errors = require('errors')
local checks = require('checks')
local cartridge_issues = require('cartridge.issues')

local config_error = errors.new_class('invalid maintenance config')

local vars = require('common.vars').new(module_name)
local config_checks = require('common.config_checks').new(config_error)

vars:new('clock_delta_threshold_warning', 1/0)

local function role_is_enabled(roles, name)
    return roles[name] == true
end

local function validate_config(cfg)
    checks('table')

    local replicasets = cfg.topology and cfg.topology.replicasets or {}
    for _, replicaset in pairs(replicasets) do
        local roles = replicaset.roles
        if role_is_enabled(roles, 'vshard-storage') and not role_is_enabled(roles, 'storage') then
            error(config_error:new('Unable to enable "vshard-storage" role without "storage"'))
        end

        config_error:assert(replicaset.all_rw ~= true, 'Usage of "all writable" option is prohibited')
    end

    local conf = cfg.maintenance

    -- Skip all the checks if cartridge_issues_limits is skipped
    if not conf then
        return true
    end

    -- Check cartridge.issues values
    config_checks:check_optional_luatype('maintenance.cartridge_issue_limits',
        conf.cartridge_issue_limits, 'table')
    local res, err = cartridge_issues.validate_limits(conf.cartridge_issue_limits)
    if res == nil then
        error(err)
    end

    return true
end

local function apply_config(cfg)
    checks('table')

    -- Get issues_limits from config
    local issues_conf = {}
    if cfg.maintenance and cfg.maintenance.cartridge_issue_limits then
        issues_conf = cfg.maintenance.cartridge_issue_limits
    end

    -- Cache clock_delta_threshold_warning for graphql calls
    vars.clock_delta_threshold_warning = issues_conf.clock_delta_threshold_warning
        or cartridge_issues.default_limits.clock_delta_threshold_warning
    -- Pass new limits to cartridge
    cartridge_issues.set_limits(issues_conf)
end

local function get_max_clock_delta()
    local servers = cartridge.admin_get_servers()

    local max_positive_delta = 0
    local max_negative_delta = 0

    for _, server in pairs(servers) do
        local delta = server.clock_delta or 0
        if delta > max_positive_delta then
            max_positive_delta = delta
        elseif delta < max_negative_delta then
            max_negative_delta = delta
        end
    end

    local max_delta = math.abs(max_negative_delta) + max_positive_delta

    return {
        value = max_delta,
        is_threshold_exceeded = max_delta > vars.clock_delta_threshold_warning,
    }
end

return {
    validate_config = function(...)
        return config_error:pcall(validate_config, ...)
    end,
    apply_config = apply_config,

    get_max_clock_delta = get_max_clock_delta,
}
