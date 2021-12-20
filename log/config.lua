local checks = require('checks')
local errors = require('errors')
local config_error = errors.new_class('Invalid logger config')
local config_checks = require('common.config_checks').new(config_error)
local severities = require('log.severities')

local function validate(conf)
    checks('table')

    local cfg = conf['logger']

    if not cfg then
        return true
    end

    config_checks:check_luatype('logger config', cfg, 'table')
    config_checks:check_table_keys('config', cfg,
        {'enabled', 'max_msg_in_log', 'max_log_size', 'remove_older_n_hours', 'severity'})
    config_checks:check_optional_luatype('config.enabled', cfg.enabled, 'boolean')
    config_checks:check_optional_luatype('config.severity', cfg.severity, 'string')

    local severity = cfg.severity
    if severity then
        config_checks:assert(severities.is_valid_string_value(severity), 'Invalid severity value: %q', severity)
    end
    config_checks:check_optional_luatype('config.max_msg_in_log', cfg.max_msg_in_log, 'number')
    config_checks:check_optional_luatype('config.max_log_size', cfg.max_log_size, 'number')
    config_checks:check_optional_luatype('config.remove_older_n_hours',
    cfg.remove_older_n_hours, 'number')

    return true
end

return {
    validate = function(...)
        return config_error:pcall(validate, ...)
    end
}
