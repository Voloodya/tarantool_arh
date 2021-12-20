local checks = require('checks')
local errors = require('errors')
local config_error = errors.new_class('Invalid tracing config')
local config_checks = require('common.config_checks').new(config_error)
local utils = require('common.utils')

local function validate(conf)
    checks('table')

    local cfg = conf['tracing']

    if cfg == nil then
        return true
    end

    config_checks:check_optional_luatype('tracing config', cfg, 'table')
    config_checks:check_optional_luatype('config.base_url', cfg.base_url, 'string')
    config_checks:check_luatype('config.api_method', cfg.api_method, 'string')
    config_checks:assert(utils.has_value({'POST', 'GET', 'PUT'}, cfg.api_method), 'Unsupported HTTP method')
    config_checks:check_optional_luatype('config.report_interval', cfg.report_interval, 'number')
    if cfg.report_interval ~= nil then
        config_checks:assert(cfg.report_interval >= 0,
            'config.report_interval should be greater than or equal to zero')
    end
    config_checks:check_optional_luatype('config.spans_limit', cfg.spans_limit, 'number')
    if cfg.spans_limit ~= nil then
        config_checks:assert(cfg.spans_limit > 0, 'cfg.spans_limit should be greater than zero')
    end
    return true
end

return {
    validate = function(...)
        return config_error:pcall(validate, ...)
    end
}
