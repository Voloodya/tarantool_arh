local errors = require('errors')
local config_error = errors.new_class('Invalid backup config')
local config_checks = require('common.config_checks').new(config_error)
local config_filter = require('common.config_filter')

local function validate(cfg)
    cfg = config_filter.compare_and_get(cfg, 'backup')
    if cfg == nil then
        return true
    end

    config_checks:check_luatype('backup', cfg, 'table')
    config_checks:check_optional_luatype('backup.keep_config_count', cfg.keep_config_count, 'number')
    if cfg.keep_config_count ~= nil then
        config_checks:assert(cfg.keep_config_count > 0, 'backup.keep_config_count expected to be greater than 0')
    end

    return true
end

return {
    validate = validate,
}
