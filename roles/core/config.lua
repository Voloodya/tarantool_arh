local errors = require('errors')
local config_error = errors.new_class('Invalid core config')
local config_checks = require('common.config_checks').new(config_error)
local app_version = require('common.app_version')

local function validate_tdg_version(cfg)
    config_checks:check_luatype('tdg_version', cfg.tdg_version, 'string')
    config_checks:assert(app_version.check(cfg.tdg_version))
end

local function validate_config(cfg)
    if cfg.tdg_version then
        validate_tdg_version(cfg)
    end

    return true
end

return {
    validate_config = validate_config,
    validate = function(cfg)
        local ok, err = pcall(validate_config, cfg)
        if not ok then
            return nil, err
        end
        return true
    end,
}
