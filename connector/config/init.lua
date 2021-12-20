local errors = require('errors')
local input_config = require('connector.config.input')
local output_config = require('connector.config.output')
local routing_config = require('connector.config.routing')
local config_error = errors.new_class('Invalid connector config')
local config_checks = require('common.config_checks').new(config_error)

local function validate_config(cfg)
    local tc_cfg = cfg['connector'] or {}
    config_checks:assert(type(tc_cfg) == 'table', 'config must be a table')

    if tc_cfg.input ~= nil then
        input_config.validate(cfg)
    end

    if tc_cfg.output ~= nil then
        output_config.validate(cfg)
    end

    if tc_cfg.routing ~= nil then
        routing_config.validate(cfg)
    end
    config_checks:check_table_keys('config', tc_cfg, {'input', 'output', 'routing'})
end

return {
    validate = function(cfg)
        local ok, err = pcall(validate_config, cfg)
        if not ok then
            return nil, err
        end
        return true
    end,
}
