local checks = require('checks')

local errors = require('errors')
local config_error = errors.new_class('Invalid repair queue config')

local config_checks = require('common.config_checks').new(config_error)

local function validate(conf)
    checks('table')

    local cfg = conf['repair_queue']

    if not cfg then
        return true
    end

    config_checks:check_luatype('repair_queue', cfg, 'table')

    for name, section in pairs(cfg) do
        if name == 'on_object_added' then
            config_checks:assert(
                type(section) == 'table', 'on_object_added must be a table')

            for routing_key, properties in pairs(section) do
                config_checks:check_table_keys(
                    routing_key, properties, { 'postprocess_with_routing_key' })
                config_checks:check_luatype(
                    routing_key .. '.postprocess_with_routing_key',
                    properties.postprocess_with_routing_key, 'string')
                config_checks:assert(properties.postprocess_with_routing_key ~= '',
                    'postprocess_with_routing_key must be non-empty')
            end
        else
            config_checks:assert(false, 'Unknown section: %q', name)
        end
    end

    return true
end

return {
    validate = function(...)
        return config_error:pcall(validate, ...)
    end
}
