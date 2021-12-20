local errors = require('errors')
local config_error = errors.new_class('Invalid connector config')
local config_checks = require('common.config_checks').new(config_error)

local function validate_route(cfg, field, route)
    config_checks:check_luatype(field..'.key', route.key, 'string')
    config_checks:check_luatype(field..'.output', route.output, 'string')

    local route_exists = false
    for _, output in pairs(cfg.output or {}) do
        if output.name == route.output then
            route_exists = true
            break
        end
    end
    config_checks:assert(route_exists,
        '%s %q does not exist', field..'.output', route.output)

    config_checks:check_table_keys(field, route, {'key', 'output'})
end

local function validate_config(cfg)
    local tc_cfg = cfg['connector'] or {}
    config_checks:check_luatype('routing', tc_cfg.routing, 'table')

    local seen_routes = {}
    for k, route in pairs(tc_cfg.routing) do
        local field = string.format('route[%s]', k)

        config_checks:check_luatype(field, route, 'table')
        validate_route(tc_cfg, field, route)

        config_checks:assert(not seen_routes[route.key],
            'duplicate routes with key %q', route.key)
        seen_routes[route.key] = true
    end
end

return {
    validate = validate_config,
}
