local errors = require('errors')
local config_error = errors.new_class('Invalid gc config')
local config_checks = require('common.config_checks').new(config_error)
local config_filter = require('common.config_filter')

local function validate(config)
    local conf = config_filter.compare_and_get(config, 'gc')
    if conf == nil then
        return true
    end

    config_checks:check_luatype('gc', conf, 'table')

    config_checks:check_luatype('gc.forced', conf.forced, 'boolean')

    if not conf.forced then
        return true
    end

    config_checks:check_luatype('gc.period_sec', conf.period_sec, 'number')
    config_checks:assert(conf.period_sec > 0,
        'gc.period_sec must be a positive number')

    config_checks:check_luatype('garbage_collector.steps', conf.steps, 'number')
    config_checks:assert(conf.steps > 0,
        'gc.steps must be a positive number')

    return true
end

return {
    validate = validate
}
