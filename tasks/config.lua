local checks = require('checks')

local errors = require('errors')
local cron = require('common.cron')
local config_error = errors.new_class('Invalid tasks config')
local config_checks = require('common.config_checks').new(config_error)
local sandbox_registry = require('common.sandbox.registry')
local kinds = require('tasks.kinds')

local function validate(conf)
    checks('table')

    local cfg = conf['tasks']
    if cfg == nil then
        return true
    end

    config_checks:check_luatype('tasks', cfg, 'table')
    local sandbox = assert(sandbox_registry.get('tmp'), 'Sandbox not registered when validating a function')

    for key, props in pairs(cfg) do
        local field = 'tasks.' .. key

        config_checks:check_luatype(field .. '.kind', props.kind, 'string')
        config_checks:assert(
            kinds.is_valid(props.kind),
            '%s has invalid kind value %s', field, props.kind)

        config_checks:check_luatype(field .. '.function', props['function'], 'string')
        local _, err = sandbox:dispatch_function(props['function'], {protected = true})
        config_checks:assert(err == nil,
            'Invalid function %q is specified for task %q: %s', props['function'], key, err)

        local required_fields = {'kind', 'function'}

        if props.keep then
            config_checks:check_luatype(field .. '.keep', props.keep, 'number')
            config_checks:assert(props.keep > 0, '%s.keep should be greater than zero', field)
            table.insert(required_fields, 'keep')
        end

        if props.pause_sec then
            config_checks:check_luatype(field .. '.pause_sec', props.pause_sec, 'number')
            table.insert(required_fields, 'pause_sec')
        end

        if (props.kind == kinds.PERIODICAL or props.kind == kinds.CONTINUOUS) and props.run_as ~= nil then
            config_checks:check_luatype(field .. '.run_as', props.run_as, 'table')
            if props.run_as.user == nil then
                config_checks:assert(false, '%s.run_as user should be specified', field)
            end

            table.insert(required_fields, 'run_as')
        end

        if props.kind == kinds.PERIODICAL then
            config_checks:check_luatype(field .. '.schedule', props.schedule, 'string')
            local ok, err = cron.validate(props.schedule)
            config_checks:assert(ok, 'Invalid %s schedule: %s', key, err)
            table.insert(required_fields, 'schedule')
            config_checks:check_table_keys(field, props, required_fields)
        else
            config_checks:check_table_keys(field, props, required_fields)
        end
    end

    return true
end

return {
    validate = function(...)
        return config_error:pcall(validate, ...)
    end
}
