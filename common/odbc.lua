local odbc = require('odbc')
local json = require('json')
local checks = require('checks')
local errors = require('errors')

local module_name = 'common.odbc'
local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)

local odbc_runtime_error = errors.new_class('ODBC runtime error')
local odbc_connection_error = errors.new_class('ODBC connection failed')
local odbc_initialization_error = errors.new_class('ODBC initialization failed')

local config_error = errors.new_class('Invalid odbc config')
local config_checks = require('common.config_checks').new(config_error)
local config_filter = require('common.config_filter')

vars:new('odbc_env', nil)
vars:new('sources'
    -- [name] = {
    --     name = ...,
    --     dsn = ...,
    --     timeout_sec = ...,
    --     conn = ...,
    -- }
)

local function validate_config(cfg)
    local conf_odbc = config_filter.compare_and_get(cfg, 'odbc', module_name)
    if conf_odbc == nil then
        return true
    end

    config_checks:assert(type(conf_odbc) == 'table', 'config must be a table')

    for k, conn in ipairs(conf_odbc) do
        local field = string.format('odbc[%s]', k)

        config_checks:check_luatype(field, conn, 'table')
        config_checks:check_luatype(field..'.name', conn.name, 'string')
        config_checks:check_luatype(field..'.dsn', conn.dsn, 'string')
        config_checks:check_optional_luatype(field..'.timeout_sec', conn.timeout_sec, 'number')
    end

    return true
end

local function apply_config(cfg, _)
    checks('table', '?')

    vars.sources = vars.sources or {}

    local conf, err = config_filter.compare_and_set(cfg, 'odbc', module_name)
    if err ~= nil then
        return true
    end

    local sources_new = conf or {}
    local sources_old = vars.sources or {}

    for _, src in ipairs(sources_new) do
        if sources_old[src.name] == nil then
            log.info('Adding odbc connection %q with DSN %q', src.name, src.dsn)
            vars.sources[src.name] = {
                name = src.name,
                dsn = src.dsn,
                timeout_sec = src.timeout_sec,
                conn = nil,
            }
        else
            if sources_old[src.name].dsn == src.dsn and sources_old[src.name].timeout_sec == src.timeout_sec then
                log.info('Retaining odbc connection %q with DSN %q', src.name, src.dsn)
                vars.sources[src.name] = sources_old[src.name]
            else
                log.info('Updating odbc connection %q DSN %q', src.name, src.dsn)
                vars.sources[src.name] = {
                    name = src.name,
                    dsn = src.dsn,
                    timeout_sec = src.timeout_sec,
                    conn = nil,
                }
            end
        end
    end

    return true
end

local function init()
    if vars.odbc_env ~= nil then
        return true
    end

    local env, err = odbc.create_env()

    if env == nil then
        return nil, odbc_initialization_error:new(json.encode(err))
    end

    vars.odbc_env = env
    return true
end

local function connect(name)
    checks('string')

    odbc_connection_error:assert(vars.odbc_env ~= nil, "ODBC isn't initialized")

    local source = vars.sources[name]
    if source == nil then
        return nil, odbc_connection_error:new('Unknown source %q', name)
    end

    if source.conn ~= nil and not source.conn:is_connected() then
        source.conn:close()
        source.conn = nil
    end

    if source.conn == nil then
        local conn, err = vars.odbc_env:connect(source.dsn)
        if conn == nil then
            return nil, odbc_connection_error:new(json.encode(err))
        end

        if source.timeout_sec ~= nil then
            conn:set_timeout(source.timeout_sec)
        end
        source.conn = conn
    end

    return source.conn
end

local function execute(connection_name, statement, params)
    checks('string', 'string', '?table')

    local conn, err = connect(connection_name)

    if conn == nil then
        return nil, err
    end

    local ret, err = conn:execute(statement, params or {})
    if ret == nil then
        return nil, odbc_runtime_error:new(json.encode(err))
    end

    return ret
end

local function prepare(connection_name, query)
    checks('string', 'string')

    local conn, err = connect(connection_name)

    if conn == nil then
        return nil, err
    end

    local prep, err = conn:prepare(query)
    if prep == nil then
        return nil, odbc_runtime_error:new(json.encode(err))
    end

    return {
        execute = function(params)
                local ret, err = prep:execute(params or {})
                if ret == nil then
                    return nil, odbc_runtime_error:new(json.encode(err))
                end

                return ret
            end,
        close = function()
            prep:close()
        end
    }
end

return {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,
    connect = connect,
    execute = execute,
    prepare = prepare,
}
