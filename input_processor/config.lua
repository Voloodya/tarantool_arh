local module_name = 'input_processor.config'

local log = require('log.log').new(module_name)
local errors = require('errors')
local services_config = require('input_processor.services.config')
local config_error = errors.new_class('Invalid input_processor config')
local config_checks = require('common.config_checks').new(config_error)

local model = require('common.model')
local model_ddl = require('common.model_ddl')
local model_graphql = require('common.model_graphql')

local repository = require('common.repository')
local sandbox_registry = require('common.sandbox.registry')

local function validate_storage(field, storage)
    config_checks:check_luatype(field..'.key', storage.key, 'string')
    config_checks:check_luatype(field..'.type', storage.type, 'string')
    config_checks:check_table_keys(field, storage, {'key', 'type'})
end

local function validate_handler(_, field, handler)
    config_checks:check_luatype(field..'.key', handler.key, 'string')
    config_checks:assert(#handler['key'] > 0,
        field..'.key must not be empty')
    config_checks:check_luatype(field..'.function', handler['function'], 'string')
    config_checks:assert(#handler['function'] > 0,
        field..'.function must not be empty')
    config_checks:assert(handler['function']:find('[0-9]') ~= 1,
    field..'.function must not start with a digit, got "%s"', handler['function'])
    config_checks:check_table_keys(field, handler,{'key', 'function'})
    local sandbox = sandbox_registry.get('tmp')
    local fn, err = sandbox:dispatch_function(handler['function'], {protected = true})
    config_checks:assert(fn ~= nil, field..'.function %s', err)
end

local function validate_config(cfg)
    local types = cfg['types'] ~= box.NULL and cfg['types'] or ''

    local input_processor_cfg = cfg['input_processor'] or {}
    config_checks:assert(type(input_processor_cfg) == 'table',
        'config must be a table')

    config_checks:assert(type(types) == 'string',
                         'types must be a string, got: %s', type(types))

    local mdl, err = model.load_string(types)
    if err ~= nil then
        error(err)
    end

    local _, err = model_graphql.validate(mdl)
    if err ~= nil then
        error(err)
    end

    local _, err = repository.validate_config(cfg)
    if err ~= nil then
        error(err)
    end

    local ddl = cfg['ddl']

    -- for tests
    if ddl == nil then
        local err
        ddl, err = model_ddl.generate_ddl(mdl)
        if ddl == nil then
            error(err)
        end
    end

    if input_processor_cfg.storage ~= nil then
        local model_types = {}
        for _, v in ipairs(mdl) do
            if v.indexes ~= nil then
                model_types[v.name] = true
            end
        end

        config_checks:check_luatype('storage', input_processor_cfg.storage, 'table')

        local seen_storage_keys = {}
        for k, storage in pairs(input_processor_cfg.storage) do
            local field = string.format('storage[%s]', k)

            config_checks:check_luatype(field, storage, 'table')
            validate_storage(field, storage)

            -- check storage[i].type is defined and correct
            config_checks:check_luatype(field .. '.type', storage.type, 'string')
            config_checks:assert(model_types[storage.type] ~= nil,
                'unknown type %q', storage.type)

            config_checks:assert(not seen_storage_keys[storage.key],
                'duplicate storages of key %q', storage.key)
            seen_storage_keys[storage.key] = true
        end
    end

    if input_processor_cfg.handlers ~= nil then
        config_checks:check_luatype('handlers', input_processor_cfg.handlers, 'table')

        local seen_handlers = {}
        for k, handler in pairs(input_processor_cfg.handlers) do
            local field = string.format('handlers[%s]', k)

            config_checks:check_luatype(field, handler, 'table')
            validate_handler(cfg, field, handler)

            config_checks:assert(not seen_handlers[handler.key],
                'duplicate handlers with key %q', handler.key)
            seen_handlers[handler.key] = true
        end
    end

    if cfg.services ~= nil then
        log.info("service config found, checking...")
        local ok, err = services_config.validate(mdl, cfg)
        if not ok then
            error(err)
        end
    end

    config_checks:check_table_keys('config', input_processor_cfg, {'handlers', 'storage', 'output'})
end

return {
    validate = function(cfg)
        local ok, err = pcall(validate_config, cfg)
        if not ok then
            return nil, err
        end
        return true
    end
}
