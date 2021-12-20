local errors = require('errors')
local model = require('common.model')
local model_graphql =  require('common.model_graphql')
local utils = require('common.utils')
local config_error = errors.new_class('Invalid services config')
local config_checks = require('common.config_checks').new(config_error)
local sandbox_registry = require('common.sandbox.registry')
local cartridge_utils = require('cartridge.utils')

local function validate_arg(service_name, arg_name, type_entry, mdl, types)
    if type(type_entry) == 'table' then
        type_entry = table.deepcopy(type_entry)
        cartridge_utils.table_setrw(type_entry)
    end
    local normalized_type_entry = model.normalize_defined_type(type_entry)
    local res, err = model.validate_type_entry(types, normalized_type_entry)
    config_checks:assert(res, 'service[%s] argument[%s] has unknown type[%s]', service_name, arg_name, err)

    local _, err = model_graphql.resolve_graphql_type(mdl, types, type_entry, {input=true})
    config_checks:assert(err == nil, 'service[%s] argument[%s] has unknown type[%s]', service_name, arg_name, err)
end

local function validate_service(service_name, service_cfg, _, mdl, types)
    config_checks:assert(type(service_cfg) == 'table', 'service[%s] definition must be a table', service_name)

    config_checks:assert(string.match(service_name, '^[a-zA-Z]+[a-zA-Z0-9_]*$'),
        'service[%s] name should start with char and contain only ASCII characters', service_name)

    local doc = service_cfg['doc']
    if doc ~= nil then
        config_checks:assert(type(doc) == 'string', 'service[%s] doc must be a string', service_name)
    end

    local fun = service_cfg['function']
    local sandbox = sandbox_registry.get('tmp')
    config_checks:assert(sandbox ~= nil, 'sandbox must be registered before validating service "%s"', service_name)
    config_checks:assert(fun ~= nil, 'service[%s] function is mandatory', service_name)
    config_checks:assert(type(fun) == 'string', 'service[%s] function must be a string', service_name)
    config_checks:assert(sandbox:dispatch_function(fun))

    local return_type = service_cfg['return_type']
    config_checks:assert(return_type ~= nil, 'service[%s] return_type is mandatory', service_name)
    if type(return_type) == 'table' then
        return_type = table.deepcopy(return_type)
        cartridge_utils.table_setrw(return_type)
    end
    local normalized_return_type = model.normalize_defined_type(return_type)
    local res, err = model.validate_type_entry(types, normalized_return_type)
    config_checks:assert(res, 'service[%s] has unknown return_type[%s]', service_name, err)
    local _, err = model_graphql.resolve_graphql_type(mdl, types, return_type)
    config_checks:assert(err == nil, 'service[%s] return_type has unknown type', service_name)

    local args = service_cfg['args']
    if args ~= nil then
        config_checks:assert(type(args) == 'table', 'service[%s] args must be a table', service_name)
        for arg_name, type_entry in pairs(args) do
            validate_arg(service_name, arg_name, type_entry, mdl, types)
        end
    end

    local service_type = service_cfg['type']
    if service_type ~= nil then
        config_checks:assert(type(service_type) == 'string', 'service[%s] type must be a string', service_name)

        config_checks:assert(utils.has_value({"query", "mutation"}, service_type),
                             'service[%s] type must be "query" or "mutation"', service_name)
    end
end

local function validate_config(mdl, types, cfg)
    config_checks:assert(type(cfg) == 'table',
        'services must be a table')

    local services = cfg.services or {}
    for service_name, service_cfg in pairs(services) do
        config_checks:assert(type(service_name) == 'string',
            'services must contain string keys')

        validate_service(service_name, service_cfg, cfg, mdl, types)
    end

    for _, model in pairs(mdl) do
        if services[model.name] ~= nil then
            config_checks:assert(services[model.name] == nil,
                'Unable to specify model and service with similar names: %s', model.name)
        end
    end
end

return {
    validate = function(mdl, cfg)
        local types = {}
        for _, t in ipairs(mdl) do
            types[t.name] = t
        end

        local ok, err = pcall(validate_config, mdl, types, cfg)
        if not ok then
            return nil, err
        end
        return true
    end,
}
