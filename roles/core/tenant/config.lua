local errors = require('errors')
local model = require('common.model')
local config_error = errors.new_class('Invalid tenant config')
local config_checks = require('common.config_checks').new(config_error)

local function c(...)
    return table.concat({...}, '.')
end

local function validate_shared_types(cfg)
    if cfg.shared_types == nil then
        return true
    end
    config_checks:check_luatype('shared_types', cfg.shared_types, 'table')

    local types = ''
    if cfg.types ~= nil then
        config_checks:check_luatype('types', cfg.types, 'string')
        types = cfg.types
    end

    local mdl, err = model.load_string(types)
    if mdl == nil then
        return nil, err
    end

    local types_map = {}
    for _, mdl_type in ipairs(mdl) do
        types_map[mdl_type.name] = mdl_type
    end

    for type_name, type_options in pairs(cfg.shared_types) do
        config_checks:assert(type(type_name) == 'string',
            'shared_types expected to be a map, got non-string index %q', type_name)
        config_checks:assert(types_map[type_name] ~= nil, 'Type %q is not defined', type_name)

        if type_options == box.NULL then
            goto continue
        end
        config_checks:check_luatype(c('shared_types', type_name), type_options, 'table')
        config_checks:check_luatype(c('shared_types', type_name, 'query_prefix'),
            type_options.query_prefix, 'string')

        if type_options.tenants == box.NULL then
            goto continue
        end

        config_checks:check_luatype(c('shared_types', type_name, 'tenants'), type_options.tenants, 'table')

        for tenant_uid, access_rights in pairs(type_options.tenants) do
            -- TODO: check tenant existence
            config_checks:check_luatype(
                c('shared_types', type_name, 'tenants', tenant_uid), tenant_uid, 'string')
            config_checks:check_luatype(
                c('shared_types', type_name, 'tenants', tenant_uid), access_rights, 'table')
            config_checks:check_table_keys(
                c('shared_types', type_name, 'tenants', tenant_uid), access_rights, {'read', 'write'})

            config_checks:check_optional_luatype(c('shared_types', type_name, 'tenants', tenant_uid, 'read'),
                access_rights.read, 'boolean')
            config_checks:check_optional_luatype(c('shared_types', type_name, 'tenants', tenant_uid, 'write'),
                access_rights.write, 'boolean')
        end
        ::continue::
    end
    return true
end

local function validate_config(cfg)
    validate_shared_types(cfg)
    return true
end

return {
    validate = validate_config,

    -- For tests
    validate_shared_types = validate_shared_types,
}
