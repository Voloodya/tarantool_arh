local model = require('common.model')
local tenant = require('common.tenant')

local sformat = string.format
local function validate(new_cfg, migrations)
    if type(migrations) ~= 'table' then
        error('Migration expected to be a table')
    end

    local current_mdl = tenant.get_mdl()
    -- FIXME: Remove expiration
    local current_expiration = tenant.get_cfg_non_null('versioning', 'expiration')

    local compatibility, err = model.are_models_compatible(current_mdl, current_expiration, new_cfg)
    if err ~= nil then
        error(err)
    end

    local migrations_map = {}
    for _, section in ipairs(migrations) do
        local type_name = section.type_name
        migrations_map[type_name] = section.code
        if compatibility[type_name] == nil then
            error(sformat('Excess migration section for type %q - migration is not required', type_name))
        end

        local f, err = load(section.code, sformat('@migration-for-type-%s', type_name), 't')
        if err ~= nil then
            error(sformat('Migration for type %q is invalid: %s', type_name, err))
        end

        local ok, lib = pcall(f)
        if not ok then
            error(sformat('Migration for type %q is invalid: %s', type_name, lib))
        end

        if type(lib) ~= 'table' then
            error(sformat('Migration for type %q does not return a table with "transform" function', type_name))
        end

        if type(lib.transform) ~= 'function' then
            error(sformat('Migration for type %q does not export "transform" function', type_name))
        end
    end

    for type_name in pairs(compatibility) do
        if migrations_map[type_name] == nil then
            error(sformat('Migration for type %q is not found', type_name))
        end
    end
end

return {
    validate = validate,
}
