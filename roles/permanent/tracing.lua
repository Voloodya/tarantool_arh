local tracing_config = require('common.tracing.config')
local tracing = require('common.tracing')

-- TODO: Should be tracing global?
local function tenant_validate_config(cfg)
    return tracing_config.validate(cfg)
end

local function tenant_apply_config(cfg)
    local _, err = tracing.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

return {
    tenant_validate_config = tenant_validate_config,
    tenant_apply_config = tenant_apply_config,

    permanent = true,
    role_name = 'tracing',
    dependencies = {},
}
