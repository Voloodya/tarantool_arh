local config = require('roles.permanent.gc.config')
local gc = require('roles.permanent.gc.gc')

local function validate_config(cfg)
    return config.validate(cfg)
end

local function apply_config(cfg)
    local _, err = gc.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

return {
    validate_config = validate_config,
    apply_config = apply_config,

    permanent = true,
    role_name = 'gc',
    dependencies = {},
}
