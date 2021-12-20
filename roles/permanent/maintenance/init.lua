local maintenance = require('roles.permanent.maintenance.server')

-- @monkeypatch
-- Hide built-in cartridge roles
require('cartridge.roles.vshard-storage').hidden = true
require('cartridge.roles.vshard-router').hidden = true

local function init()
    return true
end

local function validate_config(cfg)
    return maintenance.validate_config(cfg)
end

local function apply_config(cfg)
    local _, err = maintenance.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function get_max_clock_delta()
    return maintenance.get_max_clock_delta()
end

return {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,

    get_max_clock_delta = get_max_clock_delta,

    permanent = true,
    role_name = 'maintenance',
    dependencies = {},
}
