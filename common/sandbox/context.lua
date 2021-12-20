local module_name = 'common.sandbox.context' -- luacheck: ignore

local odbc = require('common.odbc')

local function apply_config(_, cfg)
    odbc.apply_config(cfg)
    return true
end

local function new()
    odbc.init()

    return setmetatable({}, {
        __index = {
            apply_config = apply_config,
        }
    })
end

return {
    new = new,
}
