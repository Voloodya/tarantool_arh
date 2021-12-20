local fiber = require('fiber')

local api_raw = {
    assert   = assert,
    error    = error,
    ipairs   = ipairs,
    next     = next,
    pairs    = pairs,
    pcall    = pcall,
    print    = print, -- luacheck: ignore
    select   = select,
    tonumber = tonumber,
    tostring = tostring,
    type     = type,
    unpack   = unpack,
    xpcall   = xpcall,
    sleep    = fiber.sleep,
    -- Builtin modules
    box = require('common.sandbox.modules.box'),
    math = require('common.sandbox.modules.math'),
    string = require('common.sandbox.modules.string'),
    table = require('common.sandbox.modules.table'),
    utf8 = require('common.sandbox.modules.utf8'),
}

local api = {}

for k, v in pairs(api_raw) do
    api[k] = table.deepcopy(v)
end

return api
