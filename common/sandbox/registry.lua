local module_name = 'common.sandbox.registry'

local checks = require('checks')
local vars = require('common.vars').new(module_name)

vars:new('registry')

local function set(key, value)
    checks('string', '?sandbox')
    if vars.registry == nil then
        vars.registry = {}
    end
    vars.registry[key] = value
end

local function unset(key)
    set(key, nil)
end

local function get(key, default)
    checks('string', '?sandbox')
    assert(key == 'active' or key == 'tmp')
    return vars.registry and vars.registry[key] or default
end

local function list()
    local result = {}
    for key, value in pairs(vars.registry) do
        table.insert(result, { key, value })
    end
    return result
end

local function clear()
    vars.registry = {}
end

local function set_cfg(key, cfg)
    checks('string', 'table')
    assert(key == 'active' or key == 'tmp')

    local sb = get(key)
    if not sb then
        sb = require('common.sandbox').new(cfg, key)
        set(key, sb)
    else
        sb:update(cfg)
    end

    return sb
end

--

return {
    get = get,
    set = set,
    set_cfg = set_cfg,
    unset = unset,
    list = list,
    clear = clear,
}
