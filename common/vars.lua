local checks = require('checks')
local tenant

if rawget(_G, "_tdg_vars_defaults") == nil then
    _G._tdg_vars_defaults = {}
end

if rawget(_G, "_tdg_vars_globals") == nil then
    _G._tdg_vars_globals = {}
end

if rawget(_G, "_tdg_vars_values") == nil then
    _G._tdg_vars_values = {}
end

local defaults = _G._tdg_vars_defaults
local globals = _G._tdg_vars_globals
local vars = _G._tdg_vars_values

local function new_var(self, name, default_value, is_global)
    checks("table", "string", "?", '?boolean')

    local module_name = self.module_name
    if defaults[module_name] == nil then
        defaults[module_name] = {}
    end

    if is_global ~= true then
        if default_value ~= nil
            and type(default_value) ~= 'number'
            and type(default_value) ~= 'boolean'
            and type(default_value) ~= 'string' then
            error(('Shared value: %q: %s'):format(name, debug.traceback()))
        end
    end

    defaults[module_name][name] = default_value
end

local function new_global_var(self, name, default_value)
    checks("table", "string", "?")

    local module_name = self.module_name
    if globals[module_name] == nil then
        globals[module_name] = {}
    end

    new_var(self, name, default_value, true)

    globals[module_name][name] = true
end

local function is_global(module_name, name)
    if globals[module_name] == nil then
        return false
    end

    return globals[module_name][name] == true
end

local function set_var(self, name, value)
    checks("table", "string", "?")

    local module_name = self.module_name

    local tenant_uid
    if is_global(module_name, name) then
        tenant_uid = 'default'
    else
        tenant_uid = tenant.uid()
    end

    local tenant_vars = vars[tenant_uid]
    if tenant_vars == nil then
        vars[tenant_uid] = {}
        tenant_vars = vars[tenant_uid]
    end

    local module_vars = tenant_vars[module_name]
    if module_vars == nil then
        tenant_vars[module_name] = {}
        module_vars = tenant_vars[module_name]
    end

    module_vars[name] = value
end

local function get_var(self, name)
    checks("table", "string")

    local module_name = self.module_name
    local tenant_uid
    if is_global(module_name, name) then
        tenant_uid = 'default'
    else
        tenant_uid = tenant.uid()
    end

    local tenant_vars = vars[tenant_uid]
    if tenant_vars == nil then
        vars[module_name] = {}
        tenant_vars = vars[module_name]
    end

    if tenant_vars[module_name] ~= nil then
        local res = tenant_vars[module_name][name]
        if res ~= nil then
            return res
        end
    else
        tenant_vars[module_name] = {}
    end

    if defaults[module_name] == nil then
        defaults[module_name] = {}
    end

    local default_value = defaults[module_name][name]

    tenant_vars[module_name][name] = default_value

    return default_value
end


local function new(module_name)
    checks("string")

    local obj = {
        module_name = module_name,
        new = new_var,
        new_global = new_global_var,
    }

    local mt = {
        __newindex = set_var,
        __index = get_var,
    }

    setmetatable(obj, mt)

    return obj
end

local function init()
    tenant = require('common.tenant')
end

return {
    init = init,
    new = new,
}
