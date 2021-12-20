local module_name = 'sandbox.local_cache'

local vars = require('common.vars').new(module_name)

vars:new('data')

local function check_key(key)
    if type(key) ~= 'string' then
        error(module_name .. ': key must be a string, got ' .. type(key))
    end
end

local function set(key, val)
    check_key(key)
    if vars.data == nil then
        vars.data = {[key] = val}
    else
        vars.data[key] = val
    end
end

local function get(key)
    check_key(key)
    if vars.data == nil then
        return nil
    end
    return vars.data[key]
end

return {
    set = set,
    get = get,
}
