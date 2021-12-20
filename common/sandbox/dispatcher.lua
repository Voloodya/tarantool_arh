local checks = require('checks')
local errors = require('errors')
local require_mod = require('common.sandbox.require')
local _require = require_mod.require

local function_dispatch_error = errors.new_class('function_dispatch_error',
    { capture_stack = false })

local function raise(msg, ...)
    local err = string.format(msg, ...)
    error(err, 0)
end

local string_sub = string.sub
local function pos_from_end(str, c)
    local i = #str
    while i > 0 do
        if string_sub(str, i, i) ~= c then
            i = i - 1
        else
            return i
        end
    end
    return i
end

local function split(name)
    local pos = pos_from_end(name, '.')
    if pos > 0 then
        return string_sub(name, 1, pos-1), string_sub(name, pos+1)
    end
    return name
end

local function dispatch_function_impl(self, name)
    checks('sandbox', 'string')

    local modname, fn_name = split(name)

    if not fn_name then
        raise('Function name for module "%s" must be specified', modname)
    end

    local mod = _require(self, modname)
    if type(mod) ~= 'table' or type(mod[fn_name]) ~= 'function' then
        raise('Expect module "%s" to export "%s" function',
            modname, fn_name)
    end

    return mod[fn_name]
end

local function dispatch_function(self, name, opts)
    opts = opts or {}
    checks('sandbox', 'string', {protected = '?boolean'})
    if opts.protected then
        return function_dispatch_error:pcall(dispatch_function_impl, self, name)
    end

    return dispatch_function_impl(self, name)
end

return {
    get = dispatch_function,
}
