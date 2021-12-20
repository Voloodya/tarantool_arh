local checks = require('checks')
local errors = require('errors')

local funcall_error = errors.new_class("funcall_error")

local function exists(function_name)
    checks("string")
    local parts = string.split(function_name, '.')

    if #parts < 2 then
        return nil, funcall_error:new(
            "funcall.call() expects function_name to contain module name. Got: '%s'", function_name)
    end

    local funname = parts[#parts]
    parts[#parts] = nil
    local modulename = table.concat(parts, '.')

    local mdl = package.loaded[modulename]

    if mdl == nil then
        return nil, funcall_error:new("Can't find module %s", modulename)
    end

    if mdl[funname] == nil then
        return nil, funcall_error:new("No function '%s' in module '%s'", funname, modulename)
    end

    return { obj = mdl, fun = mdl[funname] }
end

local function call(function_name, ...)
    checks("string")

    local result, err = exists(function_name)
    if err ~= nil then
        return nil, err
    end

    local res, err = funcall_error:pcall(result.fun, result.obj, ...)
    return res, err
end

return {
    call = call,
    exists = exists,
}
