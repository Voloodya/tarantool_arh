local errors = require('errors')
local checks = require('checks')
local dispatcher = require('common.sandbox.dispatcher')

local function_call_error = errors.new_class("function_call_error")

local function call(untrusted_function, ...)
    checks("function")

    local res, err = function_call_error:pcall(untrusted_function, ...)
    if err ~= nil then
        return nil, err
    end

    return res
end

local function call_by_name(self, fn_name, ...)
    checks("sandbox", "string")
    local fn = dispatcher.get(self, fn_name)
    return call(fn, ...)
end

local function batch(fn, data, ...)
    checks("function", "table")

    local result = table.new(#data, 0)
    for _, element in ipairs(data) do
        local res, err = call(fn, element, ...)
        if err ~= nil then
            return nil, err
        end
        if res ~= nil then
            table.insert(result, res)
        end
    end

    return result
end

local function accumulate(fn, state, data, ...)
    checks("function", "?", "table")

    for _, elem in ipairs(data) do
        local err
        state, err = call(fn, state, elem, ...)
        if err ~= nil then
            return nil, err
        end
    end

    return state
end

return {
    call = call,
    call_by_name = call_by_name,
    batch_call = batch,
    batch_accumulate = accumulate,
}
