local function wrap(context, fn)
    return function(...)
        return fn(context, ...)
    end
end

local function load(mod, context)
    local M = {}

    assert(type(mod) == 'table', 'Dynamic module must be a table')
    assert(type(mod.exports) == 'table', 'Dynamic module must export functions with "exports" key')

    for name, fn in pairs(mod.exports) do
        M[name] = context == nil and fn or wrap(context, fn)
    end

    return M
end

return {
    load = load,
}
