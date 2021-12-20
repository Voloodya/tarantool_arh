local csv = require('csv')
local fun = require('fun')
local errors = require('errors')
local lines = require('connector.iterators.lines')

local e_parsing = errors.new_class('parsing_error')

local function iterate(fh)
    local it, state = lines.iterate(fh)
    local _, header = it(state)
    local ok, keys = pcall(csv.load, header)
    if not ok then
        error(e_parsing:new(keys)) -- Any suggestions to do better
    end
    keys = keys[1]
    return function(st)
        local i, line = it(st)
        if not line then return end

        local values = csv.load(line)[1]
        local obj = fun.zip(keys, values):tomap()
        return i-1, obj
    end, state
end

local function new(readable, _)
    local iter, state = iterate(readable)
    return setmetatable({
        state = state,
    }, {
        __call = iter,
        __index = {
            call = iter,
        }
    })
end

return {
    new = new,
}
