local json = require('json')
local errors = require('errors')
local lines = require('connector.iterators.lines')

local e_parsing = errors.new_class('parsing_error')

local function iterate(fh)
    local it, state = lines.iterate(fh)
    return function(st)
        local i, line = it(st)
        if not line then return end
        local ok, obj = pcall(json.decode, line)

        if not ok then
            st.err = e_parsing:new(obj)
            return
        end
        if type(obj) ~= 'table' then
            -- TODO: Handle auto closing. Take a look at io.lines
            st.err = e_parsing:new('Line must represent an object, got: \'%s\'', obj)
            return
        end

        return i, obj
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
