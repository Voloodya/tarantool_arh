--[[
Sorted table iterator, allow to iterate on the natural order of the keys of a
table.

Example:

    local t = {
        ['a'] = 1,
        ['b'] = 2,
        ['c'] = 3,
        ['d'] = 4,
        ['e'] = 5 }

    Normal iterating with pairs

        for key, val in pairs(t) do
            print(key.." : "..val)
        end

        a : 1
        c : 3
        b : 2
        e : 5
        d : 4

    Ordered iterating

        local sorted_pairs = require('common.sorted_pairs')

        for key, val in sorted_pairs(t) do
            print(key.." : "..val)
        end

        a : 1
        b : 2
        c : 3
        d : 4
        e : 5
]]

local function make_sorted_keys(data)
    local sorted_keys = {}

    for key in pairs(data) do
        table.insert(sorted_keys, key)
    end

    table.sort(sorted_keys)

    return sorted_keys
end

local sorted_pairs = function (data)
    local sorted_keys = make_sorted_keys(data)
    local data = data
    local index = 0
    local total = #sorted_keys

    local instance = function()
        index = index + 1

        if index <= total then
            local key = sorted_keys[index]
            return key, data[key]
        end

        sorted_keys = nil
        data = nil

        return nil
    end

    return instance
end

return sorted_pairs
