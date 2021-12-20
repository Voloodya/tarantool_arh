local errors = require('errors')

local usage_error = errors.new_class("usage_error")

--[[
    Document does not support tarantool `ALL` iterator.
    It's made for 'code style'.
]]

local op_to_tarantool_table = {
    ["=="] = 'EQ',
    ["<"] = 'LT',
    ["<="] = 'LE',
    [">"] = 'GT',
    [">="] = 'GE',
    ["LIKE"] = 'GE',
    ["ILIKE"] = 'GE',
}

local op_to_types_table = {
    ["LIKE"] = { ['string'] = 'string' },
    ["ILIKE"] = { ['string'] = 'string' },
}

local function op_to_tarantool(op_str)
    local op = op_to_tarantool_table[op_str]
    usage_error:assert(op ~= nil,  'unsupported comparator %q', op_str)
    return op
end

local function check_value_type(op_str, value)
    local op_types = op_to_types_table[op_str]
    if op_types ~= nil and op_types[type(value)] == nil then
        usage_error:assert(op_types ~= nil,
            'unsupported value type %q for operator', value, op_str)
    else
        return nil
    end
end

local invert_tarantool_op_table = {
    EQ = 'REQ',
    REQ = 'EQ',
    LT = 'GT',
    LE = 'GE',
    GT = 'LT',
    GE = 'LE',
}

local function invert_tarantool_op(op)
    local reverse_op = invert_tarantool_op_table[op]
    usage_error:assert(reverse_op ~= nil, 'unsupported tarantool comparator %q', op)
    return reverse_op
end

local tarantool_to_sort_op_table = {
    REQ = '<',
    LT = '<',
    LE = '<',
    EQ = '>',
    GT = '>',
    GE = '>',
    LIKE = '<',
    ILIKE = '<',
}

local function tarantool_to_sort_op(op_str)
    local sort_op = tarantool_to_sort_op_table[op_str]
    usage_error:assert(sort_op ~= nil, 'unsupported tarantool comparator %q', op_str)
    return sort_op
end

local function eq(lhs, rhs)
    return lhs == rhs
end
local function eq_unicode(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.cmp(lhs, rhs) == 0
    end
    return eq(lhs)
end
local function eq_unicode_ci(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.casecmp(lhs, rhs) == 0
    end
    return lhs == rhs
end
local function lt(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    -- boolean compare
    if type(lhs) == 'boolean' and type(rhs) == 'boolean' then
        return (not lhs) and rhs
    elseif type(lhs) == 'boolean' or type(rhs) == 'boolean' then
        usage_error
            :assert(false,
                    'Could not compare boolean and not boolean')
    end
    -- general compare
    return lhs < rhs
end

local function lt_unicode(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.cmp(lhs, rhs) == -1
    end
    return lt(lhs, rhs)
end

local function lt_unicode_ci(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.casecmp(lhs, rhs) == -1
    end
    return lt(lhs, rhs)
end

local function array_eq_def(lhs, rhs, len, _, equals)
    for i=1,len do
        if not equals[i](lhs[i], rhs[i]) then
            return false
        end
    end

    return true
end

local function array_lt_def(lhs, rhs, len, lessthans, equals)
    for i=1,len do
        if lessthans[i](lhs[i], rhs[i]) then
            return true
        elseif not equals[i](lhs[i], rhs[i]) then
            return false
        end
    end
    return false
end

local function array_le_def(lhs, rhs, len, lessthans, equals)
    for i=1,len do
        if lessthans[i](lhs[i], rhs[i]) then
            return true
        elseif not equals[i](lhs[i], rhs[i]) then
            return false
        end
    end
    return true
end

local function array_gt_def(lhs, rhs, len, lessthans, equals)
    for i=1,len do
        if lessthans[i](lhs[i], rhs[i]) then
            return false
        elseif not equals[i](lhs[i], rhs[i]) then
            return true
        end
    end
    return false
end

local function array_ge_def(lhs, rhs, len, lessthans, equals)
    for i=1,len do
        if lessthans[i](lhs[i], rhs[i]) then
            return false
        elseif not equals[i](lhs[i], rhs[i]) then
            return true
        end
    end
    return true
end

local function make_typed_array_cmp(target, index_parts)
    local len = #index_parts
    local lessthans = table.new(len, 0)
    local equals = table.new(len, 0)
    for i = 1, len do
        local part = index_parts[i]
        if part.collation == nil then
            lessthans[i] = lt
            equals[i] = eq
        elseif part.collation == 'unicode' then
            lessthans[i] = lt_unicode
            equals[i] = eq_unicode
        elseif part.collation == 'unicode_ci' then
            lessthans[i] = lt_unicode_ci
            equals[i] = eq_unicode_ci
        else
            usage_error
                :assert(false,
                        'unsupported tarantool collation %q',
                        part.collation)
        end
    end

    return function(lhs, rhs)
        return target(lhs, rhs, len, lessthans, equals)
    end
end

local function op_to_array_fun_def(op_str, parts)
    if op_str == "==" then
        return make_typed_array_cmp(array_eq_def, parts)
    elseif op_str == "<" then
        return make_typed_array_cmp(array_lt_def, parts)
    elseif op_str == "<=" then
        return make_typed_array_cmp(array_le_def, parts)
    elseif op_str == ">" then
        return make_typed_array_cmp(array_gt_def, parts)
    elseif op_str == ">=" then
        return make_typed_array_cmp(array_ge_def, parts)
    else
        usage_error
            :assert(false, 'unsupported tarantool comparator %q', op_str)
    end
end

return {
    op_to_tarantool = op_to_tarantool,
    op_to_array_fun_def = op_to_array_fun_def,
    array_eq_def = array_eq_def,
    tarantool_to_sort_op = tarantool_to_sort_op,
    invert_tarantool_op = invert_tarantool_op,
    check_value_type = check_value_type,
}
