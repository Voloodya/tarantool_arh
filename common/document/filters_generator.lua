local utils = require('common.utils')
local decimal = require('decimal')
local uuid = require('uuid')

local LIKE_OPS = {
    ['LIKE'] = 'LIKE',
    ['ILIKE'] = 'ILIKE',
}

local function format_value(value)
    -- box.NULL is cdata but equals nil
    if type(value) == 'nil' then
        return 'nil'
    elseif value == nil then
        return 'NULL'
    elseif type(value) == 'string' then
        return ("%q"):format(value)
    elseif decimal.is_decimal(value) then
        return ("%q"):format(value)
    elseif uuid.is_uuid(value) then
        return ("%q"):format(value)
    elseif type(value) == 'number' then
        return tostring(value)
    elseif type(value) == 'cdata' then
        return tostring(value)
    elseif type(value) == 'boolean' then
        return tostring(value)
    end
    assert(false, ('Unexpected value %s (type %s)'):format(value, type(value)))
end

local function prepare_pattern(reg)
    if reg == nil then
        return nil
    end

    local p = reg
    -- escape with % lua pattern special symbols: ().^$[]+-*?%
    p = p:gsub('([().^$%[%]%+%-%*%?%%])', '%%%1')
    if not reg:startswith('%') then
        p = '^' .. p
    end
    if not reg:endswith('%') then
        p = p .. '$'
    end
    p = p:gsub('%%%%', '.*')
    p = p:gsub('_', '.')
    return p
end

local checks_template = [[
local tuple, multi_position = ...
]]

--[[
  There are two variants of "multikey_cmp_template".
  The first function checks that in case when multikey position is absent
  if at least ONE element from array matches condition the whole condition
  is true.
  The second function requires that ALL element should match specified condition.

  It's important in case of comparisons.
    - "<"/"<=": LT if only one element less than argument.
    - ">"/">=": GT if all elements less than argument.
--]]

local multikey_cmp_template_one = [[
function M.check_multikey_%s(part, multi_position)
    if multi_position == nil then
        if part ~= nil then
            for _, elem in ipairs(part) do
                if %s(elem, %s) then
                    return true
                end
            end
        end
        return false
    end
    local field
    if part ~= nil then
        field = part[multi_position]
    end
    return %s(field, %s)
end
]]

local multikey_cmp_template_all = [[
function M.check_multikey_%s(part, multi_position)
    if multi_position == nil then
        if part ~= nil then
            local all_lt = true
            for _, elem in ipairs(part) do
                if not %s(elem, %s) then
                    all_lt = false
                    break
                end
            end
            return all_lt
        end
        return false
    end
    local field
    if part ~= nil then
        field = part[multi_position]
    end
    return %s(field, %s)
end
]]

local function format_multikey_cmp_template(template, i, cmp_fun, value, is_const)
    if is_const then
        value = format_value(value)
    end
    return template:format(
        i,
        cmp_fun,
        value,
        cmp_fun,
        value
    )
end

local eq_template = [[
function M.eq_%s(%s)
    return %s
end
]]

local like_template = [[
function M.%slike_%s(%s)
    return %s
end
]]

local op_to_lt_result = {
    ['<'] = true,
    ['<='] = true,
    ['>'] = false,
    ['>='] = false,
}

local op_to_eq_result = {
    ['<'] = false,
    ['<='] = false,
    ['>'] = true,
    ['>='] = true,
}

local op_to_end_result = {
    ['<'] = false,
    ['<='] = true,
    ['>'] = false,
    ['>='] = true,
}

local function gen_cmp_values_code(lt_cond, lt_result, eq_cond, eq_result)
    local str1 = ('    if %s then return %s'):format(lt_cond, lt_result)
    local str2 = ('\n    elseif not %s then return %s\n    end'):format(eq_cond, eq_result)
    return str1 .. str2
end

local header_template = [[
function M.cmp_array_%s(%s)]]
local function gen_cmp_array_code(id, eq_conds, lt_conds, op, args)
    local rows = {}
    local header = header_template:format(id, args)

    table.insert(rows, header)

    assert(#lt_conds == #eq_conds)
    local len = #eq_conds

    local lt_result = op_to_lt_result[op]
    assert(lt_result ~= nil, op)
    local eq_result = op_to_eq_result[op]
    assert(eq_result ~= nil, op)
    for i = 1, len do
        table.insert(rows, gen_cmp_values_code(lt_conds[i], lt_result, eq_conds[i], eq_result))
    end

    local exit_cond = op_to_end_result[op]
    local footer = ('    return %s\nend\n'):format(exit_cond)

    table.insert(rows, footer)
    return table.concat(rows, '\n')
end

local complex_condition_template = [[%s(tuple_%s, %s)]]

local function format_complex_condition(tuple_path, func, value, is_const)
    if is_const == true then
        value = format_value(value)
    end
    return complex_condition_template:format(
        func,
        tuple_path,
        value
    )
end

local tuple_ref_template = 'local tuple_%s = tuple[%s]'
local function gen_cache_tuple_fields_code(num_array)
    if #num_array == 0 then
        return nil
    end
    local variables = {}
    for _, num in ipairs(num_array) do
        table.insert(variables, tuple_ref_template:format(num, num))
    end
    return table.concat(variables, '\n')
end

local function format_multikey_function_name(i, fieldno)
    return ('M.check_multikey_%s(tuple_%s, multi_position)'):format(i, fieldno)
end

local function format_eq(check, internal_functions, internal_variables)
    local conditions = {}

    local parts = check.index_parts
    for j = 1, #check.values do
        local fieldno = check.field_nos[j]
        local value = check.values[j]
        local collation = parts[j].collation
        local part_type = parts[j].type
        local postfix = ''
        if parts[j].is_nullable == false then
            postfix = '_strict'
        end

        local fun
        if collation ~= nil then
            if collation == 'unicode' then
                fun = 'eq_unicode' .. postfix
            elseif collation == 'unicode_ci' then
                fun = 'eq_unicode_ci' .. postfix
            else
                error('unknown collation: ' .. tostring(collation))
            end
        else
            fun = 'eq'
        end

        local is_const = true
        if (part_type == 'decimal' or part_type == 'uuid') and value ~= box.NULL then
            local i = utils.table_count(internal_variables) + 1
            local name = part_type .. '_' .. tostring(i)
            table.insert(internal_variables,
                {name = name, type = part_type, value = value})
            is_const = false
            value = name
        end

        local is_multikey = parts[j].path ~= nil

        if is_multikey == true then
            local i = utils.table_count(internal_functions) + 1
            local multi_key_code = format_multikey_cmp_template(
                multikey_cmp_template_one, i, fun, value, is_const)

            internal_functions['check_multikey_' .. i] = multi_key_code
            table.insert(conditions, format_multikey_function_name(i, fieldno))
        else
            table.insert(conditions,
                format_complex_condition(fieldno, fun, value, is_const)
            )
        end
    end

    return conditions
end

local function format_like(check, is_ci)
    local conditions = {}

    for j = 1, #check.values do
        local fieldno = check.field_nos[j]
        local value = check.values[j]

        local fun
        if value == nil then
            fun = 'eq'
        else
            fun = is_ci and 'like_ci' or 'like'
        end

        table.insert(conditions,
            format_complex_condition(fieldno, fun, prepare_pattern(value), true)
        )
    end

    return conditions
end

local function format_lt(check, internal_functions, internal_variables)
    local conditions = {}
    local parts = check.index_parts

    for j = 1, #check.values do
        local fieldno = check.field_nos[j]
        local value = check.values[j]
        local collation = parts[j].collation
        local part_type = parts[j].type
        local postfix = ''
        if parts[j].is_nullable == false then
            postfix = '_strict'
        end

        local fun
        if collation ~= nil then
            if collation == 'unicode' then
                fun = 'lt_unicode'
            elseif collation == 'unicode_ci' then
                fun = 'lt_unicode_ci'
            else
                error('unknown collation: ' .. tostring(collation))
            end
        elseif part_type == 'boolean' then
            fun = 'lt_boolean'
        else
            fun = 'lt'
        end
        fun = fun .. postfix

        local is_const = true
        if (part_type == 'decimal' or part_type == 'uuid') and value ~= box.NULL then
            local i = utils.table_count(internal_variables) + 1
            local name = part_type .. '_' .. tostring(i)
            table.insert(internal_variables,
                {name = name, type = part_type, value = value})
            is_const = false
            value = name
        end

        local is_multikey = parts[j].path ~= nil

        if is_multikey == true then
            local i = utils.table_count(internal_functions) + 1
            local multi_key_code
            if check.comparator == '>' or check.comparator == '>=' then
                multi_key_code = format_multikey_cmp_template(
                    multikey_cmp_template_all, i, fun, value, is_const)
            else
                multi_key_code = format_multikey_cmp_template(
                    multikey_cmp_template_one, i, fun, value, is_const)
            end

            internal_functions['check_multikey_' .. i] = multi_key_code
            table.insert(conditions, format_multikey_function_name(i, fieldno))
        else
            table.insert(conditions,
                format_complex_condition(fieldno, fun, value, is_const)
            )
        end
    end

    return conditions
end

local function concat_conditions(conditions)
    return '(' .. table.concat(conditions, ' and ') .. ')'
end

local function define_variable(variable_def)
    local result = 'local ' .. variable_def.name .. ' = '
    if variable_def.type == 'decimal' then
        result = result .. 'decimal.new(' .. format_value(variable_def.value) .. ')'
    elseif variable_def.type == 'uuid' then
        result = result .. 'assert(uuid.fromstr(' .. format_value(variable_def.value) .. '), "Expected UUID type")'
    end

    return result
end

local function extract_tuple_field_numbers(checks)
    local num_dict = {}
    local num_array = {}

    for _, check in ipairs(checks) do
        for i in ipairs(check.values) do
            local num = check.field_nos[i]
            if num_dict[num] ~= true then
                table.insert(num_array, num)
                num_dict[num] = true
            end
        end
    end

    return num_array
end

local function function_args_by_fieldnos(fieldnos)
    local arg_names = {}
    for _, fieldno in ipairs(fieldnos) do
        table.insert(arg_names, 'tuple_' .. fieldno)
    end
    return table.concat(arg_names, ', ')
end

local function gen_index_filter(filter_conditions)
    if #filter_conditions == 0 then
        return nil
    end

    local subcheckers = {}
    local variables = {}
    local code = ''

    local if_prefix
    for i, cond in ipairs(filter_conditions) do
        if #cond.values > 0 then
            -- variables table is passed only at first function to avoid values duplication
            local eq_conds = format_eq(cond, subcheckers, variables)
            local lt_conds = format_lt(cond, subcheckers, {})

            local comparator = cond.comparator

            local args_fieldnos = { unpack(cond.field_nos, 1, #cond.values) }
            local fun_args = function_args_by_fieldnos(args_fieldnos) .. ', multi_position'
            local generated_code, func_name
            if comparator == '==' then
                local eq = concat_conditions(eq_conds)
                generated_code = eq_template:format(i, fun_args, eq)
                func_name = ('eq_%s'):format(i)
            elseif LIKE_OPS[comparator] then
                local is_ci_like = comparator == "ILIKE"
                local ci_prefix = ""
                if is_ci_like then ci_prefix = "i" end
                local like_conds = format_like(cond, is_ci_like)
                local like = concat_conditions(like_conds)
                generated_code = like_template:format(ci_prefix, i, fun_args, like)
                func_name = ('%slike_%s'):format(ci_prefix, i)
            else
                func_name = ('cmp_array_%s'):format(i)
                generated_code = gen_cmp_array_code(i, eq_conds, lt_conds, comparator, fun_args)
            end

            subcheckers[func_name] = generated_code

            filter_conditions.opts = filter_conditions.opts or {}

            if i == 1 then
                if_prefix = 'if not '
            else
                if_prefix = 'elseif not '
            end

            code = ('%s%s%s(%s) then return false, %s\n'):format(
                code, if_prefix, func_name, fun_args, cond.opts.is_early_exit)
        end
    end

    local tuple_fields_no = extract_tuple_field_numbers(filter_conditions)
    local tuple_fields_def = gen_cache_tuple_fields_code(tuple_fields_no)
    if tuple_fields_def ~= nil then
        code = ('%s%s\n%send\nreturn true, false'):format(checks_template, tuple_fields_def, code)
    else
        code = 'return true, false'
    end

    -- Library to avoid recreation function on each call
    local variables_code = {}
    for _, variable_def in ipairs(variables) do
        table.insert(variables_code, define_variable(variable_def))
    end
    variables_code = table.concat(variables_code, '\n')
    if #variables_code > 0 then
        variables_code = variables_code .. '\n\n'
    end

    local checkers_code_lines = {}
    for _, internal_function_code in pairs(subcheckers) do
        table.insert(checkers_code_lines, internal_function_code)
    end
    local checkers_code = table.concat(checkers_code_lines, '\n')
    checkers_code = 'local M = {}\n\n' .. variables_code .. checkers_code .. 'return M'

    return {
        code = code,
        library = checkers_code,
    }
end

local function lt_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return lhs < rhs
end

local function lt_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return lhs < rhs
end

local function lt_unicode_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) < 0
end

local function lt_unicode_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) < 0
end

local function lt_boolean_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return (not lhs) and rhs
end

local function lt_boolean_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return (not lhs) and rhs
end

local function lt_unicode_ci_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) < 0
end

local function lt_unicode_ci_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) < 0
end

local function eq(lhs, rhs)
    return lhs == rhs
end

local function like(str, reg)
    if str == nil and reg == nil then
        return true
    end
    if str == nil or reg == nil then
        return false
    end
    return str:find(reg)
end

local function eq_unicode_nullable(lhs, rhs)
    if lhs == nil and rhs == nil then
        return true
    elseif lhs == nil or rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) == 0
end

local function eq_unicode_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) == 0
end

local function eq_unicode_ci_nullable(lhs, rhs)
    if lhs == nil and rhs == nil then
        return true
    elseif lhs == nil or rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) == 0
end

local function eq_unicode_ci_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) == 0
end

local function like_ci_nullable(str, reg)
    if str == nil and reg == nil then
        return true
    elseif str == nil or reg == nil then
        return false
    end
    return string.match(string.lower(str), string.lower(tostring(reg)))
end

local comparators = {
    -- EQ
    eq = eq,
    -- nullable
    eq_unicode = eq_unicode_nullable,
    eq_unicode_ci = eq_unicode_ci_nullable,
    -- strict
    eq_unicode_strict = eq_unicode_strict,
    eq_unicode_ci_strict = eq_unicode_ci_strict,

    -- LT
    -- nullable
    lt = lt_nullable,
    lt_unicode = lt_unicode_nullable,
    lt_unicode_ci = lt_unicode_ci_nullable,
    lt_boolean = lt_boolean_nullable,
    -- strict
    lt_strict = lt_strict,
    lt_unicode_strict = lt_unicode_strict,
    lt_unicode_ci_strict = lt_unicode_ci_strict,
    lt_boolean_strict = lt_boolean_strict,

    -- LIKE
    like = like,
    like_ci = like_ci_nullable,

    utf8 = utf8,
    -- for multikey
    ipairs = ipairs,
    -- NULL
    NULL = box.NULL,

    -- decimal
    decimal = decimal,

    -- uuid
    uuid = uuid,
    assert = assert,
}

local function compile(filter)
   local lib, err = load(filter.library, 'library', 'bt', comparators)
   assert(lib, err)
   lib = lib()

    for name, f in pairs(comparators) do
        lib[name] = f
    end

    local fun, err = load(filter.code, 'code', 'bt', lib)
    assert(fun, err)
    return fun
end

return {
    gen_index_filter = gen_index_filter,
    compile = compile,
}
