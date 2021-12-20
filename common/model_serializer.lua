local ffi = require('ffi')
local json = require('json')
local decimal = require('decimal')
local uuid = require('uuid')
local utils = require('common.utils')

local model_deserialize = require('common.model.deserialize')
local model_serialize = require('common.model.serialize')

local function create_flatten_function(mdl, ddl, auxiliary_env)
    local code_lines = {}
    table.insert(code_lines,
        string.format('local result = {%s}', string.rep('NULL,', #ddl.format)))

    local ctx = {
        src_path = {},
        dst_path = {},

        lines = code_lines,
        ddl = ddl,
        variable = 'object',

        dst_index = '',

        states = {},

        array_nesting = {},
        nesting = 0,

        validation_path = {},
        validation_indexes = {},
        root = nil,

        env = auxiliary_env,
    }
    model_serialize.new(mdl, ctx)

    table.insert(code_lines, 'return result')


    for i, line in ipairs(code_lines) do
        code_lines[i] = '    ' .. line
    end

    local name = mdl.name
    table.insert(code_lines, 1, ('M[%q] = function(object)'):format(name))
    table.insert(code_lines, 'end\n')

    local code = table.concat(code_lines, '\n')
    return code, ctx.env
end

local function create_unflatten_function(mdl, ddl)
    local is_versioning_enabled = false
    for _, field in ipairs(ddl.format) do
        if field.name == 'version' then
            is_versioning_enabled = true
            break
        end
    end

    if is_versioning_enabled then
        table.insert(mdl.fields, {name = 'version', type = 'long'})
    end

    local code_lines = {}
    table.insert(code_lines, 'local result = {}')

    local ctx = {
        src_path = {},
        dst_path = {},

        lines = code_lines,
        ddl = ddl,
        src_variable = 'object',
        dst_variable = 'result',

        src_index = '',
        dst_index = '',

        states = {},

        array_nesting = {},
        nesting = 0,
    }
    model_deserialize.new(mdl, ctx)

    table.insert(code_lines, 'return result')

    for i, line in ipairs(code_lines) do
        code_lines[i] = '    ' .. line
    end

    table.insert(code_lines, 1, ('M[%q] = function(object)'):format(mdl.name))
    table.insert(code_lines, 'end\n')

    local code = table.concat(code_lines, '\n')

    return code
end

local format = string.format
local floor = math.floor

local function from_datetime(value)
    if value == nil then
        return box.NULL
    end

    local nsec, err = utils.nsec_to_iso8601_str(value)
    if nsec == nil then
        error(format('Unable to convert %s to datetime %s', value, err), 0)
    end
    return nsec
end

local function from_date(value)
    if value == nil then
        return box.NULL
    end

    local nsec, err = utils.nsec_to_date_str(value)
    if nsec == nil then
        error(format('Unable to convert %s to date %s', value, err), 0)
    end
    return nsec
end

local function from_time(value)
    if value == nil then
        return box.NULL
    end

    local nsec, err = utils.nsec_to_time_str(value)
    if nsec == nil then
        error(format('Unable to convert %s to date %s', value, err), 0)
    end
    return nsec
end

local function from_decimal(value)
    return value
end

local function from_uuid(value)
    return value
end

local function is_int_impl(data)
    if type(data) ~= 'number' and
        (not ffi.istype('int64_t', data)) and
        (not ffi.istype('uint64_t', data)) then
        return false
    end

    if data < -2147483648 or data > 2147483647 or floor(tonumber(data)) ~= data then
        return false
    end
    return true
end

local function to_int_var(data, path, ...)
    if not is_int_impl(data) then
        local fullpath = format(path, ...)
        error(format('%s is not an "int": %s', fullpath, json.encode(data)), 0)
    end
    return data
end

local function to_int(data, path)
    if not is_int_impl(data) then
        error(format('%s is not an "int": %s', path, json.encode(data)), 0)
    end
    return data
end

local function to_double_impl(data)
    local xtype = type(data)
    if xtype == "number" then
        return true
    else
        if xtype == "cdata" then
            local xdata = tonumber(data)
            if xdata == nil then
                return false
            else
                return true
            end
        end
    end
    return false
end

local function to_double(data, path)
    if not to_double_impl(data) then
        error(format('%s is not an "double": %s', path, json.encode(data)), 0)
    end
    return data
end

local function to_double_var(data, path, ...)
    if not to_double_impl(data) then
        local fullpath = format(path, ...)
        error(format('%s is not an "double": %s', fullpath, json.encode(data)), 0)
    end
    return data
end

local function to_long_impl(data)
    if type(data) ~= 'number' and
        (not ffi.istype('int64_t', data)) and
        (not ffi.istype('uint64_t', data)) then
        return false
    end

    local n = tonumber(data)
    -- note: if it's not a number or cdata(numbertype),
    --       the expression below will raise
    -- note: boundaries were carefully picked to avoid
    --       rounding errors, they are INT64_MIN and INT64_MAX+1,
    --       respectively (both 2**k)
    if n < -9223372036854775808 or n >= 9223372036854775808 or
       floor(n) ~= n then
        -- due to rounding errors, INT64_MAX-1023..INT64_MAX
        -- fails the range check above, check explicitly for this
        -- case; in number > cdata(uint64_t) expression, number
        -- is implicitly coerced to uint64_t
        if n ~= 9223372036854775808 or n > 9223372036854775807ULL then
            return false
        end
    end
    return true
end

local function to_long(data, path)
    if not to_long_impl(data) then
        error(format('%s is not a "long": %s', path, json.encode(data)), 0)
    end
    return data
end

local function to_long_var(data, path, ...)
    if not to_long_impl(data) then
        local fullpath = format(path, ...)
        error(format('%s is not a "long": %s', fullpath, json.encode(data)), 0)
    end
    return data
end

local function to_string(data, path)
    if type(data) ~= 'string' then
        error(format('%s is not a "string": %s', path, json.encode(data)), 0)
    end
    return data
end

local function to_string_var(data, path, ...)
    if type(data) ~= 'string' then
        local fullpath = format(path, ...)
        error(format('%s is not a "string": %s', fullpath, json.encode(data)), 0)
    end
    return data
end

local function to_boolean(data, path)
    if type(data) ~= 'boolean' then
        error(format('%s is not a "boolean": %s', path, json.encode(data)), 0)
    end
    return data
end

local function to_boolean_var(data, path, ...)
    if type(data) ~= 'boolean' then
        local fullpath = format(path, ...)
        error(format('%s is not a "boolean": %s', fullpath, json.encode(data)), 0)
    end
    return data
end

local function to_datetime(data, path)
    local nsec = utils.iso8601_str_to_nsec(data)
    if nsec == nil then
        error(format('%s is not a "DateTime": %s', path, json.encode(data)), 0)
    end
    return nsec
end

local function to_datetime_var(data, path, ...)
    local nsec = utils.iso8601_str_to_nsec(data)
    if nsec == nil then
        local fullpath = format(path, ...)
        error(format('%s is not a "DateTime": %s', fullpath, json.encode(data)), 0)
    end
    return nsec
end

local function to_date(data, path)
    local nsec = utils.date_str_to_nsec(data)
    if nsec == nil then
        error(format('%s is not a "Date": %s', path, json.encode(data)), 0)
    end
    return nsec
end

local function to_date_var(data, path, ...)
    local nsec = utils.date_str_to_nsec(data)
    if nsec == nil then
        local fullpath = format(path, ...)
        error(format('%s is not a "Date": %s', fullpath, json.encode(data)), 0)
    end
    return nsec
end

local function to_time(data, path)
    local nsec = utils.time_str_to_nsec(data)
    if nsec == nil then
        error(format('%s is not a "Time": %s', path, json.encode(data)), 0)
    end
    return nsec
end

local function to_time_var(data, path, ...)
    local nsec = utils.time_str_to_nsec(data)
    if nsec == nil then
        local fullpath = format(path, ...)
        error(format('%s is not a "Time": %s', fullpath, json.encode(data)), 0)
    end
    return nsec
end

local function to_decimal(data, path)
    local ok, result = pcall(decimal.new, data)
    if not ok then
        error(format('%s is not a "Decimal": %s', path, json.encode(data)), 0)
    end
    return result
end

local function to_decimal_var(data, path, ...)
    local ok, result = pcall(decimal.new, data)
    if not ok then
        local fullpath = format(path, ...)
        error(format('%s is not a "Decimal": %s', fullpath, json.encode(data)), 0)
    end
    return result
end

local function to_uuid(data, path)
    if uuid.is_uuid(data) then
        return data
    end

    local ok, result = pcall(uuid.fromstr, data)
    if ok == false or result == nil then
        error(format('%s is not a "UUID": %s', path, json.encode(data)), 0)
    end
    return result
end

local function to_uuid_var(data, path, ...)
    if uuid.is_uuid(data) then
        return data
    end

    local ok, result = pcall(uuid.fromstr, data)
    if ok == false or result == nil then
        local fullpath = format(path, ...)
        error(format('%s is not a "UUID": %s', fullpath, json.encode(data)), 0)
    end
    return result
end

local function to_any(data, path)
    if type(data) == 'nil' then
        error(format('%s is not an "any": %s', path, 'nil'), 0)
    end
    return data
end

local function to_any_var(data, path, ...)
    if type(data) == 'nil' then
        local fullpath = format(path, ...)
        error(format('%s is not an "any": %s', fullpath, 'nil'), 0)
    end
    return data
end

local function is_array(data, path)
    if type(data) ~= 'table' then
        error(format('%s is not an array: %s', path, json.encode(data)), 0)
    end

    for k in pairs(data) do
        if type(k) ~= 'number' then
            error(format('%s contains non-number keys: %s', path, json.encode(k)), 0)
        end
    end

    return data
end

local function is_array_var(data, path, ...)
    if type(data) ~= 'table' then
        local fullpath = format(path, ...)
        error(format('%s is not an array: %s', fullpath, json.encode(data)), 0)
    end

    for k in pairs(data) do
        if type(k) ~= 'number' then
            local fullpath = format(path, ...)
            error(format('%s contains non-number keys: %s', fullpath, json.encode(k)), 0)
        end
    end

    return data
end

local env = {
    NULL = box.NULL,
    pairs = pairs,
    ipairs = ipairs,
    next = next,
    format = format,
    table_new = table.new,

    -- to: serialize
    to_long = to_long,
    to_long_var = to_long_var,

    to_int = to_int,
    to_int_var = to_int_var,

    to_double = to_double,
    to_double_var = to_double_var,

    to_string = to_string,
    to_string_var = to_string_var,

    to_boolean = to_boolean,
    to_boolean_var = to_boolean_var,

    to_datetime = to_datetime,
    to_datetime_var = to_datetime_var,

    to_date = to_date,
    to_date_var = to_date_var,

    to_time = to_time,
    to_time_var = to_time_var,

    to_decimal = to_decimal,
    to_decimal_var = to_decimal_var,

    to_uuid = to_uuid,
    to_uuid_var = to_uuid_var,

    to_any = to_any,
    to_any_var = to_any_var,

    is_array = is_array,
    is_array_var = is_array_var,

    -- from: deserialize
    from_datetime = from_datetime,
    from_date = from_date,
    from_time = from_time,
    from_decimal = from_decimal,
    from_uuid = from_uuid,
}

local function generate_code(mdl, ddl)
    local serializers_code = 'local M = {}\n\n'
    local deserializers_code = 'local M = {}\n\n'

    local auxiliary_env = {enums = {}, records = {}, unions = {}}
    for _, record_def in ipairs(mdl) do
        if record_def.indexes ~= nil then
            local name = record_def.name
            local code = create_flatten_function(record_def, ddl[name], auxiliary_env)
            serializers_code = serializers_code .. code

            local record_def_copy = table.deepcopy(record_def)
            code = create_unflatten_function(record_def_copy, ddl[name])
            deserializers_code = deserializers_code .. code
        end
    end

    local result = {
        serializers_code = serializers_code .. '\nreturn M',
        deserializers_code = deserializers_code .. '\nreturn M',
        env = auxiliary_env,
    }

    return result
end

local function new(mdl, ddl)
    local result = generate_code(mdl, ddl)

    local code_env = table.copy(env)
    local auxiliary_env = result.env
    for name, fn in pairs(auxiliary_env.enums or {}) do
        code_env['is_enum_' .. name] = fn
    end

    for name, fn in pairs(auxiliary_env.records or {}) do
        code_env['is_record_' .. name] = fn
    end

    for num, fn in pairs(auxiliary_env.unions or {}) do
        code_env['to_union_' .. tostring(num)] = fn
    end

    local serializers_code = result.serializers_code
    local deserializers_code = result.deserializers_code

    local serializers = assert(load(serializers_code, '@serializers', 't', code_env))
    local deserializers = assert(load(deserializers_code, '@deserializers', 't', code_env))

    serializers = serializers()
    deserializers = deserializers()

    return serializers, deserializers
end

return {
    new = new,

    -- For tests
    generate_code = generate_code,
}
