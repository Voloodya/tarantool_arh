local null = box.NULL
local floor = math.floor
local format = string.format
local insert = table.insert
local concat = table.concat
local sub = string.sub
local ffi = require('ffi')
local debug = require('debug')
local uuid = require('uuid')
local decimal = require('decimal')
local is_uuid  = uuid.is_uuid
local is_decimal = decimal.is_decimal

local function type_tag(t)
    return (type(t) == 'string' and t) or t.name or t.type
end

local dcache = setmetatable({}, {__mode = 'k'})

local function get_union_tag_map(union)
    local res = dcache[union]
    if not res then
        res = {}
        for bi, b in ipairs(union) do
            res[type_tag(b)] = bi
        end
        dcache[union] = res
    end
    return res
end

local function get_enum_symbol_map(enum)
    local res = dcache[enum]
    if not res then
        res = {}
        for si, s in ipairs(enum.symbols) do
            res[s] = si
        end
        dcache[enum] = res
    end
    return res
end

local function get_record_field_map(record)
    local res = dcache[record]
    if not res then
        res = {}
        for fi, f in ipairs(record.fields) do
            res[f.name] = fi
        end
        dcache[record] = res
    end
    return res
end

local validate
local _

-- validate data against a schema
validate = function(schema, data)
    -- error handler peeks into ptr using debug.getlocal()
    local ptr
    local schematype = type(schema) == 'string' and schema or schema.type
    local logicalType = type(schema) == 'table' and schema.logicalType or nil
    -- primitives
    -- Note: sometimes we don't check the type explicitly, but instead
    -- rely on an operation to fail on a wrong type. Done with integer
    -- and fp types, also with tables.
    -- Due to this technique, a error message is often misleading,
    -- e.x. "attempt to perform arithmetic on a string value". Unless
    -- a message starts with '@', we replace it (see validate_data_eh).
    if schema.nullable and (data == null or data == nil) then
        return
    end
    if     schematype == 'null' then
        if data ~= null then
            error()
        end
    elseif schematype == 'boolean' then
        if type(data) ~= 'boolean' then
            error()
        end
    elseif schematype == 'int' then
        if data < -2147483648 or data > 2147483647 or floor(tonumber(data)) ~= data then
            error()
        end
    elseif schematype == 'long' then
        _ = data < 0
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
            if n ~= 9223372036854775808 or data > 9223372036854775807ULL then
                error()
            end
        end
    elseif schematype == 'double' or schematype == 'float' then
        local xtype = type(data)
        if xtype ~= "number" then
            if xtype == "cdata" then
                local xdata = tonumber(data)
                if xdata == nil then
                    -- `tonumber` returns `nil` in case of an error
                    -- crutch: replace data with typeof(data) to produce more
                    -- readable error message
                    data = ffi.typeof(data) -- luacheck: ignore
                    error()
                end
            else
                error()
            end
        end
    elseif logicalType == 'UUID' then
        if not is_uuid(data) then
            local ok, result = pcall(uuid.fromstr, data)
            if ok == false or result == nil then
                error(format('@Not a UUID: %s', data), 0)
            end
        end
    elseif logicalType == 'Decimal' then
        if not is_decimal(data) then
            local ok, result = pcall(decimal.new, data)
            if ok == false or result == nil then
                error(format('@Not a Decimal: %s', data), 0)
            end
        end
    elseif schematype == 'bytes' or schematype == 'string' then
        if type(data) ~= 'string' then
            error()
        end
    elseif schematype == 'enum' then
        if not get_enum_symbol_map(schema)[data] then
            error()
        end
    else
        -- Replace nil -> NULL to allow it to be a key in a table.
        data = data ~= nil and data or null
        -- record, enum, array
        if     schematype == 'record' then
            local fieldmap = get_record_field_map(schema)
            -- check if the data contains unknown fields
            for k, _ in pairs(data) do
                ptr = k -- luacheck: ignore
                local field = schema.fields[fieldmap[k]]
                if not field or field.name ~= k then
                    error('@Unknown field', 0)
                end
            end
            ptr = nil   -- luacheck: ignore
            -- validate data
            for _, field in ipairs(schema.fields) do
                if data[field.name] ~= nil then
                    -- a field is present in data
                    ptr = field.name -- luacheck: ignore
                    validate(field.type, data[field.name])
                    ptr = nil -- luacheck: ignore
                    -- value is nullable
                elseif not (field.type and field.type.nullable) and
                    -- value is nullable union
                        not (field.type and type(field.type) == 'table' and
                                #field.type > 0 and get_union_tag_map(field.type)['null']) then
                    error(format('@Field %s missing', field.name), 0)
                end
            end
        elseif schematype == 'array'  then
            for i, v in pairs(data) do
                ptr = i -- luacheck: ignore
                if type(i) ~= 'number' then
                    error('@Non-number array key', 0)
                end
                validate(schema.items, v)
            end
        elseif not schematype then -- union
            local tagmap = get_union_tag_map(schema)
            if data == null then
                if not tagmap['null'] then
                    error('@Unexpected type in union: null', 0)
                end
            else
                local k, v = next(data)
                local bpos = tagmap[k]
                ptr = k -- luacheck: ignore
                if not bpos then
                    error('@Unexpected key in union', 0)
                end
                validate(schema[bpos], v)
                ptr = next(data, k) -- luacheck: ignore
                if ptr then
                    error('@Unexpected key in union', 0)
                end
            end
        elseif schematype == 'any' then
            if type(data) == 'table' then
                for k, v in pairs(data) do
                    ptr = k -- luacheck: ignore
                    if type(k) == 'table' then
                        error('@Invalid key', 0)
                    end
                    validate('any', v)
                end
            end
        else
            assert(false)
        end
    end
    goto l
::l::
end

local function find_frames(func)
    local top
    for i = 2, 1000000 do
        local info = debug.getinfo(i)
        if not info then
            return 1, 0
        end
        if info.func == func then
            top = i
            break
        end
    end
    for i = top, 1000000 do
        local info = debug.getinfo(i)
        if not info or info.func ~= func then
            return top - 1, i - 2
        end
    end
end

local function handle_validation_error(err)
    local top, bottom = find_frames(validate)
    local path = {}
    for i = bottom, top, -1 do
        local _, ptr = debug.getlocal(i, 3)
        insert(path, (ptr ~= nil and tostring(ptr)) or nil)
    end
    if type(err) == 'string' and sub(err, 1, 1) == '@' then
        err = sub(err, 2)
    else
        local _, schema = debug.getlocal(top, 1)
        local _, data   = debug.getlocal(top, 2)
        err = format('Not a %s: %s', (
            type(schema) == 'table' and (
                schema.name or schema.type or 'union')) or schema, data)
    end
    if #path == 0 then
        return err
    else
        return format('%s: %s', concat(path, '/'), err)
    end
end

local function validate_data(schema, data)
    return xpcall(validate, handle_validation_error, schema, data)
end

return {
    validate_data = validate_data,
}
