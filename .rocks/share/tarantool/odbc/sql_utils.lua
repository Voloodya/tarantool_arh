local ffi = require('ffi')

local log = require('log')
local odbc = require('odbc.odbc_lib')
local decimal = require('decimal')

local SQL_SUCCEEDED = require('odbc.utils').SQL_SUCCEEDED
local extract_errors = require('odbc.utils').extract_errors

local function sql2luavalue(env, sqlvalue, column)
    if sqlvalue == box.NULL then
        return box.NULL
    end
    if column.data_type == odbc.SQL_CHAR
        or column.data_type == odbc.SQL_VARCHAR then
        return sqlvalue
    elseif column.data_type == odbc.SQL_INTEGER
        or column.data_type == odbc.SQL_BIGINT then
        return tonumber64(sqlvalue)
    elseif column.data_type == odbc.SQL_SMALLINT then
        return tonumber(sqlvalue)
    elseif column.data_type == odbc.SQL_FLOAT
        or column.data_type == odbc.SQL_REAL
        or column.data_type == odbc.SQL_DOUBLE then
        if tonumber(sqlvalue) ~= nil then
            return tonumber(sqlvalue)
        end
    elseif column.data_type == odbc.SQL_BINARY
        or column.data_type == odbc.SQL_LONGVARBINARY then
        return string.fromhex(sqlvalue)
    elseif column.data_type == odbc.SQL_DECIMAL
        or column.data_type == odbc.SQL_NUMERIC then
        if env.decimal_as_luanumber then
            if tonumber(sqlvalue) ~= nil then
                return tonumber(sqlvalue)
            end
        else
            local ok, res = pcall(decimal.new, sqlvalue)
            if ok then
                return res -- case for MONEY type
            end
        end
        return sqlvalue
    elseif column.data_type == odbc.SQL_BIT then
        return sqlvalue == '1'
    else
        return sqlvalue
    end
    return sqlvalue
end

local function extract_columns(statement, colcount)
    local columns = {}
    for i = 0, colcount - 1 do
        local name
        local ret_name_len = ffi.new('SQLSMALLINT[1]', { 0 })
        local data_type = ffi.new('SQLSMALLINT[1]', { 0 })
        local data_size = ffi.new('SQLULEN[1]', { 0 })
        local data_digits = ffi.new('SQLSMALLINT[1]', { 0 })
        local is_nullable = ffi.new('SQLSMALLINT[1]', { 0 })

        local rc = odbc.SQLDescribeCol(
            statement,
            i + 1,
            ffi.cast('char*', 0), 0, ret_name_len,
            data_type,
            data_size,
            data_digits,
            is_nullable)
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_STMT, statement)
        end
        name = ffi.new('char[?]', ret_name_len[0] + 1)

        local rc = odbc.SQLDescribeCol(
            statement,
            i + 1,
            name, ffi.sizeof(name), ret_name_len,
            data_type,
            data_size,
            data_digits,
            is_nullable)
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_STMT, statement)
        end

        local column = {
            name = ffi.string(name, ret_name_len[0]),
            data_size = data_size[0],
            data_type = data_type[0],
            is_nullable = is_nullable[0],
        }

        table.insert(columns, column)
    end
    return columns
end

local function fetch_row(connection, columns)
    local row = {}

    local buf
    local ind = ffi.new('SQLLEN[1]')

    local prealloc = 64 --<<< NOT LESS 22
    for i, column in ipairs(columns) do
        --[[
            Probably realloc
        ]]
        buf = ffi.new('char[?]', prealloc + 1)

        local rc = odbc.SQLGetData(connection.statement, i,
                                   odbc.SQL_C_CHAR,
                                   buf, ffi.sizeof(buf), ind)
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_STMT, connection.statement)
        end
        if ind[0] == odbc.SQL_NULL_DATA then
            row[column.name] = box.NULL
        elseif ind[0] == odbc.SQL_NO_TOTAL or ind[0] >= ffi.sizeof(buf) then
            row[column.name] = ffi.string(buf, ffi.sizeof(buf) - 1) -- remove last null

            while true do
                ind[0] = 0
                local rc = odbc.SQLGetData(connection.statement, i,
                                           odbc.SQL_C_CHAR,
                                           buf, ffi.sizeof(buf), ind)
                if rc == odbc.SQL_NO_DATA then
                    break
                end
                if not SQL_SUCCEEDED(rc) then
                    return nil, extract_errors(odbc.SQL_HANDLE_STMT, connection.statement)
                end
                if ind[0] > 0 and ind[0] < ffi.sizeof(buf) then
                    row[column.name] = row[column.name] .. ffi.string(buf, ind[0])
                elseif ind[0] == odbc.SQL_NO_TOTAL or ind[0] >= ffi.sizeof(buf) then
                    row[column.name] = row[column.name] .. ffi.string(buf, ffi.sizeof(buf) - 1)
                else
                    log.warn('SQLGetData strange indicator')
                    assert(false)
                end
            end
        else
            row[column.name] = ffi.string(buf, ind[0])
        end

        local converted = sql2luavalue(connection.env, row[column.name], column)
        if converted == nil and converted ~= box.NULL then
            log.warn("Could not convert type for val %s", row[column.name])
        else
            row[column.name] = converted
        end
    end
    return row
end

local function luatype2sql(value)
    local description = {

    }
    local t = type(value)
    if value == nil or t == nil then
        description.type = odbc.SQL_CHAR
        description.c_type = odbc.SQL_C_CHAR
        description.value = nil
        description.value_len = 0
        description.ind = ffi.new('SQLLEN[1]', { odbc.SQL_NULL_DATA })
    elseif t == 'number' then
        if math.floor(value) == value then
            description.type = odbc.SQL_BIGINT
            description.c_type = odbc.SQL_C_CHAR
            description.value = tostring(value)
            description.value_len = #description.value
        else
            description.type = odbc.SQL_DOUBLE
            description.c_type = odbc.SQL_C_CHAR
            description.value = tostring(value)
            description.value_len = #description.value
        end
    elseif t == 'string' then
        description.type = odbc.SQL_CHAR
        description.c_type = odbc.SQL_C_CHAR
        description.value = value
        description.value_len = #description.value
    elseif t == 'boolean' then
        description.type = odbc.SQL_BIT
        description.c_type = odbc.SQL_C_CHAR
        if value == true then
            description.value = '1'
        else
            description.value = '0'
        end
        description.value_len = #description.value
    elseif t == 'cdata' and ffi.istype('uint64_t', value) then
        description.type = odbc.SQL_BIGINT
        description.c_type = odbc.SQL_C_CHAR
        description.value = tostring(value):sub(1, -4)
        description.value_len = #description.value
    elseif t == 'cdata' and ffi.istype('int64_t', value) then
        description.type = odbc.SQL_BIGINT
        description.c_type = odbc.SQL_C_CHAR
        description.value = tostring(value):sub(1, -3)
        description.value_len = #description.value
    else
        description.type = odbc.SQL_CHAR
        description.c_type = odbc.SQL_C_CHAR
        description.value = tostring(value)
        description.value_len = #description.value
    end
    return description
end

local function bind_params(connection, statement, params)
    --[[
        clear previous
    ]]
    local rc = odbc.SQLFreeStmt(statement, odbc.SQL_RESET_PARAMS);
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_STMT, statement)
    end

    -- TODO assign to statement, not to connection
    connection.params = {} -- ffi.new cdata

    for i, param in ipairs(params) do
        local description = luatype2sql(param)
        table.insert(connection.params, description)

        local rc = odbc.SQLBindParameter(statement,
                                         i,
                                         odbc.SQL_PARAM_INPUT,
                                         description.c_type,
                                         description.type,
                                         0,
                                         0,
                                         ffi.cast('SQLPOINTER', description.value),
                                         description.value_len,
                                         ffi.cast('SQLPOINTER', description.ind))
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_STMT, statement)
        end
    end
    return true
end

return {
    extract_columns = extract_columns,
    fetch_row = fetch_row,
    bind_params = bind_params,
}
