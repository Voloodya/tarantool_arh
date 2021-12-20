require('strict').on()

local log = require('log')
local ffi = require('ffi')

local odbc = require('odbc.odbc_lib')

local SQL_SUCCEEDED = require('odbc.utils').SQL_SUCCEEDED
local extract_errors = require('odbc.utils').extract_errors

local sql_utils = require('odbc.sql_utils')
local utils = require('odbc.utils')

--[[ Make methods synchronized ]]
local function make_synchronized(func)
    return function(...)
        local self = select(1, ...)
        return utils.synchronized(self.connection.mutex, 60*60*48, func, ...)
    end
end

local M = {}

M.execute = make_synchronized(function(self, params, opts)

    if self.closed == true then
        return nil, {{message="Cursor is closed"}}
    end

    opts = opts or {}
    params = params or {}
    local timeout = opts.timeout or self.statement_timeout
    timeout = timeout or 30

    local statement = self.connection.statement

    local rc = odbc.SQLSetStmtAttr(statement,
                                   odbc.SQL_ATTR_QUERY_TIMEOUT,
                                   ffi.cast('SQLPOINTER',timeout),
                                   odbc.SQL_IS_UINTEGER)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_STMT, statement)
    end

    local _, err = sql_utils.bind_params(self, statement, params)
    if err ~= nil then
        self:close()
        return nil, err
    end

    local rc = odbc.coio_SQLExecute(statement)
    if not SQL_SUCCEEDED(rc) and rc ~= odbc.SQL_NO_DATA then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, statement)
        self:close()
        return nil, errs
    end

    local rowcount = ffi.new('SQLLEN[1]', {0})
    local rc = odbc.SQLRowCount(statement, rowcount);
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, statement)
        self:close()
        return nil, errs
    end

    local colcount = ffi.new('SQLSMALLINT[1]', {0})
    local rc = odbc.SQLNumResultCols(statement, colcount)
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, statement)
        self:close()
        return nil, errs
    end

    if colcount[0] == 0 then
        --[[
            no results, e.g. UPDATE, INSERT
        ]]
        return rowcount[0]
    end

    local columns, err = sql_utils.extract_columns(statement, colcount[0])
    if err ~= nil then
        self:close()
        return nil, err
    end
    local pos = 0
    local results = {}
    while true do
        local rc = odbc.coio_SQLFetch(statement)
        if rc == odbc.SQL_NO_DATA then
            break
        end
        if not SQL_SUCCEEDED(rc) then
            local errs = extract_errors(odbc.SQL_HANDLE_STMT, statement)
            self:close()
            return nil, errs
        end
        local row, err = sql_utils.fetch_row(self.connection, columns)
        if err ~= nil then
            self:close()
            return nil, err
        end

        table.insert(results, row)
        pos = pos + 1
    end
    return results
end)

M.close = make_synchronized(function(self)

    if self.closed ~= true then
        self.closed = true
        local statement = self.connection.statement
        local rc = odbc.SQLFreeStmt(statement, odbc.SQL_CLOSE)
        if not SQL_SUCCEEDED(rc) then
            log.warn(extract_errors(odbc.SQL_HANDLE_STMT, self.connection.statement))
        end

        return true
    end
    return true -- TODO, maybe false
end)

function M.is_open(self)

    return self.closed ~= true
end

for _, implementation in pairs(M) do
    if type(implementation) == 'function' then
        jit.off(implementation, true)
    end
end

return M
