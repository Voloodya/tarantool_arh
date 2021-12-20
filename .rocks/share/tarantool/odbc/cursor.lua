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

M.fetchrow = make_synchronized(function(self)

    if self:is_closed() then
        return nil, {{message='cursor is closed'}}
    end

    local statement = self.connection.statement

    local rowcount = ffi.new('SQLLEN[1]', { 0 })
    local rc = odbc.SQLRowCount(statement, rowcount);
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, statement)
        self:close()
        return nil, errs
    end

    if self.colcount == 0 then
        --[[
            no results, e.g. UPDATE, INSERT
        ]]
        -- TODO think about closing
        self:close()
        return rowcount[0]
    end

    local rc = odbc.coio_SQLFetch(statement)
    if rc == odbc.SQL_NO_DATA then
        self:close()
        return nil
    end

    local row, err = sql_utils.fetch_row(self.connection, self.columns)
    if err ~= nil then
        self:close()
        return nil, err
    end
    return row
end)

M.fetchall = make_synchronized(function(self)

    if self:is_closed() then
        return nil, {{message='cursor is closed'}}
    end

    local statement = self.connection.statement

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
        local row, err = sql_utils.fetch_row(self.connection, self.columns)
        if err ~= nil then
            self:close()
            return nil, err
        end

        table.insert(results, row)
    end
    return results
end)

M.fetch = make_synchronized(function(self, count)

    assert(count > 0, "Please provide positive count")
    if self:is_closed() then
        return nil, {{message='cursor is closed'}}
    end

    local statement = self.connection.statement

    local capture = 0
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
        local row, err = sql_utils.fetch_row(self.connection, self.columns)
        if err ~= nil then
            self:close()
            return nil, err
        end

        table.insert(results, row)
        capture = capture + 1
        if capture == count then
            break
        end
    end
    return results
end)

-- TODO? is_open

function M.is_open(self)

    return self.closed ~= true
end

function M.is_closed(self)

    return self.closed == true
end

M.close = make_synchronized(function(self)


    if self.closed ~= true then
        self.closed = true
        local rc = odbc.SQLFreeStmt(self.connection.statement, odbc.SQL_CLOSE)
        if not SQL_SUCCEEDED(rc) then
            log.warn(extract_errors(odbc.SQL_HANDLE_STMT, self.connection.statement))
        end

        return true
    end
    return true -- TODO, may be false
end)

for _, implementation in pairs(M) do
    if type(implementation) == 'function' then
        jit.off(implementation, true)
    end
end

return M
