require('strict').on()

local log = require('log')
local ffi = require('ffi')

local odbc = require('odbc.odbc_lib')

local SQL_SUCCEEDED = require('odbc.utils').SQL_SUCCEEDED
local extract_errors = require('odbc.utils').extract_errors

local sql_utils = require('odbc.sql_utils')
local utils = require('odbc.utils')

local ODBC_CONNECTING = 1
local ODBC_CONNECTED = 2
local ODBC_DISCONNECTING = 3
local ODBC_DISCONNECTED = 4

local DEFAULT_QUERY_TIMEOUT = 60

--[[ Make methods synchronized ]]
local function make_synchronized(func)
    return function(...)
        local self = select(1, ...)
        return utils.synchronized(self.mutex, 60*60*48, func, ...)
    end
end

local M = {}

M.ODBC_CONNECTING = ODBC_CONNECTING
M.ODBC_CONNECTED = ODBC_CONNECTED
M.ODBC_DISCONNECTING = ODBC_DISCONNECTING
M.ODBC_DISCONNECTED = ODBC_DISCONNECTED

function M.is_connected(self)
    return self.state == ODBC_CONNECTED
end

function M.state(self)
    return self.state
end

M.set_autocommit = make_synchronized(function(self, on)


    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end

    local value
    if on == true then
        value = odbc.SQL_AUTOCOMMIT_ON
    else
        value = odbc.SQL_AUTOCOMMIT_OFF
    end
    local rc = odbc.SQLSetConnectAttr(self.handle,
                                      odbc.SQL_ATTR_AUTOCOMMIT,
                                      ffi.cast('SQLPOINTER', value),
                                      odbc.SQL_IS_UINTEGER);
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, self.handle)
    end
    return true
end)

local function reset_statement(self)
    for k, child in pairs(self.children) do
        if child.close ~= nil then
            child:close()
        end
        self.children[k] = nil
    end

    if self.statement ~= odbc.SQL_NULL_HSTMT then
        local rc = odbc.SQLFreeStmt(self.statement, odbc.SQL_CLOSE)
        if not SQL_SUCCEEDED(rc) then
            log.warn(extract_errors(odbc.SQL_HANDLE_STMT, self.statement))
        end
    end
end

M.set_isolation = make_synchronized(function(self, level)
    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end
    reset_statement(self)

    local rc = odbc.SQLSetConnectAttr(self.handle,
                                      odbc.SQL_ATTR_TXN_ISOLATION,
                                      ffi.cast('SQLPOINTER', level),
                                      odbc.SQL_IS_UINTEGER);
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, self.handle)
    end
    return true
end)

function M.set_timeout(self, timeout)
    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end
    -- Make it assert
    if timeout < 0 then
        return nil, "Timeout have to be positive"
    end
    self.statement_timeout = timeout
    return true
end

---- Commits previously opened transaction
-- @function
M.commit = make_synchronized(function(self)

    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end

    local rc = odbc.coio_SQLEndTran(odbc.SQL_HANDLE_DBC, self.handle,
                                    ffi.cast('SQLSMALLINT', odbc.SQL_COMMIT))
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, self.handle)
    end
    return true
end)

M.rollback = make_synchronized(function(self)

    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end
    local rc = odbc.coio_SQLEndTran(odbc.SQL_HANDLE_DBC, self.handle,
                                    ffi.cast('SQLSMALLINT', odbc.SQL_ROLLBACK))
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, self.handle)
    end
    return true
end)

M.close = make_synchronized(function(self)
    if self.state ~= ODBC_DISCONNECTING
        and self.state ~= ODBC_DISCONNECTED
    then
        self.state = ODBC_DISCONNECTING
        --[[
            Deallocate Statement Handle
        ]]
        reset_statement(self)
        odbc.SQLFreeHandle(odbc.SQL_HANDLE_STMT, ffi.gc(self.statement, nil))
        self.statement = odbc.SQL_NULL_HSTMT

        local rc = odbc.SQLDisconnect(self.handle);
        if not SQL_SUCCEEDED(rc) then
            log.warn("Seems that odbc connection already closed")
        end
        odbc.SQLFreeHandle(odbc.SQL_HANDLE_DBC, ffi.gc(self.handle, nil))
        self.handle = odbc.SQL_NULL_HDBC
        self.state = ODBC_DISCONNECTED
        return true
    else
        return false
    end
end)

local function close_conn_if_errors(self, errs)
    if errs == nil then
        return false
    end
    for _, err in ipairs(errs) do
        if err.sql_state == '08001' or -- Client unable to establish connection
            err.sql_state == '08003' or -- Connection does not exist
            err.sql_state == '08004' or -- Server rejected the connection
            err.sql_state == '08007' or -- Connection failure during transaction
            err.sql_state == '08S01' or -- Communication link failure
            err.sql_state == 'HYT00' or -- Query timeout expired
            err.sql_state == 'HYT01' then
            -- Connection timeout expired
            self:close()
            return true
        end
    end
    return false
end

M.tables = make_synchronized(function(self)

    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end

    --[[
        Get table list
    ]]
    reset_statement(self)
    local rc = odbc.coio_SQLTables(self.statement, box.NULL, 0, box.NULL, 0, box.NULL, 0, box.NULL, 0)
    if not SQL_SUCCEEDED(rc) then
        -- force gc
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local rowcount = ffi.new('SQLLEN[1]', { 0 })
    local rc = odbc.SQLRowCount(self.statement, rowcount);
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local colcount = ffi.new('SQLSMALLINT[1]', { 0 })
    local rc = odbc.SQLNumResultCols(self.statement, colcount)
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local columns, errs = sql_utils.extract_columns(self.statement, colcount[0])
    if errs ~= nil then
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end
    local pos = 0
    local results = {}
    while true do
        local rc = odbc.coio_SQLFetch(self.statement)
        if rc == odbc.SQL_NO_DATA then
            break
        end
        if not SQL_SUCCEEDED(rc) then
            local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
            if close_conn_if_errors(self, errs) then
                return nil, errs
            end
            reset_statement(self)
            return nil, errs
        end
        local row, err = sql_utils.fetch_row(self, columns)
        if err ~= nil then
            if close_conn_if_errors(self, err) then
                return nil, err
            end
            reset_statement(self)
            return nil, err
        end

        table.insert(results, row)
        pos = pos + 1
    end
    reset_statement(self)
    return results
end)

M.execute = make_synchronized(function(self, query, params, opts)
    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end
    assert(self.handle ~= nil, "Connection invalid")
    assert(query ~= nil, "Provide query")
    assert(type(query) == 'string', "Provide string query")

    params = params or {}
    opts = opts or {}
    local timeout = opts.timeout or self.statement_timeout
    timeout = timeout or DEFAULT_QUERY_TIMEOUT

    reset_statement(self)

    local rc = odbc.SQLSetStmtAttr(self.statement,
                                   odbc.SQL_ATTR_QUERY_TIMEOUT,
                                   ffi.cast('SQLPOINTER', timeout),
                                   odbc.SQL_IS_UINTEGER)
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        return nil, extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
    end

    local _, errs = sql_utils.bind_params(self, self.statement, params)
    if errs ~= nil then
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        return nil, errs
    end

    local rc = odbc.coio_SQLExecDirect(self.statement,
                                       ffi.cast('SQLCHAR*', query),
                                       odbc.SQL_NTS);
    if not SQL_SUCCEEDED(rc) and rc ~= odbc.SQL_NO_DATA then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local rowcount = ffi.new('SQLLEN[1]', { 0 })
    local rc = odbc.SQLRowCount(self.statement, rowcount);
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local colcount = ffi.new('SQLSMALLINT[1]', { 0 })
    local rc = odbc.SQLNumResultCols(self.statement, colcount)
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    if colcount[0] == 0 then
        --[[
            no results, e.g. UPDATE, INSERT
        ]]
        reset_statement(self) -- ignore error
        return rowcount[0] ~= -1 and rowcount[0] or 0
    end

    local columns, errs = sql_utils.extract_columns(self.statement, colcount[0])
    if errs ~= nil then
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end
    local pos = 0
    local results = {}
    while true do
        local rc = odbc.coio_SQLFetch(self.statement)
        if rc == odbc.SQL_NO_DATA then
            break
        end
        if not SQL_SUCCEEDED(rc) then
            local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
            if close_conn_if_errors(self, errs) then
                return nil, errs
            end
            reset_statement(self)
            return nil, errs
        end
        local row, errs = sql_utils.fetch_row(self, columns)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        if errs ~= nil then
            reset_statement(self)
            return nil, errs
        end

        table.insert(results, row)
        pos = pos + 1
    end

    reset_statement(self)
    return results
end)

local prepare_m = require('odbc.prepare')

M.prepare = make_synchronized(function(self, query)
    assert(self.handle ~= nil, "Connection invalid")
    assert(query ~= nil, "Provide query")
    assert(type(query) == 'string', "Provide string query")

    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end

    reset_statement(self)
    local rc = odbc.coio_SQLPrepare(self.statement, ffi.cast('SQLCHAR*', query),
                                    odbc.SQL_NTS);
    if not SQL_SUCCEEDED(rc) and rc ~= odbc.SQL_NO_DATA then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        return nil, extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
    end

    local prepare = setmetatable({}, { __index = prepare_m })
    prepare.connection = self

    table.insert(self.children, prepare)
    return prepare
end)

local cursor_m = require('odbc.cursor')

M.cursor = make_synchronized(function(self, query, params, opts)
    assert(self.handle ~= nil, "Connection invalid")
    assert(query ~= nil, "Provide query")
    assert(type(query) == 'string', "Provide string query")

    if not self:is_connected() then
        return nil, { { message = 'connection is closed' } }
    end

    params = params or {}
    opts = opts or {}
    local timeout = opts.timeout or self.statement_timeout
    timeout = timeout or DEFAULT_QUERY_TIMEOUT

    reset_statement(self)
    local rc = odbc.SQLSetStmtAttr(self.statement,
                                   odbc.SQL_ATTR_QUERY_TIMEOUT,
                                   ffi.cast('SQLPOINTER', timeout),
                                   odbc.SQL_IS_UINTEGER)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
    end

    local _, errs = sql_utils.bind_params(self, self.statement, params)
    if errs ~= nil then
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local rc = odbc.coio_SQLExecDirect(self.statement, ffi.cast('SQLCHAR*', query),
                                       odbc.SQL_NTS);
    if not SQL_SUCCEEDED(rc) and rc ~= odbc.SQL_NO_DATA then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local colcount = ffi.new('SQLSMALLINT[1]', { 0 })
    local rc = odbc.SQLNumResultCols(self.statement, colcount)
    if not SQL_SUCCEEDED(rc) then
        local errs = extract_errors(odbc.SQL_HANDLE_STMT, self.statement)
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local columns, errs = sql_utils.extract_columns(self.statement, colcount[0])
    if errs ~= nil then
        if close_conn_if_errors(self, errs) then
            return nil, errs
        end
        reset_statement(self)
        return nil, errs
    end

    local cursor = setmetatable({}, { __index = cursor_m })
    cursor.connection = self
    cursor.colcount = colcount[0]
    cursor.columns = columns

    table.insert(self.children, cursor)
    return cursor
end)

-- backward compatilibilty
function M.drivers(self)
    return self.env:drivers()
end

function M.datasources(self)
    return self.env:datasources()
end

for _, implementation in pairs(M) do
    if type(implementation) == 'function' then
        jit.off(implementation, true)
    end
end

return M
