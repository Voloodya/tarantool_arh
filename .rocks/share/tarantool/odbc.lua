require('strict').on()
local ffi = require('ffi')

local fiber = require('fiber')

local odbc = require('odbc.odbc_lib')

local SQL_SUCCEEDED = require('odbc.utils').SQL_SUCCEEDED
local extract_errors = require('odbc.utils').extract_errors

local M = require('odbc.connection')

local ENV_M = {}

local function free_env(handle)
    if handle ~= odbc.SQL_NULL_HENV then
        odbc.SQLFreeHandle(odbc.SQL_HANDLE_ENV, handle)
    end
end

function ENV_M.close(self)
    ffi.gc(self.envhandle, nil)
    free_env(self.envhandle)
    self.envhandle = odbc.SQL_NULL_HENV
end

function ENV_M.create_env(opts)
    assert(type(opts) == 'nil' or type(opts) == 'table', "opts must be a table")

    opts = opts or {}
    if type(opts.decimal_as_luanumber) == 'nil' then
        opts.decimal_as_luanumber = true
    end

    local handlep = ffi.new('SQLHENV[1]', { ffi.cast('void*', odbc.SQL_NULL_HENV) })
    --[[
        Allocate the ODBC environment and save handle.
    ]]
    local rc = odbc.SQLAllocHandle(odbc.SQL_HANDLE_ENV, box.NULL, handlep);
    local handle = ffi.new('SQLHENV', handlep[0])
    ffi.gc(handle, free_env)

    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_ENV, handle)
    end

    --[[
        Notify ODBC that this is an ODBC 3.0 app.
    ]]
    local rc = odbc.SQLSetEnvAttr(handle, odbc.SQL_ATTR_ODBC_VERSION,
                                  ffi.cast('void*', odbc.SQL_OV_ODBC3), 0)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_ENV, handle)
    end

    local env = setmetatable({}, { __index = ENV_M })
    env.envhandle = handle
    env.decimal_as_luanumber = opts.decimal_as_luanumber

    return env
end

local function free_connection_handle(handle)
    if handle ~= odbc.SQL_NULL_HDBC then
        odbc.SQLDisconnect(handle)
        odbc.SQLFreeHandle(odbc.SQL_HANDLE_DBC, handle)
    end
end

local function free_statement(handle)
    if handle ~= odbc.SQL_NULL_HSTMT then
        odbc.SQLFreeHandle(odbc.SQL_HANDLE_STMT, handle)
    end
end

function ENV_M.connect(self, dsn, opts)
    assert(dsn ~= nil, "Please provide dsn argument")
    assert(type(dsn) == 'string', "Please provide string dsn argument")

    opts = opts or {}
    local timeout = opts.timeout or 30

    local connection = setmetatable({}, {__index=M})
    connection.children = setmetatable({}, {__mode="v"})

    --[[
        Allocate ODBC connection handle and connect.
    ]]
    local handlep = ffi.new('SQLHDBC[1]', { ffi.cast('SQLHDBC', odbc.SQL_NULL_HDBC) })
    local rc = odbc.SQLAllocHandle(odbc.SQL_HANDLE_DBC, self.envhandle, handlep)
    local handle = ffi.new('SQLHDBC', handlep[0])
    ffi.gc(handle, free_connection_handle)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_ENV, self.envhandle)
    end

    local rc = odbc.SQLSetConnectAttr(handle, odbc.SQL_ATTR_CONNECTION_TIMEOUT, ffi.cast('void*', timeout), 0)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, handle)
    end

    local rc = odbc.SQLSetConnectAttr(handle, odbc.SQL_ATTR_LOGIN_TIMEOUT, ffi.cast('void*', timeout), 0)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, handle)
    end

    connection.state = M.ODBC_CONNECTING
    --[[
        Connect to DATASOURCE
    ]]
    local rc = odbc.coio_SQLDriverConnect(
        handle,
        box.NULL,
        ffi.cast('SQLCHAR*', dsn),
        odbc.SQL_NTS,
        box.NULL,
        0,
        box.NULL,
        odbc.SQL_DRIVER_NOPROMPT)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, handle)
    end

    local connection = setmetatable({}, { __index = M })
    connection.env = self
    connection.handle = handle
    connection.state = M.ODBC_CONNECTED
    connection.mutex = fiber.channel(1)
    connection.mutex:put(true)

    --[[
        Preallocate statement handle
    ]]
    local handlep = ffi.new('SQLHSTMT[1]', { ffi.cast('SQLHSTMT', odbc.SQL_NULL_HSTMT) })
    local rc = odbc.SQLAllocHandle(odbc.SQL_HANDLE_STMT, connection.handle, handlep);
    local statement = ffi.new('SQLHSTMT', handlep[0])
    ffi.gc(statement, free_statement)
    if not SQL_SUCCEEDED(rc) then
        return nil, extract_errors(odbc.SQL_HANDLE_DBC, connection.handle)
    end

    connection.statement = statement
    connection.children = setmetatable({}, {__mode="v"})

    return connection
end

function ENV_M.drivers(self)
    --[[
        List drivers
    ]]
    local direction = odbc.SQL_FETCH_FIRST;
    local descr
    local descrlen = ffi.new('SQLSMALLINT[1]', { 0 })
    local attrs
    local attrslen = ffi.new('SQLSMALLINT[1]', { 0 })
    local drivers = {}
    while true do
        local rc = odbc.SQLDrivers(self.envhandle, direction,
                                   ffi.cast('char*', 0), 0, descrlen,
                                   ffi.cast('char*', 0), 0, attrslen)
        if rc == odbc.SQL_NO_DATA then
            break
        end
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_ENV, self.envhandle)
        end

        descr = ffi.new('char[?]', descrlen[0] + 1)
        attrs = ffi.new('char[?]', attrslen[0] + 1)

        local rc = odbc.SQLDrivers(self.envhandle, direction,
                                   descr, ffi.sizeof(descr), descrlen,
                                   attrs, ffi.sizeof(attrs), attrslen)
        if rc == odbc.SQL_NO_DATA then
            break
        end
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_ENV, self.envhandle)
        end
        table.insert(drivers, { name = ffi.string(descr), attributes = ffi.string(attrs) })
        direction = odbc.SQL_FETCH_NEXT;
    end
    return drivers
end

function ENV_M.datasources(self)
    --[[
        List datasources
    ]]
    local direction = odbc.SQL_FETCH_FIRST;
    local descr
    local descrlen = ffi.new('SQLSMALLINT[1]', { 0 })
    local attrs
    local attrslen = ffi.new('SQLSMALLINT[1]', { 0 })
    local drivers = {}
    while true do
        local rc = odbc.SQLDataSources(self.envhandle, direction,
                                       ffi.cast('char*', 0), 0, descrlen,
                                       ffi.cast('char*', 0), 0, attrslen)
        if rc == odbc.SQL_NO_DATA then
            break
        end
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_ENV, self.envhandle)
        end

        descr = ffi.new('char[?]', descrlen[0] + 1)
        attrs = ffi.new('char[?]', attrslen[0] + 1)

        local rc = odbc.SQLDataSources(self.envhandle, direction,
                                       descr, ffi.sizeof(descr), descrlen,
                                       attrs, ffi.sizeof(attrs), attrslen)
        if rc == odbc.SQL_NO_DATA then
            break
        end
        if not SQL_SUCCEEDED(rc) then
            return nil, extract_errors(odbc.SQL_HANDLE_ENV, self.envhandle)
        end
        table.insert(drivers, { desc = ffi.string(descr), attrs = ffi.string(attrs) })
        direction = odbc.SQL_FETCH_NEXT;
    end
    return drivers
end

ENV_M['isolation'] = {}
ENV_M['isolation']["READ_UNCOMMITTED"] = odbc.SQL_TXN_READ_UNCOMMITTED
ENV_M['isolation']["READ_COMMITTED"] = odbc.SQL_TXN_READ_COMMITTED
ENV_M['isolation']["REPEATABLE_READ"] = odbc.SQL_TXN_REPEATABLE_READ
ENV_M['isolation']["SERIALIZABLE"] = odbc.SQL_TXN_SERIALIZABLE

local P = require('odbc.pool')

function ENV_M.create_pool(self, opts)
    opts = opts or {}
    assert(opts.dsn ~= nil, "Provide dsn for pool")
    local dsn = opts.dsn
    local connect_opts = opts.connect_opts
    local size = opts.size or 5

    local pool = setmetatable({}, { __index = P })
    pool.env = self
    pool.dsn = dsn
    pool.connect_opts = connect_opts
    pool.size = size
    pool.connections = {}
    pool.queue = fiber.channel(size)
    pool.parked = {}
    pool.rent = {}
    return pool
end

for _, implementation in pairs(ENV_M) do
    if type(implementation) == 'function' then
        jit.off(implementation, true)
    end
end

return ENV_M
