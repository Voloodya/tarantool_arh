require('strict').on()
local ffi = require('ffi')

local fiber = require('fiber')

local odbc = require('odbc.odbc_lib')

local function SQL_SUCCEEDED(rc)
    if rc == odbc.SQL_INVALID_HANDLE then
        print('alarm @!!!s')
        print(debug.traceback())
    end
    if rc == odbc.SQL_SUCCESS or rc == odbc.SQL_SUCCESS_WITH_INFO then
        return true
    end
    return false
end

local function extract_errors(htype, handle)
    local num_recs = ffi.new('SQLLEN[1]', { 0 })
    local i = 1
    local native = ffi.new('SQLINTEGER[1]', { 0 })
    local state = ffi.new('char[7]', "")
    local text = ffi.new('char[256]', "")
    local len = ffi.new('SQLSMALLINT[1]', { 0 })
    local results = {}

    odbc.SQLGetDiagField(htype, handle, 0, odbc.SQL_DIAG_NUMBER, num_recs, 0, ffi.new("void*", nil))

    while i <= ffi.cast('uint64_t', num_recs) and
        odbc.SQLGetDiagRec(htype, handle, i, state, native,
                           text,
                           ffi.sizeof(text), len) ~= odbc.SQL_NO_DATA do
        table.insert(results,
                     {
                         sql_state = ffi.string(state),
                         record_number = i,
                         native_error = native[0],
                         message = ffi.string(text, len[0]),
                     })
        i = i + 1
    end
    return results
end

local function synchronized(mutex, timeout, func, ...)
    assert(mutex ~= nil)
    assert(mutex:size() == 1)

    if mutex:is_empty() then
        -- recursive locking the same fiber possible
        if fiber.self().storage['__odbc_connection_locked'] == true then
            local rc, res, err = pcall(func, ...)
            if rc then
                return res, err
            end
            error(res)
        end
    end
    local lock = mutex:get(timeout)
    fiber.self().storage['__odbc_connection_locked'] = true
    if not lock then
        -- timeout or anything
        return nil, { { message = 'connection is locked long time' } }
    end
    local rc, res, err = pcall(func, ...)
    fiber.self().storage['__odbc_connection_locked'] = false
    mutex:put(lock)
    if rc then
        return res, err
    end
    error(res)
end

return {
    SQL_SUCCEEDED = SQL_SUCCEEDED,
    extract_errors = extract_errors,
    synchronized = synchronized,
}
