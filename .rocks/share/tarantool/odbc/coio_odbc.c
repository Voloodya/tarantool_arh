#include <tarantool/module.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <sql.h>
#include <sqlext.h>

ssize_t odbc_connect(va_list args) {
    SQLHDBC            hdbc = va_arg(args, SQLHDBC);
    SQLHWND            hwnd = va_arg(args, SQLHWND);
    SQLCHAR 		  *szConnStrIn = va_arg(args, SQLCHAR*);
    SQLSMALLINT        cbConnStrIn = va_arg(args, int);
    SQLCHAR           *szConnStrOut = va_arg(args, SQLCHAR*);
    SQLSMALLINT        cbConnStrOutMax = va_arg(args, int);
    SQLSMALLINT 	  *pcbConnStrOut = va_arg(args, SQLSMALLINT*);
    SQLUSMALLINT       fDriverCompletion = va_arg(args, unsigned int);

    // Connect to ODBC
    return SQLDriverConnect( hdbc,
                             hwnd,
                             szConnStrIn,
                             cbConnStrIn,
                             szConnStrOut,
                             cbConnStrOutMax,
                             pcbConnStrOut,
                             fDriverCompletion);
}

SQLRETURN coio_SQLDriverConnect(
    SQLHDBC            hdbc,
    SQLHWND            hwnd,
    SQLCHAR 		  *szConnStrIn,
    SQLSMALLINT        cbConnStrIn,
    SQLCHAR           *szConnStrOut,
    SQLSMALLINT        cbConnStrOutMax,
    SQLSMALLINT 	  *pcbConnStrOut,
    SQLUSMALLINT       fDriverCompletion) {
    return coio_call(odbc_connect,
                     hdbc,
                     hwnd,
                     szConnStrIn,
                     cbConnStrIn,
                     szConnStrOut,
                     cbConnStrOutMax,
                     pcbConnStrOut,
                     fDriverCompletion);
}

ssize_t odbc_exec_direct(va_list args) {
    SQLHSTMT StatementHandle = va_arg(args, SQLHSTMT);
    SQLCHAR *StatementText = va_arg(args, SQLCHAR*);
    SQLINTEGER TextLength = va_arg(args, SQLINTEGER);

    return SQLExecDirect(StatementHandle,
                         StatementText, TextLength);
}

SQLRETURN coio_SQLExecDirect(SQLHSTMT StatementHandle,
                             SQLCHAR *StatementText, SQLINTEGER TextLength) {
    return coio_call(odbc_exec_direct, StatementHandle, StatementText, TextLength);
}

ssize_t odbc_tables(va_list args) {
    SQLHSTMT StatementHandle = va_arg(args, SQLHSTMT);
    SQLCHAR *CatalogName = va_arg(args, SQLCHAR*);
    SQLSMALLINT NameLength1 = va_arg(args, int);
    SQLCHAR *SchemaName = va_arg(args, SQLCHAR*);
    SQLSMALLINT NameLength2 = va_arg(args, int);
    SQLCHAR *TableName = va_arg(args, SQLCHAR*);
    SQLSMALLINT NameLength3 = va_arg(args, int);
    SQLCHAR *TableType = va_arg(args, SQLCHAR*);
    SQLSMALLINT NameLength4 = va_arg(args, int);

    return SQLTables(StatementHandle,
                     CatalogName, NameLength1,
                     SchemaName, NameLength2,
                     TableName, NameLength3,
                     TableType, NameLength4);
}

SQLRETURN   SQL_API coio_SQLTables(SQLHSTMT StatementHandle,
                                   SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                   SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                   SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                   SQLCHAR *TableType, SQLSMALLINT NameLength4) {
    return coio_call(odbc_tables, StatementHandle,
                                  CatalogName, NameLength1,
                                  SchemaName, NameLength2,
                                  TableName, NameLength3,
                                  TableType, NameLength4);
}

ssize_t odbc_fetch(va_list args) {
    SQLHSTMT StatementHandle = va_arg(args, SQLHSTMT);
    return SQLFetch(StatementHandle);
}

SQLRETURN  SQL_API coio_SQLFetch(SQLHSTMT StatementHandle) {
    return coio_call(odbc_fetch, StatementHandle);
}

ssize_t odbc_prepare(va_list args) {
    SQLHSTMT StatementHandle = va_arg(args, SQLHSTMT);
    SQLCHAR *StatementText = va_arg(args, SQLCHAR*);
    SQLINTEGER TextLength = va_arg(args, SQLINTEGER);

    return SQLPrepare(StatementHandle,
                      StatementText, TextLength);
}

SQLRETURN coio_SQLPrepare(SQLHSTMT StatementHandle,
                          SQLCHAR *StatementText, SQLINTEGER TextLength) {
    return coio_call(odbc_prepare, StatementHandle, StatementText, TextLength);
}

ssize_t odbc_execute(va_list args) {
    SQLHSTMT StatementHandle = va_arg(args, SQLHSTMT);
    return SQLExecute(StatementHandle);
}

SQLRETURN  SQL_API coio_SQLExecute(SQLHSTMT StatementHandle) {
    return coio_call(odbc_execute, StatementHandle);
}


ssize_t odbc_end_tran(va_list args) {
    SQLSMALLINT HandleType = va_arg(args, int);
    SQLHANDLE Handle = va_arg(args, SQLHANDLE);
    SQLSMALLINT CompletionType = va_arg(args, int);
    return SQLEndTran(HandleType, Handle, CompletionType);
}

SQLRETURN coio_SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle,
                          SQLSMALLINT CompletionType) {
    return coio_call(odbc_end_tran, HandleType, Handle, CompletionType);
}

/* STUB FUNC TO REQUIRE */
int luaopen_odbc_libcoio_odbc(lua_State* L) {
    (void)L;
    return 0;
}
