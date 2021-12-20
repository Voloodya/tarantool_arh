local ffi = require( 'ffi' )
ffi.cdef [[
  typedef unsigned short WORD;
  typedef unsigned short USHORT;
  typedef uint32_t DWORD;

  typedef short RETCODE; //Alias
  typedef WORD WINAPI_ODBC_DSN_REQUEST; //Alias

  typedef short SQLSMALLINT; //Alias
  typedef unsigned short SQLUSMALLINT; //Alias
  typedef void* SQLPOINTER; //Alias
  typedef long SQLINTEGER; //Alias
  typedef int64_t SQLLEN; //Alias
  typedef uint64_t SQLULEN; //Alias
  typedef uint64_t SQLSETPOSIROW; //Alias

  typedef void* HWND;
  typedef HWND SQLHWND; //Alias
  typedef void* SQLHANDLE; //Alias
  typedef SQLHANDLE SQLHENV; //Alias
  typedef SQLHANDLE SQLHDBC; //Alias
  typedef SQLHANDLE SQLHSTMT; //Alias
  typedef SQLHANDLE SQLHDESC; //Alias
  typedef unsigned char SQLCHAR; //Alias

  typedef wchar_t TCHAR;
  typedef TCHAR* LPTSTR;

  typedef TCHAR SQLTCHAR; //Alias
  typedef LPTSTR SQLTCHAR; //Pointer
  typedef SQLSMALLINT SQLRETURN; //Alias

  static const uint32_t SQL_NULL_HENV = 0;
  static const uint32_t SQL_NULL_HDBC = 0;
  static const uint32_t SQL_NULL_HSTMT = 0;
  static const uint32_t SQL_NULL_HDESC = 0;
  static const uint32_t SQL_NULL_DESC = 0;

  static const uint32_t SQL_UNKNOWN_TYPE = 0;
  static const uint32_t SQL_CHAR =            1;
  static const uint32_t SQL_NUMERIC =         2;
  static const uint32_t SQL_DECIMAL =         3;
  static const uint32_t SQL_INTEGER =         4;
  static const uint32_t SQL_SMALLINT =        5;
  static const uint32_t SQL_FLOAT =           6;
  static const uint32_t SQL_REAL =            7;
  static const uint32_t SQL_DOUBLE =          8;

  static const uint32_t SQL_DATETIME =         9;

  static const uint32_t SQL_VARCHAR =        12;

  static const int32_t  SQL_DATE                                =9;
  static const int32_t  SQL_INTERVAL							=10;
  static const int32_t  SQL_TIME                                =10;
  static const int32_t  SQL_TIMESTAMP                           =11;
  static const int32_t  SQL_LONGVARCHAR                         =(-1);
  static const int32_t  SQL_BINARY                              =(-2);
  static const int32_t  SQL_VARBINARY                           =(-3);
  static const int32_t  SQL_LONGVARBINARY                       =(-4);
  static const int32_t  SQL_BIGINT                              =(-5);
  static const int32_t  SQL_TINYINT                             =(-6);
  static const int32_t  SQL_BIT                                 =(-7);
  static const int32_t  SQL_GUID				=(-11);

  /* One-parameter shortcuts for date/time data types */
  static const uint32_t SQL_TYPE_DATE =      91;
  static const uint32_t SQL_TYPE_TIME =      92;
  static const uint32_t SQL_TYPE_TIMESTAMP = 93;

  static const uint32_t SQL_C_CHAR = SQL_CHAR;                 /* CHAR, VARCHAR, DECIMAL, NUMERIC */
  static const uint32_t SQL_C_LONG = SQL_INTEGER;              /* INTEGER                      */
  static const uint32_t SQL_C_SHORT = SQL_SMALLINT;            /* SMALLINT                     */
  static const uint32_t SQL_C_FLOAT = SQL_REAL;                /* REAL                         */
  static const uint32_t SQL_C_DOUBLE = SQL_DOUBLE;             /* FLOAT, DOUBLE                */
  static const uint32_t SQL_C_NUMERIC = SQL_NUMERIC;

  static const uint32_t SQL_PARAM_TYPE_UNKNOWN           =0;
  static const uint32_t SQL_PARAM_INPUT                  =1;
  static const uint32_t SQL_PARAM_INPUT_OUTPUT           =2;
  static const uint32_t SQL_RESULT_COL                   =3;
  static const uint32_t SQL_PARAM_OUTPUT                 =4;
  static const uint32_t SQL_RETURN_VALUE                 =5;
  static const uint32_t SQL_PARAM_INPUT_OUTPUT_STREAM   =8;
  static const uint32_t SQL_PARAM_OUTPUT_STREAM         =16;

  /* Statement attribute values for cursor sensitivity */
  static const uint32_t SQL_UNSPECIFIED =      0;
  static const uint32_t SQL_INSENSITIVE =     1;
  static const uint32_t SQL_SENSITIVE =       2;

  /* GetTypeInfo() request for all data types */
  static const uint32_t SQL_ALL_TYPES =       0;

  static const uint32_t SQL_DIAG_NUMBER = 2;

  static const WINAPI_ODBC_DSN_REQUEST ODBC_ADD_DSN = 1;

  static const WINAPI_ODBC_DSN_REQUEST ODBC_CONFIG_DSN = 2;
  static const WINAPI_ODBC_DSN_REQUEST ODBC_REMOVE_DSN = 3;
  static const WINAPI_ODBC_DSN_REQUEST ODBC_ADD_SYS_DSN = 4;
  static const WINAPI_ODBC_DSN_REQUEST ODBC_CONFIG_SYS_DSN = 5;
  static const WINAPI_ODBC_DSN_REQUEST ODBC_REMOVE_SYS_DSN = 6;
  static const WINAPI_ODBC_DSN_REQUEST ODBC_REMOVE_DEFAULT_DSN = 7;
  typedef WORD WINAPI_ODBC_DRIVER_REQUEST; //Alias
  static const WINAPI_ODBC_DRIVER_REQUEST ODBC_INSTALL_DRIVER = 1;
  static const WINAPI_ODBC_DRIVER_REQUEST ODBC_REMOVE_DRIVER = 2;
  static const WINAPI_ODBC_DRIVER_REQUEST ODBC_CONFIG_DRIVER = 3;
  typedef WORD WINAPI_ODBC_INSTALL_REQUEST; //Alias
  static const WINAPI_ODBC_INSTALL_REQUEST ODBC_INSTALL_INQUIRY = 1;
  static const WINAPI_ODBC_INSTALL_REQUEST ODBC_INSTALL_COMPLETE = 2;
  typedef USHORT WINAPI_ODBC_CONFIG_MODE; //Alias
  static const WINAPI_ODBC_CONFIG_MODE ODBC_BOTH_DSN = 0;
  static const WINAPI_ODBC_CONFIG_MODE ODBC_USER_DSN = 1;
  static const WINAPI_ODBC_CONFIG_MODE ODBC_SYSTEM_DSN = 2;

  typedef uint32_t WINAPI_ODBC_ERROR_CODE;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_GENERAL_ERR = 1;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_BUFF_LEN = 2;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_HWND = 3;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_STR = 4;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_REQUEST_TYPE = 5;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_COMPONENT_NOT_FOUND = 6;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_NAME = 7;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_KEYWORD_VALUE = 8;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_DSN = 9;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_INF = 10;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_REQUEST_FAILED = 11;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_PATH = 12;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_LOAD_LIB_FAILED = 13;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_PARAM_SEQUENCE = 14;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_INVALID_LOG_FILE = 15;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_USER_CANCELED = 16;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_USAGE_UPDATE_FAILED = 17;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_CREATE_DSN_FAILED = 18;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_WRITING_SYSINFO_FAILED = 19;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_REMOVE_DSN_FAILED = 20;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_OUT_OF_MEM = 21;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_OUTPUT_STRING_TRUNCATED = 22;
  static const WINAPI_ODBC_ERROR_CODE ODBC_ERROR_NOTRANINFO = 23;

  static const uint32_t SQL_OV_ODBC2 = 2;
  static const uint32_t SQL_OV_ODBC3 = 3;
  static const uint32_t SQL_OV_ODBC3_80 = 380;

  static const uint32_t SQL_ATTR_ODBC_VERSION = 200;
  static const uint32_t SQL_ATTR_CONNECTION_POOLING = 201;
  static const uint32_t SQL_ATTR_CP_MATCH = 202;


  static const int32_t SQL_NTS = -3;
  static const uint32_t SQL_MAX_MESSAGE_LENGTH   = 512;

  static const uint32_t SQL_ACCESS_MODE                 =101;
  static const uint32_t SQL_AUTOCOMMIT                  =102;
  static const uint32_t SQL_LOGIN_TIMEOUT               =103;
  static const uint32_t SQL_OPT_TRACE                   =104;
  static const uint32_t SQL_OPT_TRACEFILE               =105;
  static const uint32_t SQL_TRANSLATE_DLL               =106;
  static const uint32_t SQL_TRANSLATE_OPTION            =107;
  static const uint32_t SQL_TXN_ISOLATION               =108;
  static const uint32_t SQL_CURRENT_QUALIFIER           =109;
  static const uint32_t SQL_ODBC_CURSORS                =110;
  static const uint32_t SQL_QUIET_MODE                  =111;
  static const uint32_t SQL_PACKET_SIZE                 =112;


  static const uint32_t SQL_ATTR_ACCESS_MODE		=SQL_ACCESS_MODE;
  static const uint32_t SQL_ATTR_AUTOCOMMIT			=SQL_AUTOCOMMIT;
  static const uint32_t SQL_ATTR_CONNECTION_TIMEOUT	=113;
  static const uint32_t SQL_ATTR_CURRENT_CATALOG	=SQL_CURRENT_QUALIFIER;
  static const uint32_t SQL_ATTR_DISCONNECT_BEHAVIOR	=114;
  static const uint32_t SQL_ATTR_ENLIST_IN_DTC		=1207;
  static const uint32_t SQL_ATTR_ENLIST_IN_XA		=1208;
  static const uint32_t SQL_ATTR_LOGIN_TIMEOUT		=SQL_LOGIN_TIMEOUT;
  static const uint32_t SQL_ATTR_ODBC_CURSORS		=SQL_ODBC_CURSORS;
  static const uint32_t SQL_ATTR_PACKET_SIZE		=SQL_PACKET_SIZE;
  static const uint32_t SQL_ATTR_QUIET_MODE			=SQL_QUIET_MODE;
  static const uint32_t SQL_ATTR_TRACE				=SQL_OPT_TRACE;
  static const uint32_t SQL_ATTR_TRACEFILE			=SQL_OPT_TRACEFILE;
  static const uint32_t SQL_ATTR_TRANSLATE_LIB		=SQL_TRANSLATE_DLL;
  static const uint32_t SQL_ATTR_TRANSLATE_OPTION	=SQL_TRANSLATE_OPTION;
  static const uint32_t SQL_ATTR_TXN_ISOLATION		=SQL_TXN_ISOLATION;


  static const uint32_t SQL_TXN_READ_UNCOMMITTED            =0x00000001;
  static const uint32_t SQL_TRANSACTION_READ_UNCOMMITTED	=SQL_TXN_READ_UNCOMMITTED;
  static const uint32_t SQL_TXN_READ_COMMITTED              =0x00000002;
  static const uint32_t SQL_TRANSACTION_READ_COMMITTED		=SQL_TXN_READ_COMMITTED;
  static const uint32_t SQL_TXN_REPEATABLE_READ             =0x00000004;
  static const uint32_t SQL_TRANSACTION_REPEATABLE_READ		=SQL_TXN_REPEATABLE_READ;
  static const uint32_t SQL_TXN_SERIALIZABLE                =0x00000008;
  static const uint32_t SQL_TRANSACTION_SERIALIZABLE		=SQL_TXN_SERIALIZABLE;

  static const uint32_t SQL_QUERY_TIMEOUT		=0;
  static const uint32_t SQL_MAX_ROWS			=1;
  static const uint32_t SQL_NOSCAN				=2;
  static const uint32_t SQL_MAX_LENGTH			=3;
  static const uint32_t SQL_ASYNC_ENABLE		=4;	/* same as SQL_ATTR_ASYNC_ENABLE */
  static const uint32_t SQL_BIND_TYPE			=5;
  static const uint32_t SQL_CURSOR_TYPE			=6;
  static const uint32_t SQL_CONCURRENCY			=7;
  static const uint32_t SQL_KEYSET_SIZE			=8;
  static const uint32_t SQL_ROWSET_SIZE			=9;
  static const uint32_t SQL_SIMULATE_CURSOR		=10;
  static const uint32_t SQL_RETRIEVE_DATA		=11;
  static const uint32_t SQL_USE_BOOKMARKS		=12;
  static const uint32_t SQL_GET_BOOKMARK		=13;      /*      GetStmtOption Only */
  static const uint32_t SQL_ROW_NUMBER			=14;      /*      GetStmtOption Only */

  static const int32_t SQL_IS_POINTER							=(-4);
  static const int32_t SQL_IS_UINTEGER							=(-5);
  static const int32_t SQL_IS_INTEGER							=(-6);
  static const int32_t SQL_IS_USMALLINT						=(-7);
  static const int32_t SQL_IS_SMALLINT							=(-8);

  static const uint32_t SQL_AUTOCOMMIT_OFF              =0;
  static const uint32_t SQL_AUTOCOMMIT_ON               =1;

  static const uint32_t SQL_ATTR_QUERY_TIMEOUT				=SQL_QUERY_TIMEOUT;

  static const SQLRETURN SQL_NULL_DATA = -1;
  static const SQLRETURN SQL_DATA_AT_EXEC = -2;
  static const SQLRETURN SQL_SUCCESS = 0;
  static const SQLRETURN SQL_SUCCESS_WITH_INFO = 1;
  static const SQLRETURN SQL_NO_DATA = 100;
  static const SQLRETURN SQL_PARAM_DATA_AVAILABLE = 101;
  static const SQLRETURN SQL_ERROR = -1;
  static const SQLRETURN SQL_INVALID_HANDLE = -2;
  static const SQLRETURN SQL_STILL_EXECUTING = 2;
  static const SQLRETURN SQL_NEED_DATA = 99;

  static const int32_t SQL_NO_TOTAL           =(-4);


  static const int32_t SQL_COLUMN_MONEY                = 9;
]]
