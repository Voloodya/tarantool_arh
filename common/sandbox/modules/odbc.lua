local odbc = require('common.odbc')

return {
    execute = odbc.execute,
    prepare = odbc.prepare,
}
