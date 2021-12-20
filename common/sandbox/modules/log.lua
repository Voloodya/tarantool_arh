local sandbox_log = require('log.log').new('sandbox')

return {
    error = sandbox_log.error,
    warn = sandbox_log.warn,
    info = sandbox_log.info,
    verbose = sandbox_log.verbose,
    debug = sandbox_log.debug
}
