local defaults = require('common.defaults')

local readahead = 1024*1024*10
local memtx_max_tuple_size = 1024*1024*5
local vinyl_max_tuple_size = 1024*1024*5
local watchdog_timeout = defaults.WATCHDOG_TIMEOUT
local watchdog_enable_coredump = defaults.WATCHDOG_ENABLE_COREDUMP

return {
    binarydir = debug.sourcedir(),
    readahead = readahead,
    memtx_max_tuple_size = memtx_max_tuple_size,
    vinyl_max_tuple_size = vinyl_max_tuple_size,
    watchdog_timeout = watchdog_timeout,
    watchdog_enable_coredump = watchdog_enable_coredump,
    dev_mode = false,
}
