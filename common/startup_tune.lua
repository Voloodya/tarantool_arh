local module_name = 'common.startup_tune'

local log = require('log.log').new(module_name)
local rlimit = require('common.rlimit')

local function banner(format, ...)
    log.info('**********')
    log.info(format, ...)
    log.info('**********')
end

local function init()
    local limits = {
        rlim_cur = rlimit.RLIM_INFINITY,
        rlim_max = rlimit.RLIM_INFINITY
    }
    local rc = rlimit.setrlimit(rlimit.RLIMIT_CORE, limits)
    if rc == nil then
        banner([[Unable to set max coredump size to unlimited!
Setting this parameter is necessary for troubleshooting purposes!
It's impossible to analyze application server crash situation now!
Please tune it with `ulimit -c unlimited` and restart service!]])
    end

    local fd_limits = {
        rlim_cur = rlimit.OPEN_MAX,
        rlim_max = rlimit.OPEN_MAX
    }
    local rc = rlimit.setrlimit(rlimit.RLIMIT_NOFILE, fd_limits)
    if rc == nil then
        banner(([[Unable to set max open files to %d!
Please tune it with `ulimit -n 20000` and restart service!]]):format(fd_limits.rlim_max))
    end
end

return {
    init = init
}
