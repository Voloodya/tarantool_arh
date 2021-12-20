local watchdog = require('watchdog')

local log = require('log.log').new('watchdog')
local env = require('env')

local function init()
    local watchdog_timeout = env.watchdog_timeout
    if watchdog_timeout > 0 then
        local watchdog_enable_coredump = env.watchdog_enable_coredump
        log.info('Watchdog is started with timeout value: ' .. tostring(watchdog_timeout))
        watchdog.start(watchdog_timeout, watchdog_enable_coredump)
    else
        log.info('Watchdog is disabled')
    end
end


return {
    init = init,

    permanent = true,
    role_name = 'watchdog',
    dependencies = {},
}
