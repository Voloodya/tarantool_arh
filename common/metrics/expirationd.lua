local metrics = require('metrics')
local expirationd = require('expirationd')

local checked_count = metrics.gauge(
    'tdg_exporation_checked_count',
    'Expiration: count of checked tuples (expired + skipped)'
)

local expired_count = metrics.gauge(
    'tdg_exporation_expired_count',
    'Expiration: count of expired tuples'
)

local restarts = metrics.gauge(
    'tdg_exporation_restarts',
    'Expiration: count of task restarts'
)

local working_time = metrics.gauge(
    'tdg_exporation_working_time',
    'Expiration: task operation time'
)

local function update()
    local expirationd_stats = expirationd.stats()
    for name, stats in pairs(expirationd_stats) do
        local labels = { name = name }
        checked_count:set(stats.checked_count, labels)
        expired_count:set(stats.expired_count, labels)
        restarts:set(stats.restarts, labels)
        working_time:set(stats.working_time, labels)
    end
end

return {
    update = update,
}
