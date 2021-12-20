local module_name = 'tasks.system.data_expiration'
local log = require('log.log').new(module_name)

local function start(_, type_name)
    local replicaset, err = vshard.router.routeall()
    if err ~= nil then
        return nil, err
    end

    for _, replica in pairs(replicaset) do
        local _, err = replica:callrw('vshard_proxy.start_expiration', { type_name }, { is_async = true })
        if err ~= nil then
            log.error('Expiration start failed: %s', err)
        end
    end
    return true
end

return {
    start = start,
}
