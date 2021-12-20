local cartridge = require('cartridge')

local M = {}

function M.vshard_is_bootstrapped()
    local vshard_groups = cartridge.config_get_readonly('vshard_groups')
    if vshard_groups == nil or vshard_groups.default == nil then
        return false
    end

    if vshard_groups.default.bootstrapped == false then
        return false
    end

    M.vshard_is_bootstrapped = function()
        return true
    end

    return true
end

function M.get_call_name(options)
    local mode
    local prefer_replica
    local balance

    if options ~= nil then
        mode = options.mode
        prefer_replica = options.prefer_replica
        balance = options.balance
    end

    if mode == 'write' then
        return 'callrw'
    end

    -- default value
    if prefer_replica == nil and balance == nil then
        return 'callbro'
    end

    if not prefer_replica and not balance then
        return 'callro'
    end

    if not prefer_replica and balance then
        return 'callbro'
    end

    if prefer_replica and not balance then
        return 'callre'
    end

    -- prefer_replica and balance
    return 'callbre'
end

return M
