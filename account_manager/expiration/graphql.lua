local types = require('graphql.types')
local graphql = require('common.graphql')
local tenant = require('common.tenant')
local expiration = require('account_manager.expiration.server')

local function init()
    graphql.add_callback({
        schema = 'admin',
        prefix = 'config',
        name = 'ban_inactive_more_seconds',
        doc = 'Get time limit for inactive users and tokens',
        kind = types.long.nonNull,
        callback = 'account_manager.expiration.graphql.get_ban_inactive_more_seconds',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'config',
        name = 'ban_inactive_more_seconds',
        doc = 'Set time limit for inactive users and tokens',
        args = { value = types.long.nonNull },
        kind = types.long.nonNull,
        callback = 'account_manager.expiration.graphql.set_ban_inactive_more_seconds',
    })
end

local function get_ban_inactive_more_seconds(_, _)
    return expiration.get_ban_inactive_more_seconds()
end

local function set_ban_inactive_more_seconds(_, args)
    local config = tenant.get_cfg_deepcopy('account_manager') or {}
    config.ban_inactive_more_seconds = args.value
    local _, err = tenant.patch_config({account_manager = config})
    if err ~= nil then
        return nil, err
    end

    return expiration.get_ban_inactive_more_seconds()
end

return {
    init = init,

    get_ban_inactive_more_seconds = get_ban_inactive_more_seconds,
    set_ban_inactive_more_seconds = set_ban_inactive_more_seconds,
}
