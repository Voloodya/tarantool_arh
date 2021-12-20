local module_name = 'account_provider.activity_observer'

local fiber = require('fiber')
local cartridge = require('cartridge')
local vars = require('common.vars').new(module_name)
local log = require('log.log').new(module_name)
local account = require('common.admin.account')
local constants = require('account_manager.constants')
local task = require('common.task')

vars:new_global('observer', nil)
vars:new_global('observer_user_storage', {})
vars:new_global('observer_token_storage', {})

local function flush_updates()
    local users = vars.observer_user_storage
    local _, err = cartridge.rpc_call('core',
        'update_users_activity', {users}, {leader_only = true})
    if err ~= nil then
        log.error('account provider error during users activity update: %s', err)
    else
        vars.observer_user_storage = {}
    end

    local tokens = vars.observer_token_storage
    local _, err = cartridge.rpc_call('core',
        'token_update_activity', {tokens}, {leader_only = true})
    if err ~= nil then
        log.error('account provider error during tokens activity update: %s', err)
    else
        vars.observer_token_storage = {}
    end
end


local function push()
    if account.is_empty() or
        account.is_anonymous() or
        account.is_unauthorized() then
        return
    end

    local uid = account.id()
    if uid == nil then
        return
    end

    if vars.observer == nil then
        vars.observer = task.start(module_name,'flush_updates',
            {interval = constants.ACTIVITY_TIMEOUT})
    end

    local time = fiber.time64() * 1000
    if account.is_user() then
        vars.observer_user_storage[uid] = time
    elseif account.is_token() then
        vars.observer_token_storage[uid] = time
    end
end

return {
    push = push,
    flush_updates = flush_updates,
}
