local module_name = 'account_manager.expiration.server'

local fiber = require('fiber')
local clock = require('clock')
local checks = require('checks')
local errors = require('errors')

local vars = require('common.vars').new(module_name)
local task = require('common.task')
local defaults = require('common.defaults')
local config_error = errors.new_class('account_manager_expiration_config_error')
local states = require('account_manager.states')
local constants = require('account_manager.constants')
local config_checks = require('common.config_checks').new(config_error)
local config_filter = require('common.config_filter')
local cartridge = require('cartridge')

local NANOSECONDS_IN_SECONDS = 1e9 * 1ULL
local MAX_BAN_INACTIVE_MORE_SECONDS = 45 * 24 * 60 * 60 -- 45 days in seconds

vars:new('ban_inactive_more_seconds', MAX_BAN_INACTIVE_MORE_SECONDS)
vars:new('expiration', nil)
vars:new('users')
vars:new('tokens')

local function get_interval_in_seconds_since(time64)
    return (clock.time64() - time64) / NANOSECONDS_IN_SECONDS
end

local function is_inactive(account)
    if account.is_deleted == true then
        return false
    end

    if account.state == states.BLOCKED then
        return false
    end

    local last_activity = account.created_at
    if account.last_login ~= nil and account.last_login > last_activity then
        last_activity = account.last_login
    end
    if account.unblocked_at ~= nil and account.unblocked_at > last_activity then
        last_activity = account.unblocked_at
    end

    if get_interval_in_seconds_since(last_activity) > vars.ban_inactive_more_seconds then
        return true
    end
    return false
end

local function block_if_inactive(cls, account)
    if is_inactive(account) then
        cls.set_state_impl(account, states.BLOCKED,
            ('Inactive more than %s seconds'):format(vars.ban_inactive_more_seconds))
    end
end

local function is_expired(account)
    if account.is_deleted == true then
        return false
    end

    if account.state == states.BLOCKED then
        return false
    end

    if account.expires_in == 0 then
        return false
    end

    if get_interval_in_seconds_since(account.created_at) > account.expires_in then
        return true
    end
    return false
end

local function block_if_expired(cls, account)
    if is_expired(account) then
        cls.set_state_impl(account, states.BLOCKED, 'Expired')
    end
end

local function check_expiration()
    if box.info.ro then
        box.ctl.wait_rw()
    end

    local count  = 0
    for _, tuple in box.space.tdg_users:pairs() do
        block_if_inactive(vars.users, tuple)
        block_if_expired(vars.users, tuple)

        count = count + 1
        if count % defaults.FORCE_YIELD_LIMIT == 0 then
            fiber.yield()
        end
    end

    for _, tuple in box.space.tdg_tokens:pairs() do
        block_if_inactive(vars.tokens, tuple)
        block_if_expired(vars.tokens, tuple)

        count = count + 1
        if count % defaults.FORCE_YIELD_LIMIT == 0 then
            fiber.yield()
        end
    end
end

local function get_ban_inactive_more_seconds()
    local cfg = cartridge.config_get_readonly('account_manager')

    if cfg == nil or cfg.ban_inactive_more_seconds == nil then
        return MAX_BAN_INACTIVE_MORE_SECONDS
    end

    local ban_inactive_more_seconds = MAX_BAN_INACTIVE_MORE_SECONDS
    if cfg.ban_inactive_more_seconds < MAX_BAN_INACTIVE_MORE_SECONDS then
        ban_inactive_more_seconds = cfg.ban_inactive_more_seconds
    end

    return ban_inactive_more_seconds
end

local function validate_config(cfg)
    checks('table')
    local conf = config_filter.compare_and_get(cfg, 'account_manager', module_name)
    if conf == nil then
        return true
    end

    config_checks:check_optional_luatype('account_manager.ban_inactive_more_seconds',
            conf.ban_inactive_more_seconds, 'number')
    if conf.ban_inactive_more_seconds ~= nil then
        config_checks:assert(conf.ban_inactive_more_seconds > 0,
            'ban_inactive_more_seconds should be greater than zero')
    end

    return true
end

local function apply_config(_)
    vars.users = require('account_manager.user')
    vars.tokens = require('account_manager.token')

    if box.info.ro then
        return
    end

    if vars.expiration == nil then
        vars.expiration = task.start(module_name, 'check_expiration',
            { interval = 3 * constants.ACTIVITY_TIMEOUT})
    end

    vars.ban_inactive_more_seconds = get_ban_inactive_more_seconds()
    return true
end

return {
    apply_config = apply_config,
    validate_config = validate_config,
    check_expiration = check_expiration,
    get_ban_inactive_more_seconds = get_ban_inactive_more_seconds,

    is_inactive = is_inactive,
    is_expired = is_expired,
}
