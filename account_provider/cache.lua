local module_name = 'account_provider.cache'

local fiber = require('fiber')
local lock_with_timeout = require('common.lock_with_timeout')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local defaults = require('common.defaults')
local tenant = require('common.tenant')

local membership = require('membership')
local errors = require('errors')
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')
local cartridge = require('cartridge')

local subscribe_error = errors.new_class('subscribe_error')
local account_provider_error = errors.new_class('account_provider_error')

local states = {
    DISCONNECTED = 'DISCONNECTED',
    CONNECTED = 'CONNECTED',
    OUT_OF_SERVICE = 'OUT_OF_SERVICE',
}

vars:new_global('users', {})
vars:new_global('tokens', {})
vars:new_global('access_actions', {})
vars:new_global('data_actions', {})

vars:new_global('state', states.DISCONNECTED)
vars:new_global('clock')
vars:new_global('applier')
vars:new_global('wakeup_applier', fiber.cond())
vars:new_global('wait_connected', fiber.cond())
vars:new_global('ping_observer')
vars:new_global('ping_observer_cond', fiber.cond())
vars:new_global('connection')
vars:new_global('failed_connection_attempts_count', 0)
vars:new_global('access_actions_locks', {})
vars:new_global('data_actions_locks', {})

local function set_state(state)
    if vars.state ~= state then
        log.warn('Account provider state is changed to %s', state)
        vars.state = state
    end
end

local function is_connected()
    if vars.connection == nil then
        return false
    end

    return vars.connection:is_connected()
end

local function reset_cache()
    vars.users = {}
    vars.tokens = {}
    vars.access_actions = {}
    vars.data_actions = {}
    vars.clock = 0
    vars.failed_connection_attempts_count = 0
end

local DEFAULT_TIMEOUT = 1

local function lock_data_actions_update(role_id)
    local tenant_uid = tenant.uid()
    vars.data_actions_locks[tenant_uid] = vars.data_actions_locks[tenant_uid] or {}
    while true do
        -- If lock exists and not released => wait
        local lock = vars.data_actions_locks[tenant_uid][role_id]
        if lock == nil or lock:released() then
            break
        end

        lock:wait()
        -- We are waked up via broadcast (positive case)
        -- or via timeout (negative case).

        -- Go to next iteration to check that someone else
        -- has't locked the section
    end
    local lock = lock_with_timeout.new(DEFAULT_TIMEOUT)
    vars.data_actions_locks[tenant_uid][role_id] = lock
    return lock
end

local function update_users(users)
    vars.users = {}
    for _, user in ipairs(users) do
        vars.users[user.login] = user
    end
end

local function update_tokens(tokens)
    vars.tokens = {}
    for _, token in ipairs(tokens) do
        vars.tokens[token.uid] = token
    end
end

local function lock_access_actions_update(role_id)
    while true do
        -- If lock exists and not released => wait
        local lock = vars.access_actions_locks[role_id]
        if lock == nil or lock:released() then
            break
        end

        lock:wait(DEFAULT_TIMEOUT)
        -- We are waked up via broadcast (positive case)
        -- or via timeout (negative case).

        -- Go to next iteration to check that someone else
        -- has't locked the section
    end
    local lock = lock_with_timeout.new(DEFAULT_TIMEOUT)
    vars.access_actions_locks[role_id] = lock
    return lock
end

local function update_roles_access_actions(role_id, access_actions)
    if vars.access_actions[role_id] == nil then
        vars.access_actions[role_id] = {}
    end
    for _, action in ipairs(access_actions) do
        vars.access_actions[role_id][action.id] = action.allowed or false
    end
end

local function invalidate_user(login)
    vars.users[login] = nil
end

local function invalidate_token(uid)
    vars.tokens[uid] = nil
end

local function invalidate_data_actions_for_role(role_id)
    for tenant_uid in pairs(vars.data_actions) do
        vars.data_actions[tenant_uid][role_id] = nil
    end
end

local function invalidate_access_role(role_id)
    vars.access_actions[role_id] = {}
    invalidate_data_actions_for_role(role_id)
end

local function invalidate_access_actions(role_id)
    vars.access_actions[role_id] = {}
    invalidate_data_actions_for_role(role_id)
end

local function invalidate_data_actions(role_ids)
    for _, role_id in ipairs(role_ids) do
        invalidate_data_actions_for_role(role_id)
    end
end

local function apply_update(payload)
    local entity = payload.entity
    local keys = payload.keys
    if entity == 'users' then
        invalidate_user(keys)
    elseif entity == 'tokens' then
        invalidate_token(keys)
    elseif entity == 'access_actions' then
        invalidate_access_actions(keys)
    elseif entity == 'data_actions' then
        invalidate_data_actions(keys)
    elseif entity == 'access_role' then
        invalidate_access_role(keys)
    end
end

local function get_connection()
    local candidates = cartridge_rpc.get_candidates('core', {leader_only = true})

    local _, candidate_uri = next(candidates)
    if candidate_uri ~= nil then
        local connection = cartridge_pool.connect(candidate_uri, {
            reconnect_after = nil,
            wait_connected = DEFAULT_TIMEOUT,
        })

        if connection ~= nil and connection:is_connected() == true then
            return connection
        end
    else
        return nil, account_provider_error:new('No cores available')
    end
    return nil
end

local sync

local function update_data(_, payload)
    local clock = payload.clock
    if clock ~= vars.clock then
        log.warn('There are missing updates from account manager. Local: %s, remote: %s', vars.clock, clock)
        return sync()
    end

    apply_update(payload)
    vars.clock = vars.clock + 1
end

local FAILED_CONNECTION_ATTEMPTS_THRESHOLD_TO_DISCONNECT = 5
local function inc_failed_connection_attempts_count()
    assert(vars.failed_connection_attempts_count ~= nil)
    vars.failed_connection_attempts_count = vars.failed_connection_attempts_count + 1
    if vars.failed_connection_attempts_count > FAILED_CONNECTION_ATTEMPTS_THRESHOLD_TO_DISCONNECT then
        set_state(states.DISCONNECTED)
        return false
    end
    return true
end

local function reset_failed_connection_attempts_count()
    vars.failed_connection_attempts_count = 0
end

local function subscribe_on_updates()
    fiber.self():name('account_provider:applier')
    while pcall(fiber.testcancel) do
        local connection, err = get_connection()

        if err ~= nil then
            log.error('Account provider error: %s', err)
            vars.wait_connected:broadcast()
        end

        if connection ~= nil and connection:is_connected() == true then
            reset_cache()
            set_state(states.CONNECTED)
            vars.connection = connection
            reset_failed_connection_attempts_count()
            log.info('Subscribe for account_manager updates (%s:%s)', connection.host, connection.port)
            vars.wait_connected:broadcast()
            local _, err = subscribe_error:pcall(connection.call, connection,
                'account_manager.wait_updates_until_disconnected', {membership.myself().uri}, {on_push = update_data})
            errors.wrap(err)
            log.warn('Disconnected from account_manager: %s', err)
            set_state(states.OUT_OF_SERVICE)
        else
            local ok = inc_failed_connection_attempts_count()
            if not ok then
                vars.applier = nil
                return
            end
        end

        vars.wakeup_applier:wait(DEFAULT_TIMEOUT)
    end
end

local function observe_pings()
    fiber.self():name('account_provider:observer')
    while true do
        -- If timeout is provided, and a signal doesn't happen for the duration of the timeout, wait() returns false.
        -- If a signal or broadcast happens, wait() returns true
        local ok = vars.ping_observer_cond:wait(5 * defaults.ACCOUNT_MANAGER_PING_TIMEOUT)
        if not ok and vars.connection ~= nil then
            vars.connection:close()
            vars.wakeup_applier:signal()
        end
    end
end

local function connect_to_account_manager()
    if is_connected() then
        return
    end

    if vars.applier == nil or vars.applier:status() == 'dead' then
        vars.applier = fiber.create(subscribe_on_updates)
    end

    if not is_connected() then
        vars.wakeup_applier:signal()
        vars.wait_connected:wait(DEFAULT_TIMEOUT)
    end

    if vars.ping_observer == nil then
        vars.ping_observer = fiber.create(observe_pings)
    end
end

sync = function()
    connect_to_account_manager()

    if not is_connected() then
        log.error('Failed to synchronized with account manager: no active connection')
        inc_failed_connection_attempts_count()
        return
    end
    reset_failed_connection_attempts_count()

    local data, err = subscribe_error:pcall(vars.connection.call, vars.connection, 'account_manager.sync',
        {membership.myself().uri, vars.clock})
    if err ~= nil then
        err = errors.wrap(err)
        log.warn('Account manager error: %s', err)
        vars.connection:close()
        return false
    end

    if data == nil then
        log.warn('Unexpected account manager sync response')
        vars.connection:close()
        return false
    end

    if next(data) == nil then
        return true
    end

    for i = data.begin_n, data.end_n, 1 do
        if data[i] ~= nil then
            apply_update(data[i])
        else
            log.warn('Empty update record')
            vars.connection:close()
            return false
        end
    end
    vars.clock = data.end_n
    return true
end

local function get_user(login, opts)
    opts = opts or {}

    if opts.without_cache ~= true and vars.users[login] ~= nil then
        return vars.users[login]
    end

    connect_to_account_manager()
    if not is_connected() then
        return nil, account_provider_error:new('Account provider is in %s state', vars.state)
    end

    local user, err = cartridge.rpc_call('core', 'get_user_by_login', {login})
    if user == nil then
        return nil, err
    end

    vars.users[login] = user

    return user
end

local function get_user_list()
    connect_to_account_manager()

    local data, err = cartridge.rpc_call('core', 'get_user_list', {})
    if err ~= nil then
        return nil, err
    end

    update_users(data)
    return data
end

local function get_token(uid)
    if vars.tokens[uid] ~= nil then
        return vars.tokens[uid]
    end

    connect_to_account_manager()
    if not is_connected() then
        return nil, account_provider_error:new('Account provider is in %s state', vars.state)
    end

    local token, err = cartridge.rpc_call('core', 'token_get', {uid})
    if token == nil then
        return nil, err
    end

    vars.tokens[uid] = token

    return token
end

local function get_token_by_name(name)
    for _, token in pairs(vars.tokens) do
        if token.name == name then
            return token
        end
    end

    connect_to_account_manager()
    if not is_connected() then
        return nil, account_provider_error:new('Account provider is in %s state', vars.state)
    end

    local token, err = cartridge.rpc_call('core', 'token_get_by_name', {name})
    if token == nil then
        return nil, err
    end

    vars.tokens[token.uid] = token

    return token
end

local function get_token_list()
    connect_to_account_manager()

    local data, err = cartridge.rpc_call('core', 'token_list', {})
    if err ~= nil then
        return nil, err
    end

    update_tokens(data)
    return data
end

local function get_access_role_action_list(role_id)
    connect_to_account_manager()
    local data, err = cartridge.rpc_call('core', 'get_access_role_actions', {role_id})
    if err ~= nil then
        return nil, err
    end
    update_roles_access_actions(role_id, data)
    return data
end

local function is_action_allowed(role_id, action)
    if vars.access_actions[role_id] ~= nil and vars.access_actions[role_id][action] ~= nil then
        return vars.access_actions[role_id][action]
    end
    return nil
end

local function get_access_role_action(role_id, action)
    local allowed = is_action_allowed(role_id, action)
    if allowed ~= nil then
        return allowed
    end

    local lock = lock_access_actions_update(role_id)
    allowed = is_action_allowed(role_id, action)
    if allowed ~= nil then
        lock:broadcast_and_release()
        return allowed
    end

    local _, err = get_access_role_action_list(role_id)
    if err ~= nil then
        lock:broadcast_and_release()
        return nil, err
    end
    lock:broadcast_and_release()

    return is_action_allowed(role_id, action)
end

local function is_data_action_allowed(role_id, aggregate, what)
    if vars.data_actions == nil then
        return nil
    end

    local tenant_uid = tenant.uid()
    if vars.data_actions[tenant_uid] == nil then
        return nil
    end

    if vars.data_actions[tenant_uid][role_id] == nil then
        return nil
    end

    local data_action = vars.data_actions[tenant_uid][role_id][aggregate] or {}
    return data_action[what] == true
end

local function get_role_data_actions(role_id)
    connect_to_account_manager()
    local tenant_uid = tenant.uid()
    local aggregates, err = cartridge.rpc_call('core', 'get_aggregate_access_list_for_role', {role_id})
    if err ~= nil then
        return nil, err
    end
    vars.data_actions[tenant_uid] = vars.data_actions[tenant_uid] or {}
    vars.data_actions[tenant_uid][role_id] = aggregates
    return vars.data_actions[tenant_uid][role_id]
end

local function check_data_action(role_id, aggregate, what)
    local allowed = is_data_action_allowed(role_id, aggregate, what)
    if allowed ~= nil then
        return allowed
    end

    local lock = lock_data_actions_update(role_id)
    allowed = is_data_action_allowed(role_id, aggregate, what)
    if allowed ~= nil then
        lock:broadcast_and_release()
        return allowed
    end

    local _, err = get_role_data_actions(role_id)
    if err ~= nil then
        lock:broadcast_and_release()
        return nil, err
    end
    lock:broadcast_and_release()

    return is_data_action_allowed(role_id, aggregate, what) == true
end

local function account_provider_ping(clock)
    if clock ~= vars.clock then
        fiber.create(sync)
    end
    vars.ping_observer_cond:signal()
    return true
end

_G.account_provider = {
    ping = account_provider_ping,
}

local function init()
    vars.state = states.DISCONNECTED
    reset_failed_connection_attempts_count()
end

return {
    init = init,

    get_user = get_user,
    get_user_list = get_user_list,

    get_token = get_token,
    get_token_by_name = get_token_by_name,
    get_token_list = get_token_list,

    get_access_role_action = get_access_role_action,
    get_access_role_action_list = get_access_role_action_list,

    check_data_action = check_data_action,

    -- For tests
    sync = sync,
}
