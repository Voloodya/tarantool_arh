local module_name = 'account_manager.server'

local fiber = require('fiber')
local errors = require('errors')
local cartridge_pool = require('cartridge.pool')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local defaults = require('common.defaults')

local account_manager_cache_error = errors.new_class('account_manager_cache_error')
local account_manager_ping_error = errors.new_class('account_manager_ping_error')

vars:new_global('subscribers', {
    --[uri] = {
    --    fiber = fiber.self(),
    --    channel = channel,
    --    clock = 0,
    --    journal = {},
    --    failed_attempts_to_ping = 0,
    --},
})
vars:new_global('ping_subscribers_fiber')
vars:new_global('ping_subscribers_fiber_cond', fiber.cond())
vars:new_global('is_master', false)

local CHANNEL_CAPACITY = 1000
local UPDATE_TIMEOUT = 3
local MAX_FAILED_PING_ATTEMPTS = 5

local function ping_subscriber(uri)
    local connection = cartridge_pool.connect(uri)
    local subscriber = vars.subscribers[uri]

    if subscriber == nil then
        return
    end

    local clock = subscriber.clock
    if connection ~= nil and connection:is_connected() then
        local _, err = account_manager_ping_error:pcall(connection.call, connection, 'account_provider.ping', {clock},
            {timeout = defaults.ACCOUNT_MANAGER_PING_TIMEOUT})
        if err ~= nil then
            subscriber.failed_attempts_to_ping = subscriber.failed_attempts_to_ping + 1
        else
            subscriber.failed_attempts_to_ping = 0
        end
    else
        subscriber.failed_attempts_to_ping = subscriber.failed_attempts_to_ping + 1
    end

    if subscriber.failed_attempts_to_ping > MAX_FAILED_PING_ATTEMPTS then
        if subscriber.fiber ~= nil and subscriber.fiber:status() ~= 'dead' then
            subscriber.fiber:cancel()
        end

        if subscriber.channel ~= nil then
            subscriber.channel:close()
        end

        vars.subscribers[uri] = nil
    end
end

local function ping_subscribers()
    fiber.self():name('ping_subscribers')
    while pcall(fiber.testcancel) do
        for uri in pairs(vars.subscribers) do
            fiber.create(ping_subscriber, uri)
        end
        vars.ping_subscribers_fiber_cond:wait(defaults.ACCOUNT_MANAGER_PING_TIMEOUT)
    end
end

local function clear_subscriber_data(uri)
    local subscriber_fiber =  box.session.storage.subscriber
    local subscriber_channel =  box.session.storage.channel

    box.session.storage.is_account_manager_subscriber = nil
    box.session.storage.uri = nil
    box.session.storage.subscriber = nil
    box.session.storage.channel = nil

    -- Be careful this function can be called
    -- in on_disconnect trigger or on_connect
    -- we should not clear vars.subscribers list
    -- if we are in on_disconnect trigger and new connection is not established
    if vars.subscribers[uri] == nil then
        return
    end

    if vars.subscribers[uri].fiber == subscriber_fiber then
        vars.subscribers[uri] = nil
    end

    if subscriber_channel ~= nil then
        subscriber_channel:close()
    end

    if subscriber_fiber ~= nil then
        subscriber_fiber:cancel()
    end
end

local function on_disconnect_trigger()
    if box.session.storage.is_account_manager_subscriber ~= true then
        return
    end

    local uri = box.session.storage.uri
    log.warn('Disconnected %s from account_manager', uri)

    clear_subscriber_data(uri)
end

local function wait_updates_until_disconnected(uri)
    if vars.subscribers[uri] ~= nil then
        clear_subscriber_data(uri)
    end

    local subscriber_fiber = fiber.self()
    subscriber_fiber:name('account_manager: ' .. tostring(uri))
    local channel = fiber.channel(CHANNEL_CAPACITY)
    box.session.storage.uri = uri
    box.session.storage.is_account_manager_subscriber = true
    box.session.storage.subscriber = subscriber_fiber
    box.session.storage.channel = channel

    local subscriber = {
        fiber = subscriber_fiber,
        channel = channel,
        clock = 0,
        journal = {},
        failed_attempts_to_ping = 0,
    }
    vars.subscribers[uri] = subscriber

    while true do
        fiber.testcancel()

        if channel:is_closed() then
            clear_subscriber_data(uri)
            return
        end

        local object = channel:get(UPDATE_TIMEOUT)
        if object ~= nil then
            local _, err = box.session.push(object)
            if err ~= nil then
                clear_subscriber_data(uri)
                return
            end
        end
    end
end

local function sync(uri, clock)
    if vars.subscribers[uri] == nil then
        return nil, account_manager_cache_error:new("Subscriber is not found")
    end

    if vars.subscribers[uri].journal == nil then
        return nil, account_manager_cache_error:new("Update journal is not found")
    end

    local journal = vars.subscribers[uri].journal
    vars.subscribers[uri].journal = {}

    if clock == vars.subscribers[uri].clock then
        return {}
    end

    if clock > vars.subscribers[uri].clock then
        return nil, account_manager_cache_error:new(
            "Account provider's clock (%s) is greater than account manager's (%s)", clock, vars.subscribers[uri].clock)
    end

    local delta = {begin_n = clock + 1, end_n = vars.subscribers[uri].clock}

    for i = delta.begin_n, delta.end_n do
        if journal[i] == nil then
            return nil, account_manager_cache_error:new("Broken update journal")
        end
        delta[i] = journal[i]
    end

    return delta
end

_G.account_manager = nil
_G.account_manager = {
    wait_updates_until_disconnected = wait_updates_until_disconnected,
    sync = sync,
}

local ACCOUNT_MANAGER_MAX_JOURNAL_LENGTH = 10
local function notify_subscribers(entity, keys)
    for _, subscriber in pairs(vars.subscribers) do
        local message = {entity = entity, keys = keys, clock = subscriber.clock}
        subscriber.clock = subscriber.clock + 1
        subscriber.journal[subscriber.clock] = message
        subscriber.journal[subscriber.clock - ACCOUNT_MANAGER_MAX_JOURNAL_LENGTH] = nil
        subscriber.channel:put(message, 0)
    end
end

local function stop()
    local subscribers = vars.subscribers
    for _, subscriber in pairs(subscribers) do
        if subscriber.fiber:status() ~= 'dead' then
            subscriber.fiber:cancel()
        end
        subscriber.channel:close()
    end

    if vars.ping_subscribers_fiber ~= nil and vars.ping_subscribers_fiber:status() ~= 'dead' then
        vars.ping_subscribers_fiber:cancel()
    end
end

local function apply_config(_, opts)
    if vars.is_master == opts.is_master then
        return
    end
    -- May raise if trigger doesn't exist. And it's OK for first run and master switch
    pcall(box.session.on_disconnect, nil, on_disconnect_trigger)

    vars.is_master = opts.is_master
    if opts.is_master == false then
        stop()
        return
    end

    vars.subscribers = {}
    vars.ping_subscribers_fiber = fiber.new(ping_subscribers)
    box.session.on_disconnect(on_disconnect_trigger)
end

return {
    apply_config = apply_config,
    stop = stop,
    notify_subscribers = notify_subscribers,

    -- For tests
    ping_subscriber = ping_subscriber,
}
