local module_name = 'notifier'

local fiber = require('fiber')
local checks = require('checks')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local task = require('common.task')
local tenant = require('common.tenant')

local queue = require('common.bounded_queue')

local QUEUE_SIZE = 1024
local SENDER_NUM = 4 -- luacheck: ignore

local INITIAL_TIMEOUT = 2
local INITIAL_MESSAGE_COUNT_THRESHOLD = 64

vars:new('composer', nil)
vars:new('smtp', nil)

vars:new('new_objects')
vars:new('repair_failures')
vars:new('new_objects_fiber', nil)
vars:new('repair_failures_fiber', nil)
vars:new('users')

local function send_mail(addr, subject, body)
    checks('string', 'string', 'string')
    local ok, err = vars.smtp.send(addr, subject, body)
    if not ok then
        log.error(err)
    end
end

local function sender(context)
    checks('table')

    fiber.sleep(context.timeout)

    if not vars.smtp or context.objects:is_empty() then
        return
    end

    local mail = context.composer(context.objects)

    -- How it works?
    -- To avoid banning from the mail server we try to reduce
    -- the number of sent messages. When we receive many messages
    -- we collect them in one message. Between the sending of a new message
    -- we are waiting for the given timeout. When messages come too fast we
    -- increase the timeout and the threshold of messages. When messages
    -- come at normal speed again we gradually reduce the timeout
    -- and the threshold to their initial value.

    if mail.objects_num > context.threshold then
        context.timeout = context.timeout * 2
        context.threshold = context.threshold * 2
    elseif context.timeout > INITIAL_TIMEOUT then
        context.timeout = context.timeout / 2
        context.threshold = context.threshold / 2
    end

    for _, user in pairs(vars.users) do
        tenant.fiber_new(send_mail, user.addr, mail.subject, mail.body)
    end

    context.objects:clear()
end

local function subscribe(user)
    checks({ id = 'string', name = 'string', addr = 'string' })
    vars.users = vars.users or {}
    vars.users[user.id] = user
end

local function unsubscribe_all()
    vars.users = {}
end

local function new_object_added(id, time, reason)
    checks('string', 'cdata|number', 'string')
    if vars.new_objects ~= nil then
        vars.new_objects.objects:push({ id = id, time = time, reason = reason })
    end
end

local function repair_failure(id, time, reason)
    checks('string', 'cdata|number', 'string')
    if vars.repair_failures ~= nil then
        vars.repair_failures.objects:push({ id = id, time = time, reason = reason })
    end
end

local function init(smtp_client, mail_composer, timeout)
    checks('?table', 'table', '?number')

    vars.composer = mail_composer
    vars.smtp = smtp_client

    if vars.new_objects == nil then
        vars.new_objects = {
            timeout = INITIAL_TIMEOUT,
            threshold = INITIAL_MESSAGE_COUNT_THRESHOLD,
            objects = queue.new(QUEUE_SIZE),
            composer = function (objects) return vars.composer.new_objects(objects) end,
        }
    end

    if vars.repair_failures == nil then
        vars.repair_failures = {
            timeout = INITIAL_TIMEOUT,
            threshold = INITIAL_MESSAGE_COUNT_THRESHOLD,
            objects = queue.new(QUEUE_SIZE),
            composer = function (objects) return vars.composer.repair_failures(objects) end,
        }
    end

    if timeout ~= nil then
        vars.new_objects.timeout = timeout
        vars.repair_failures.timeout = timeout
    end

    vars.new_objects_fiber = task.start(
        'notifier.notifier',
        'sender',
        { interval = 0 },
        vars.new_objects)

    vars.repair_failures_fiber = task.start(
        'notifier.notifier',
        'sender',
        { interval = 0 },
        vars.repair_failures)
end

local function deinit()
    vars.composer = nil
    vars.smtp = nil


    if vars.new_objects_fiber ~= nil then
        task.stop(vars.new_objects_fiber)
    end

    if vars.repair_failures_fiber ~= nil  then
        task.stop(vars.new_objects_fiber)
    end
end

return {
    init = init,
    deinit = deinit,
    subscribe = subscribe,
    unsubscribe_all = unsubscribe_all,
    new_object_added = new_object_added,
    repair_failure = repair_failure,
    sender = sender,
    INITIAL_TIMEOUT = INITIAL_TIMEOUT
}
