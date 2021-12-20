local module_name = 'input_processor.repair_queue'

local env = require('env')
local errors = require('errors')
local clock = require('clock')
local fiber = require('fiber')
local log = require('log.log').new(module_name)
local vshard_utils = require('common.vshard_utils')

local request_context = require('common.request_context')
local storage = require('storage.repair_storage').new('tdg_input_repair')
local cartridge = require('cartridge')
local status = require('input_processor.repair_queue.statuses')
local task = require('common.task')
local tenant = require('common.tenant')

local repair_error = errors.new_class('runner_repair_error')

local BASE_SPACE_NAME = 'tdg_input_buffer'
local UNCLASSIFIED_ROUTING_KEY = '__unclassified__'

local RESEND_PERIOD_SEC = 60

local TIME_FIELD = 2
local REASON_FIELD = 3

local vars = require('common.vars').new(module_name)
vars:new('on_object_added')
vars:new('resend_fiber', nil)
vars:new('to_notify', true)

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    repair_error:assert(space, 'repair queue %s is not initialized', space_name)
    return space
end

local function put_tmp_object(obj)
    local space = get_space()
    space:replace(obj)
end

local function get_tmp_object(id)
    local space = get_space()
    return space:get(id):tomap({ names_only = true })
end

local function remove_tmp_object(id)
    local space = get_space()
    space:delete(id)
end

local function mark_as_to_resend(id, reason)
    local space = get_space()
    local time = clock.time64()

    space:update(id, {{ '=', TIME_FIELD, time }, {'=', REASON_FIELD, reason}})
end

local function start_processing(object)
    local context = request_context.get()
    context.start = clock.time()
    put_tmp_object({ context.id, box.NULL, box.NULL, object, context })
end

local function success()
    local context = request_context.get()

    if context.repair then
        log.info('repair completed successfully')
        local ok, err = storage:delete(context.id)
        if not ok then
            return nil, err
        end
    end

    remove_tmp_object(context.id)

    if env.dev_mode == true then
        local finish = clock.time()
        local ms = (finish - context.start) * 1000
        log.verbose('request took %.2f ms', ms)
    end

    return true
end

local function failure(routing_key, reason)
    local context = request_context.get()

    local reason_text = tostring(reason)

    if context.repair then
        log.warn('repair failure: %s', reason)

        local _, err = storage:update_status(context.id, status.REWORKED, reason_text)
        if err ~= nil then
            mark_as_to_resend(context.id, reason_text)
            return nil, repair_error:new("Can't update repair queue status: %s", err)
        end
    else
        log.warn('adding a new object to the repair queue: %s', reason)

        local object = get_tmp_object(context.id)

        local _, err = storage:save(object, status.NEW, reason_text, context)
        if err ~= nil then
            mark_as_to_resend(context.id, reason_text)
            return nil, repair_error:new("Adding to the repair queue failed: %s", err)
        end

        if routing_key == nil then
            routing_key = UNCLASSIFIED_ROUTING_KEY
        end

        local on_object_added = vars.on_object_added[routing_key]
        local postprocess_with_routing_key = on_object_added and on_object_added.postprocess_with_routing_key
        if postprocess_with_routing_key ~= nil then
            local _, err = cartridge.rpc_call('runner', 'handle_output_object',
                {nil, postprocess_with_routing_key, object}, {leader_only=true})
            if err ~= nil then
                return nil, repair_error:new(err)
            end
        end
    end

    remove_tmp_object(context.id)

    if vars.to_notify then
        local _, err = cartridge.rpc_call('core', 'notifier_repair_failure',
                         { context.id, context.start, reason_text },
                         { leader_only = true })
        if err ~= nil then
            return nil, repair_error:new("Failed to send notification: %s", err)
        end
    end
    return true
end

local function init()
    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    local space = box.space[space_name]

    if space ~= nil then
        -- Only for the first launch, not for every apply_config
        if vars.resend_fiber == nil then
            local reason = 'Aborted'
            for _, tuple in space.index.time:pairs({box.NULL}, {iterator = box.index.EQ}) do
                mark_as_to_resend(tuple.id, reason)
            end
        end
        return
    end

    box.begin()

    space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format({
        { name = 'id',      type = 'string' },
        { name = 'time',    type = 'unsigned', is_nullable = true },
        { name = 'reason',  type = 'string', is_nullable = true },
        { name = 'object',  type = 'map'    },
        { name = 'context', type = 'map'    },
    })

    space:create_index('id', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {
            { field = 'id', type = 'string' },
        },
    })

    space:create_index('time', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {
            { field = 'time', type = 'unsigned', is_nullable = true },
        },
    })

    box.commit()
end

local RESEND_TIMEOUT = 2
local function resend_objects()
    while not vshard_utils.vshard_is_bootstrapped() do
        fiber.sleep(RESEND_TIMEOUT)
    end

    local space = get_space()
    for _, t in space.index.time:pairs({0}, 'GT') do
        local _, err = storage:save(t:tomap({ names_only = true }), status.NEW, t.reason, t.context, t.time)
        if err == nil then
            remove_tmp_object(t.context.id)
        else
            -- In case of some problems with network
            -- send may not yield.
            fiber.sleep(0.1)
        end
    end
end

local function apply_config(cfg)
    if box.info.ro then
        if vars.resend_fiber ~= nil then
            log.info('input_processor: stopping resend fiber')
            task.stop(vars.resend_fiber)
        end
        log.info('input_processor: config ignored (because read only)')
        return
    end

    init()

    local candidates = cartridge.rpc_get_candidates('core',
        { healthy_only = false })
    if next(candidates) == nil then
        log.info('No remotes with role "core" available. Processing failure notifications disabled')
        vars.to_notify = false
    else
        vars.to_notify = true
    end

    if cfg.repair_queue == nil then
        vars.on_object_added = {}
    else
        vars.on_object_added = cfg.repair_queue.on_object_added or {}
    end

    if vars.resend_fiber == nil then
        log.info('input_processor: starting resend fiber')
        vars.resend_fiber = task.start(
            'input_processor.repair_queue.repair_queue',
            'resend_objects',
            { interval = RESEND_PERIOD_SEC })
    end

    log.info('input_processor.repair_queue: config applied')
end

local function restart_resend_fiber(period_sec)
    if vars.resend_fiber ~= nil then
        task.stop(vars.resend_fiber)
    end
    vars.resend_fiber = task.start(
        'input_processor.repair_queue.repair_queue',
        'resend_objects',
        { interval = period_sec })
end

return {
    start_processing = start_processing,
    success = success,
    failure = failure,
    apply_config = apply_config,

    resend_objects = resend_objects,

    -- for tests only
    restart_resend_fiber = restart_resend_fiber
}
