local module_name = 'storage.output_replication_queue'

local fiber = require('fiber')
local uuid = require('uuid')
local clock = require('clock')
local json = require('json')
local cartridge = require('cartridge')
local errors = require('errors')
local checks = require('checks')

local request_context = require('common.request_context')
local replicable = require('common.replicable')
local task = require('common.task')
local tenant = require('common.tenant')
local defaults = require('common.defaults')
local model_accessor = require('common.model_accessor')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)

--  {
--      is_async = <true|false>,
--      task_id = <task_id>,
--      new_object_cond = <fiber.cond>,
--      store_strategy = <REF|COPY>,
--      expected_outputs = expected_outputs,
--      sync_retry_timeout = sync_retry_timeout,
--      sync_failed_attempt_count_threshold = sync_failed_attempt_count_threshold,
--      expiration_timeout = expiration_timeout,
--  }
vars:new('outputs')
vars:new('SEND_AGAIN_TIMEOUT_SEC', 60)

local RECORD_INFO_TYPES = {
    REF = 1,
    COPY = 2,
}

local BASE_SPACE_NAME = 'tdg_output'

local function get_space_name(name)
    return tenant.get_space_name(BASE_SPACE_NAME) .. '_' .. name
end

local function get_output_processor_space(name)
    local space_name = get_space_name(name)
    return box.space[space_name]
end

local function replicate_object(type_name, tuples, context)
    if tuples[1] == nil then
        return
    end

    context = context or {}
    local routing_key = context.routing_key
    if routing_key == nil then
        routing_key = type_name
    end

    if replicable.is_replicable(routing_key) then
        local output = vars.outputs[routing_key]
        local store_strategy = output.store_strategy
        local expected_outputs = output.expected_outputs

        local info
        if store_strategy == RECORD_INFO_TYPES.REF then
            info = model_accessor.get_replication_info(type_name, tuples)
        elseif store_strategy == RECORD_INFO_TYPES.COPY then
            info = tuples
        end

        local bucket_id_fieldno, err = model_accessor.get_bucket_id_fieldno(type_name)
        if err ~= nil then
            log.error('Impossible to get bucket_id field number for %q: %s', type_name, err)
            return
        end

        local old_context = request_context.get()
        local old_context_id = old_context.id

        log.info('From request with id = %q output processing was started', old_context_id)
        local space = get_output_processor_space(routing_key)

        box.begin()
        for i, key in ipairs(info) do
            old_context.id = uuid.str()
            local time_added = clock.time64()
            local ok, err = pcall(space.replace, space,
                {
                    nil,
                    type_name,
                    key,
                    old_context,
                    store_strategy,
                    expected_outputs,
                    time_added,
                    box.NULL,
                    0,
                    tuples[i][bucket_id_fieldno],
                })
            if not ok then
                old_context.id = old_context_id
                log.error('Error during object %q replication: %s', json.encode(key), err)
                box.rollback()
                error(err)
            end
        end
        box.commit()

        old_context.id = old_context_id

        output.new_object_cond:signal()
    end
end

local function send_tuple(routing_key, worker_ctx, space, replication_info, options)
    checks('string', 'table', 'table', 'tuple', '?table')
    local index, type_name, key, context, store_strategy, outputs = replication_info:unpack(1, 6)

    local tuple
    if store_strategy == RECORD_INFO_TYPES.REF then
        tuple = model_accessor.get_tuple_by_pk(type_name, key)
    else
        tuple = key
    end

    if tuple ~= nil then
        local is_async = options.is_async ~= false

        request_context.set(context)
        local outputs_rest, err = cartridge.rpc_call('runner', 'handle_output_object',
            {type_name, routing_key, tuple, outputs, is_async},
            {leader_only = true})
        request_context.set(worker_ctx)
        if err ~= nil then
            if is_async == false then
                local new_failed_attempt_count = replication_info.failed_attempt_count + 1
                log.error('Error during output object processing (attempt %d): %s',
                    new_failed_attempt_count, json.encode(err))

                if options.sync_failed_attempt_count_threshold ~= nil and
                    (new_failed_attempt_count >= options.sync_failed_attempt_count_threshold) then
                    log.warn('Output processing of object %q is stopped. ' ..
                        'Sync_failed_attempt_count_threshold (%d) is exceeded',
                        context.id, options.sync_failed_attempt_count_threshold)
                    space:delete(index)
                else
                    local update_list = {
                        {'=', 'time_updated', clock.time64()},
                        {'=', 'failed_attempt_count', new_failed_attempt_count},
                    }
                    if outputs_rest ~= nil then
                        table.insert(update_list, {'=', 'expected_outputs', outputs_rest})
                    end
                    space:update({index}, update_list)
                    return true
                end
            else
                err = errors.wrap(err)
                log.error('Error during output object processing: %s', err)
            end
        else
            space:delete({index})
        end
    else
        log.error('Object %q in route %q not found by output processor', type_name, routing_key)
        space:delete({index})
    end
end

local function is_expired(tuple, timeout)
    return clock.time64() > (tuple.time_added + timeout * 1e9)
end

local function object_sender(routing_key)
    checks('string')
    local ctx = request_context.get()
    local space = get_output_processor_space(routing_key)
    local options = vars.outputs[routing_key]

    while true do
        local space_scanned = true
        local count = 0
        for _, tuple in space:pairs() do
            -- Protects us from cases when is_async = true
            -- and rpc_call doesn't yield for some reasons
            -- (e.g. no "runner" role enabled).
            count = count + 1
            if count % defaults.FORCE_YIELD_LIMIT == 0 then
                count = 0
                fiber.sleep(0.1)
            end
            if options.expiration_timeout ~= nil and is_expired(tuple, options.expiration_timeout) then
                log.warn('Output processing of object %q is stopped. Expiration_timeout is exceeded', tuple.context.id)
                space:delete(tuple.id)
            else
                local need_retry = send_tuple(routing_key, ctx, space, tuple, options)
                if need_retry == true then
                    space_scanned = false
                    local timeout = options.sync_retry_timeout
                    if timeout == nil then
                        timeout = vars.SEND_AGAIN_TIMEOUT_SEC
                    end
                    log.error('Retry for output queue of routing key %q will be done after %ss',
                        routing_key, timeout)
                    fiber.sleep(timeout)
                    break
                end
            end
        end

        if space_scanned == true then
            options.new_object_cond:wait(vars.SEND_AGAIN_TIMEOUT_SEC)
        end
    end
end

local function stop()
    local outputs = vars.outputs
    if outputs == nil then
        return
    end

    for _, output in pairs(outputs) do
        task.stop(output.task_id)
    end

    vars.outputs = nil

    log.info('Module %s uninitialized', module_name)
end

local function create_output_queue(name)
    checks('string')
    local space_name = get_space_name(name)

    local space = box.space[space_name]
    if space ~= nil then
        return space
    end

    space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format({
        { name = 'id', type = 'unsigned' },
        { name = 'type_name', type = 'string' },
        { name = 'info', type = 'array' },
        { name = 'context', type = 'map' },
        { name = 'info_type', type = 'unsigned' },
        { name = 'expected_outputs', type = 'any', is_nullable = true },
        { name = 'time_added', type = 'unsigned' },
        { name = 'time_updated', type = 'unsigned', is_nullable = true },
        { name = 'failed_attempt_count', type = 'unsigned' },
        { name = 'bucket_id', type = 'unsigned' },
    })

    space:create_index('id', {
        parts = {{field = 'id', type = 'unsigned'}},
        sequence = true,
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })

    space:create_index('bucket_id', {
        parts = {{field = 'bucket_id', type = 'unsigned'}},
        type = 'TREE',
        unique = false,
        if_not_exists = true,
    })

    return space
end

local function drop_output_queue(name)
    local space_name = get_space_name(name)
    local space = box.space[space_name]

    if space == nil then
        return
    end

    if space:len() > 0 then
        log.info('Output %q is removed from config but space %q is not empty. Drop it manually', name, space.name)
        return
    end

    box.space[space_name]:drop()
end

local function apply_config(cfg)
    if box.info.ro then
        stop()
        return
    end

    replicable.apply_config(cfg)

    vars.outputs = vars.outputs or {}

    local outputs_map = {}
    for name, options in pairs(cfg.output_processor or {}) do
        outputs_map[name] = true

        local expected_outputs = {}
        for _, handler in ipairs(options.handlers) do
            local fn_name = handler['function']
            expected_outputs[fn_name] = expected_outputs[fn_name] or {}
            for _, output in ipairs(handler.outputs) do
                assert(type(output) == 'string')
                expected_outputs[fn_name][output] = true
            end
        end

        if vars.outputs[name] == nil then
            box.atomic(create_output_queue, name)
            vars.outputs[name] = {
                new_object_cond = fiber.cond(),
            }
        end

        vars.outputs[name].is_async = options.is_async ~= false
        vars.outputs[name].sync_retry_timeout = options.sync_retry_timeout
        vars.outputs[name].sync_failed_attempt_count_threshold = options.sync_failed_attempt_count_threshold
        vars.outputs[name].store_strategy = options.store_strategy == 'copy'
            and RECORD_INFO_TYPES.COPY or RECORD_INFO_TYPES.REF
        vars.outputs[name].expected_outputs = expected_outputs
        vars.outputs[name].expiration_timeout = options.expiration_timeout
    end

    for name, options in pairs(vars.outputs) do
        if outputs_map[name] == nil then
            task.stop(options.task_id)
            vars.outputs[name] = nil
            drop_output_queue(name)
        elseif vars.outputs[name].task_id == nil then
            local task_id = task.start('storage.output_replication_queue',
                'object_sender',
                { interval = 0 },
                name
            )
            vars.outputs[name].task_id = task_id
        end
    end
end

return {
    apply_config = apply_config,
    object_sender = object_sender,
    replicate_object = replicate_object,
}
