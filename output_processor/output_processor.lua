local module_name = 'output_processor'

local fiber = require('fiber')
local json = require('json')
local tracing = require('common.tracing')

local log = require('log.log').new(module_name)

local cartridge = require('cartridge')

local output_processor_list = require('output_processor.output_processor_list')
local statuses = require('output_processor.statuses')

local errors = require('errors')
local tenant = require('common.tenant')
local model_flatten = require('common.model_flatten')
local replicable = require('common.replicable')
local request_context = require('common.request_context')
local sandbox_registry = require('common.sandbox.registry')

local vars = require('common.vars').new(module_name)

local output_processor_error = errors.new_class('output_processor failed')

vars:new('postprocess_all_fiber')
vars:new('sandbox')
vars:new_global('TIMEOUT_SEC', 5 * 60)

local function apply_config(cfg)
    replicable.apply_config(cfg)
    vars.sandbox = sandbox_registry.get('active')
end

local function send(to_send, error_list, context)
    local span = tracing.start_span('output_processor.send')
    local channel = fiber.channel()

    local count = 0
    for id, props in pairs(to_send) do
        if props.data ~= nil then
            count = count + 1

            fiber.create(function()
                request_context.init(context)
                local _, err = cartridge.rpc_call('connector', 'handle_output',
                    { props.output, props.data },
                    { leader_only = true, timeout = vars.TIMEOUT_SEC }
                )
                if err ~= nil then
                    table.insert(error_list, err)
                    to_send[id].error = { status_code = err.status, response_body = err.body }
                else
                    to_send[id] = nil
                end

                props.data = nil

                channel:put(true)
            end)
        end
    end

    for _ = 1, count do
        local res = channel:get(vars.TIMEOUT_SEC)
        if res == nil then
            table.insert(error_list, ('[%s] Timeout while waiting for data to be send'):format(context.id))
        end
    end
    if error_list and #error_list > 0 then
        span:set_error(json.encode(error_list))
    end
    span:finish()
    return to_send, error_list
end

local function process(object, name)
    local span = tracing.start_span('output_processor.process')
    local processed_object, err = vars.sandbox:call_by_name(name, object)
    span:finish({ error = err })
    if err ~= nil then
        return nil, err
    end
    return processed_object
end

local function unflat_object(type_name, tuple)
    if type_name == nil then
        return tuple
    end

    local serializer = tenant.get_serializer()

    local span = tracing.start_span('output_processor.unflat_object')
    local raw_object, err = model_flatten.unflatten_record(tuple, serializer, type_name)
    if err ~= nil then
        span:finish({ error = err })
        return nil, err
    end
    span:finish()

    return raw_object
end

local function get_reason(error_list)
    local reason = ''
    for _, err in ipairs(error_list) do
        reason = reason .. '\n' .. tostring(err)
    end
    return reason
end

local function get_outputs(routing_key)
    local options = replicable.get_properties(routing_key)
    if options == nil then
        return nil, output_processor_error:new("Can't find options for %q", routing_key)
    end

    local outputs = {}
    for _, handler in ipairs(options.handlers) do
        local fn_name = handler['function']
        outputs[fn_name] = outputs[fn_name] or {}
        for _, output in ipairs(handler.outputs) do
            outputs[fn_name][output] = true
        end
    end
    return outputs
end

local function handle_output_object(type_name, routing_key, tuple, outputs, is_async)
    log.info('Start to postprocess an object with routing_key = %q', routing_key)

    -- type_name == nil is as case when we send notification about failure
    -- when process an input object on runner.
    local object, err = output_processor_error:pcall(unflat_object, type_name, tuple)
    if err ~= nil then
        return nil, err
    end

    if type_name == nil then
        outputs, err = get_outputs(routing_key)
        if err ~= nil then
            return nil, err
        end

        is_async = false
    end

    local context = request_context.get()
    context.routing_key = routing_key

    local to_send = {}
    local error_list = {}
    local has_processing_error = false

    for func, prop in pairs(outputs) do
        repeat
            local processed_object, err = process(table.deepcopy(object), func)
            if processed_object == nil then
                has_processing_error = true
                table.insert(error_list, err)
            elseif processed_object.obj == nil then
                table.insert(error_list, "Output should contain an 'obj' payload")
            elseif processed_object.skip == true then
                log.info('Function %q said to skip object', func)
                break
            end

            for out in pairs(prop) do
                to_send[func .. out] = {
                    data = processed_object,
                    ['function'] = func,
                    output = out,
                }
            end
        until true
    end

    to_send, error_list = send(to_send, error_list, context)
    if #error_list == 0 then
        log.info('Object has been postprocessed')
        return
    end

    if is_async == true then
        local to_save = {
            type_name = type_name,
            tuple = tuple,
            to_send = to_send,
        }

        local reason = get_reason(error_list)
        local status = statuses.SENDING_ERROR
        if has_processing_error then
            status = statuses.PREPROCESSING_ERROR
        end

        local _, err = output_processor_list.add(to_save, status, reason, context)
        if err ~= nil then
            return nil, err
        end
    else
        local to_send_rest = {}
        for _, info in pairs(to_send) do
            local fun = info['function']
            local output = info['output']

            to_send_rest[fun] = to_send_rest[fun] or {}
            to_send_rest[fun][output] = true
        end
        return to_send_rest, error_list
    end
end

local function postprocess_again(id)
    log.info('Retrying postprocess %q', id)

    local data, err = output_processor_list.get(id)
    if data == nil then
        if err ~= nil then
            return nil, err
        else
            return nil, output_processor_error:new('Invalid object id: %q', id)
        end
    end

    local type_name = data.object.type_name
    local tuple = data.object.tuple
    local context = data.context
    local routing_key = context.routing_key

    local object, err = unflat_object(type_name, tuple)
    if err ~= nil then
        return nil, err
    end

    local properties = replicable.get_properties(routing_key)
    if properties == nil then
        return nil, output_processor_error:new("Can't find properties for %q", routing_key)
    end

    local to_send = data.object.to_send
    local error_list = {}
    local has_processing_error = false

    local processed_objects = {}

    for _, props in pairs(to_send) do
        local func = props['function']
        local processed = processed_objects[func]
        if processed == nil then
            local err
            processed, err = process(table.deepcopy(object), func)
            if processed == nil then
                has_processing_error = true
                table.insert(error_list, err)
            else
                processed_objects[func] = false
            end
        end

        if processed then
            props.data = processed
        end
    end

    to_send, error_list = send(to_send, error_list, context)

    if #error_list == 0 then
        output_processor_list.success(context.id)
        log.info('Object has been postprocessed')
        return true
    end

    local to_save = {
        type_name = type_name,
        tuple = tuple,
        to_send = to_send
    }

    local reason = get_reason(error_list)

    local status = statuses.REPOSTPROCESSED_SENDING_ERROR
    if has_processing_error then
        status = statuses.REPOSTPROCESSED_PREPROCESSING_ERROR
    end

    output_processor_list.processing_error(context.id, to_save, status, reason)

    return true
end

local function postprocess_again_all_impl()
    log.info('Retrying postprocess for all')

    local cursor

    local ctx = request_context.get()
    while true do
        local objects, err = output_processor_list.filter(nil, nil, nil, nil, cursor)
        if err ~= nil then
            log.error(err)
            return
        end

        if #objects == 0 then
            return
        end

        for _, obj in ipairs(objects or {}) do
            request_context.set(obj.context)
            local _, err = postprocess_again(obj.id)
            request_context.set(ctx)

            if err then
                log.error(err)
            end

            cursor = obj.cursor
        end
    end
end

local function postprocess_again_all()
    if vars.postprocess_all_fiber == nil or vars.postprocess_all_fiber:status() == 'dead' then
        vars.postprocess_all_fiber = tenant.fiber_new(postprocess_again_all_impl)
    end
    return true
end

return {
    apply_config = apply_config,
    handle_output_object = handle_output_object,
    postprocess_again = postprocess_again,
    postprocess_again_all = postprocess_again_all
}
