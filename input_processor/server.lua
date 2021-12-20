local module_name = 'input_processor.server'

local repair = require('input_processor.repair_queue.repair_queue')
local json = require('json')
local checks = require('checks')

local request_context = require('common.request_context')
local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local tenant = require('common.tenant')
local errors = require('errors')
local graphql = require('common.graphql')
local sandbox_registry = require('common.sandbox.registry')
local model_graphql = require('common.model_graphql')
local model_rest = require('input_processor.model_rest')
local model_iproto = require('input_processor.model_iproto')
local services = require('input_processor.services')
local tracing = require('common.tracing')
local worker_pool = require('common.worker_pool')

vars:new('handlers')
vars:new('default_handler')
vars:new('storage')
vars:new('output')
vars:new('runner_worker_count', 50)
vars:new('worker_pool')

local input_processor_error = errors.new_class('input_processor internal error')
local routing_error = errors.new_class('input_processor routing failed')
local sandbox_call_error = errors.new_class('sandbox call error', {capture_stack = false})

local function get_type_name(routing_key)
    if vars.storage[routing_key] == nil then
        return routing_key
    else
        return vars.storage[routing_key].type
    end
end

local function make_options(req)
    local opts = {
        first = req.first,
        after = req.after,
        version = request_context.get().version,
    }
    return opts
end

local function make_context(req)
    local context = {
        routing_key = req.routing_key,
    }
    return context
end

local function store(req)
    local type_name = get_type_name(req.routing_key)

    local repository = tenant.get_repository()
    local res, err = repository:put(type_name, req.obj,
        make_options(req), make_context(req))

    if err ~= nil then
        return nil, err
    end

    return res[1]  -- put returns array
end

local function route(req)
    local handler = vars.handlers[req.routing_key] or vars.default_handler

    if handler ~= nil then
        local span = tracing.start_span('sandbox.call_by_name: %s', handler)
        local sandbox = vars.sandbox
        local res, err = sandbox_call_error:pcall(sandbox.call_by_name, sandbox, handler, req)
        span:finish({error = err})
        if not res then
            return nil, err
        elseif res == true then
            return true
        end
        req = res
    end

    return store(req)
end

local function handle_input_object_sync(req)
    checks('table')
    local routing_key = req.routing_key

    if routing_key == nil then
        local err = routing_error:new("Routing key must be set by connector")
        return nil, err
    end

    local res, err = input_processor_error:pcall(route, req)
    if not res then
        return nil, err
    end

    return res
end

local function process_object_async(req, context)
    request_context.set(context)
    local _, err = input_processor_error:pcall(handle_input_object_sync, req)

    if err ~= nil then
        local _, err = repair.failure(req.routing_key, err)
        if err ~= nil then
            log.error('Сan\'t add an object to the repair queue: %s', err)
        end
    else
        local _, err = repair.success()
        if err ~= nil then
            log.error('Сan\'t remove an object from repair queue: %s', err)
        end
    end
    request_context.clear()
end

local function handle_input_object(req, opts)
    local span = tracing.start_span('runner.handle_input_object')
    if opts.is_async == true then
        repair.start_processing(req)

        local ok = vars.worker_pool:process(req, request_context.get())
        if not ok then
            local err = input_processor_error:new('Failed to process request')
            span:finish({error = err})
            return nil, err
        end

        span:finish()
        return 'OK'
    else
        local res, err = input_processor_error:pcall(handle_input_object_sync, req)
        span:finish({error = err})
        return res, err
    end
end

local function add_handler(handler_cfg)
    checks('table')
    if handler_cfg.key == '*' then
        vars.default_handler = handler_cfg['function']
    else
        vars.handlers[handler_cfg.key] = handler_cfg['function']
    end
end

local function add_storage(storage_cfg)
    checks('table')
    vars.storage[storage_cfg.key] = {type=storage_cfg.type}
end

local function add_output(output_cfg)
    checks('table')
    vars.output[output_cfg.type] = {name=output_cfg.name, ['function']=output_cfg['function']}
end

local function init_handlers(cfg)
    checks('table')
    local handlers = cfg['handlers'] or {}

    for _, v in pairs(handlers) do
        add_handler(v)
    end

    log.info('handlers: ' .. json.encode(vars.handlers))
end


local function init_storage(cfg)
    checks('table')

    local storage = cfg['storage']

    if storage == nil then
        return
    end

    for _, v in pairs(storage) do
        add_storage(v)
    end

    log.info('storage: ' .. json.encode(vars.storage))
end

local function init_output(cfg)
    checks('table')

    local output = cfg['output']

    if output == nil then
        return
    end

    for _, v in pairs(output) do
        add_output(v)
    end

    log.info('output: ' .. json.encode(vars.output))
end


local function init_types(cfg)
    checks('table')

    local mdl = tenant.get_mdl()

    local graphql_model, err = model_graphql.model_to_graphql(mdl)
    if graphql_model == nil then
        error(err)
    end

    local tenant_uid = tenant.uid()
    graphql.set_shared_types(mdl, cfg.shared_types, graphql_model.query_fields, graphql_model.mutation_fields)
    graphql.set_model(tenant_uid, graphql_model.query_fields, graphql_model.mutation_fields)

    local res, err = services.apply_config(mdl, graphql_model.type_map, cfg.services)
    if res == nil then
        error(err)
    end

    model_rest.init()
    model_rest.apply_config(mdl)
    model_iproto.init()
end


local function apply_config(cfg)
    checks('table')

    vars.default_handler = nil
    vars.handlers = {}
    vars.storage = {}
    vars.output = {}
    vars.sandbox = sandbox_registry.get('active')

    repair.apply_config(cfg)

    init_types(cfg)

    local input_processor_cfg = cfg['input_processor'] or {}
    init_handlers(input_processor_cfg)
    init_storage(input_processor_cfg)
    init_output(input_processor_cfg)

    if vars.worker_pool == nil then
        vars.worker_pool = worker_pool.new(vars.runner_worker_count,
            process_object_async,{ name = 'runner_worker', silent = true })
    end
end

return {
    apply_config = apply_config,
    handle_input_object = handle_input_object,
}
