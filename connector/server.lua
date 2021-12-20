local module_name = 'connector.server'

local request_context = require('common.request_context')
local json = require('json')
local msgpack = require('msgpack')
local digest = require('digest')
local checks = require('checks')
local http_client = require('http.client')
local soap = require('common.soap')
local tracing = require('common.tracing')
local utils = require('common.utils')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local errors = require('errors')

local config = require('connector.config')
local soapserver = require('connector.soapserver')
local httpserver = require('connector.httpserver')
local kafka_client = require('connector.kafka_client')
local filewatcher = require('connector.filewatcher')
local tarantool_protocol_server = require('connector.tarantool_protocol_server')
local smtp = require('connector.smtp')
local connector_common = require('connector.common')

local cartridge = require('cartridge')
local cartridge_utils = require('cartridge.utils')
local config_filter = require('common.config_filter')
local membership = require('membership')

vars:new('http_client')
vars:new('routes')
vars:new('inputs')
vars:new('outputs')
vars:new('outputs_hash')

local config_error = errors.new_class('connector_config_error')
local connector_error = errors.new_class('connector_internal_error')
local routing_error = errors.new_class('connector_routing_failed')
local output_error = errors.new_class('connector_output_error')

local INPUTS = {
    http = httpserver,
    soap = soapserver,
    kafka = kafka_client,
    file = filewatcher,
    tarantool_protocol = tarantool_protocol_server,
}

for input_type, module in pairs(INPUTS) do
    connector_error:assert(type(module.setup) == 'function',
        'setup function must be defined for %s input', input_type)
    connector_error:assert(type(module.cleanup) == 'function',
        'cleanup function must be defined for %s input', input_type)
end

local function http_call(output, obj, options)
    options = options or {}

    local http_options = table.deepcopy(output.options) or {}
    if type(options.timeout) == 'number' then
        http_options.timeout = options.timeout
    end

    if output.format == 'json' then
        local headers = {
            ["Content-Type"] = 'application/json',
            ["request-id"] = request_context.get().id,
        }

        if output.headers ~= nil then
            for k, v in pairs(output.headers) do
                headers[k] = v
            end
        end

        if type(options.headers) == 'table' then
            local dynamic_headers = options.headers
            for k, v in pairs(dynamic_headers) do
                headers[k] = v
            end
        end
        tracing.inject_http_headers(headers)
        http_options.headers = headers
    else
        error(output_error:new('Unexpected output format: %q', output.format))
    end

    local span = tracing.start_span('connector.http_call')
    local res = vars.http_client:post(output.url, json.encode(obj), http_options)
    if not connector_common.is_success_status_code(res.status) then
        span:finish({ error = res.reason })
        log.error('%s: %s [%d]', output.url, res.reason, res.status)
        local err = output_error:new(res.reason)
        err.status = res.status
        err.body = res.body
        error(err)
    end
    span:finish()
    return true
end

local function soap_call(output, obj, options)
    local span = tracing.start_span('connector.soap_call')
    options = options or {}

    local http_options = {}

    local headers = {
        ["Content-Type"] = 'text/xml;charset=UTF-8',
        ["request-id"] = request_context.get().id,
    }

    if output.headers ~= nil then
        for k, v in pairs(output.headers) do
            headers[k] = v
        end
    end

    if type(options.headers) == 'table' then
        local dynamic_headers = options.headers
        options.headers = nil
        for k, v in pairs(dynamic_headers) do
            headers[k] = v
        end
    end
    tracing.inject_http_headers(headers)
    http_options.headers = headers

    local packet, err = soap.encode(obj)
    if err ~= nil then
        span:finish({ error = err })
        log.error('%s: %s', output.url, err)
        error(output_error:new(err))
    end
    local res = vars.http_client:post(output.url, packet, http_options)
    if res.status ~= 200 then
        span:finish({ error = res.reason })
        log.error('%s: %s [%d]', output.url, res.reason, res.status)
        local err = output_error:new(res.reason)
        err.status = res.status
        err.body = res.body
        error(err)
    end
    span:finish()
    return true
end

local function kafka_call(output, obj, options)
    local span = tracing.start_span('connector.kafka_call')

    local is_async = true
    if options ~= nil and options.is_async == false then
        is_async = false
    elseif output.is_async == false then
        is_async = false
    end
    local ok, err = kafka_client.produce(obj, output.name, is_async, output.format)
    if not ok then
        span:finish({ error = err })
        error(output_error:new(err))
    end

    span:finish()

    return true
end

local function smtp_call(output, obj, options)
    options = options or {}

    local span = tracing.start_span('connector.smtp_call')
    local smtp_output = smtp.get_output_by_name(output.name)
    if smtp_output ~= nil then
        local _, err = smtp.send_object(smtp_output, obj, options)
        if err ~= nil then
            span:finish({ error = err })
            error(err)
        else
            span:finish()
        end
    else
        local err = output_error:new('Sender for %s not found', output.name)
        span:finish({ error = "Sender not found" })
        error(err)
    end

    return true
end

local function make_options(opts)
    opts = opts or {}
    return {
        is_async = opts.is_async ~= false,
    }
end

local function handle_output_impl(output, data, opts)
    checks('table', 'table', '?table')

    if output.type == 'runner' then
        opts = make_options(opts)
        return cartridge.rpc_call('runner', 'handle_input_object',
            { data, opts }, { leader_only = true })
    elseif output.type == 'http' then
        return http_call(output, data.obj, data.output_options)
    elseif output.type == 'soap' then
        return soap_call(output, data.obj, data.output_options)
    elseif output.type == 'kafka' then
        return kafka_call(output, data.obj, data.output_options)
    elseif output.type == 'smtp' then
        return smtp_call(output, data.obj, data.output_options)
    elseif output.type == 'dummy' then
        -- do nothing
        return true
    else
        return nil, routing_error("Unknown output type %q", output.type)
    end
end

local function handle_output(output_name, data)
    checks('string', 'table')
    return handle_output_impl(vars.outputs[output_name], data)
end

local function handle_input_object(obj, routing_key)
    if routing_key ~= nil then
        return { routing_key = routing_key, obj = obj }
    end

    -- Default routing
    local first_key = next(obj)

    if type(first_key) ~= 'string' then
        return nil, connector_error:new("Default router expects first key to be a string")
    end

    if obj[first_key] == nil then
        return nil, connector_error:new("Output should contain an 'obj' payload")
    end

    return { routing_key = first_key, obj = obj[first_key] }
end

local function route(data)
    local output_name = vars.routes[data.routing_key]
    if output_name == nil then
        return {
            name = 'runner',
            type = 'runner',
        }
    end

    return vars.outputs[output_name]
end

local function handle_request(obj, routing_key, opts)
    checks('?', '?string', '?table')

    local data, err = handle_input_object(obj, routing_key)
    if data == nil then
        return nil, err
    end

    local output, err = route(data)
    if output == nil then
        return nil, err
    end

    local res, err = connector_error:pcall(handle_output_impl, output, data, opts)
    if res == nil then
        log.error(err)
    end

    return res, err
end

local function stop_outputs(outputs)
    for _, output in pairs(outputs) do
        if output.type == 'smtp' then
            smtp.remove_sender(output.name)
        elseif output.type == 'kafka' then
            local ok, err = kafka_client.remove_producer(output.name)
            if not ok then
                log.error('Failed to remove kafka producer for output %q: %s',
                    output.name, err)
            end
        end
    end
end

local function calculate_hash(data)
    local hash_rv = utils.to_array(data)
    return digest.md5_hex(msgpack.encode(hash_rv))
end

-- Checks whether connector should be started on current instance or not
local function is_connector_for_current_instance(input)
    -- By default connector
    if input.only == nil then
        return true
    end

    local addr_found = false
    local myself = membership.myself()
    for _, v in ipairs(input.only) do
        -- We suppose input.olny is valid array
        -- and each its element has either 'alias' or 'uri' property only
        if v.alias ~= nil and myself.payload.alias == v.alias
            or myself.uri == v.uri then
            addr_found = true
            break
        end
    end

    if addr_found == false then
        log.info('Skipping running connector %q on current instance', input.name)
    end

    return addr_found
end

-- assertions are used here solely for the sake of brevity
-- detailed error description is available
-- validated config should never cause an apply_config error
-- if it does - then it's a huge bug
local function apply_config(cfg)
    checks('table')

    vars.http_client = vars.http_client or http_client.new()
    vars.inputs = vars.inputs or {}

    local tc_cfg = {}
    if cfg['connector'] ~= box.NULL then
        tc_cfg = cfg['connector']
    end

    local _, in_err = config_filter.compare_and_set(tc_cfg, 'input', module_name)
    local _, out_err = config_filter.compare_and_set(tc_cfg, 'output', module_name)

    for _, module in pairs(INPUTS) do
        module.init()
    end

    -- Determine each input uniquely by its content
    if in_err == nil then
        local old_inputs = table.copy(vars.inputs) or {}
        local inputs = {}

        local cfg_input = {}
        if tc_cfg.input ~= box.NULL then
            cfg_input = tc_cfg.input
        end

        for _, input in pairs(cfg_input) do
            local input_hash = calculate_hash(input)

            old_inputs[input_hash] = nil
            inputs[input_hash] = input
        end

        -- Cleanup inputs which are not appear in new config
        for _, input in pairs(old_inputs) do
            local module = INPUTS[input.type]
            config_error:assert(module ~= nil, "unknown input type: %s", input.type)

            local ok, err = module.cleanup(input.name)
            if not ok then
                log.error('Failed cleanup "%s" input: %s', input.name, err)
            else
                log.info('Removed redundant "%s" input', input.name)
            end
        end

        -- Adding new inputs. Skip if already exists
        for hash, input in pairs(inputs) do
            config_error:assert(type(input.name) == 'string')

            local module = INPUTS[input.type]
            config_error:assert(module ~= nil, "unknown input type: %s", input.type)

            -- If connector is new and it should be run on current instance, start it
            if not vars.inputs[hash] and is_connector_for_current_instance(input) then
                local options = table.deepcopy(input)
                cartridge_utils.table_setrw(options)
                options.is_async = options.is_async ~= false
                local ok, err = module.setup(options)
                if not ok then
                    log.error('Failed setup %s input: %s', input.name, err)
                end
            end
        end

        vars.inputs = inputs
    end

    if out_err == nil then
        local old_outputs_hash = table.copy(vars.outputs_hash) or {}
        local new_outputs_hash = {}
        local new_outputs = {}

        local cfg_output = {}
        if tc_cfg.output ~= box.NULL then
            cfg_output = tc_cfg.output
        end

        for _, output in pairs(cfg_output) do
            config_error:assert(type(output.name) == 'string')

            local info
            if output.type == 'runner' then
                info = {
                    type = 'runner'
                }
            elseif output.type == 'http' then
                config_error:assert(type(output.url) == 'string')
                config_error:assert(output.format == 'json')

                local options = table.deepcopy(output.options)
                if options ~= nil then
                    cartridge_utils.table_setrw(options)
                end
                info = {
                    type = "http",
                    name = output.name,
                    url = output.url,
                    format = output.format,
                    headers = output.headers,
                    options = options,
                }
            elseif output.type == 'soap' then
                config_error:assert(type(output.url) == 'string')
                info = {
                    type = "soap",
                    name = output.name,
                    url = output.url,
                    headers = output.headers,
                }
            elseif output.type == 'kafka' then
                info = {
                    type = "kafka",
                    name = output.name,

                    topic = output.topic,
                    brokers = output.brokers,
                    options = output.options,
                    is_async = output.is_async,
                    format = output.format,
                }
            elseif output.type == 'smtp' then
                info = {
                    type = "smtp",
                    name = output.name,
                    url = output.url,
                }
            elseif output.type == 'dummy' then
                info = {
                    type = "dummy"
                }
            else
                error(config_error:new("unknown output type: %q", output.type))
            end

            local hash = calculate_hash(info)
            new_outputs[output.name] = info
            old_outputs_hash[hash] = nil
            new_outputs_hash[hash] = output
        end

        -- Cleanup removed sections.
        stop_outputs(old_outputs_hash)

        -- Initialize recently added.
        for hash, output in pairs(new_outputs_hash) do
            local is_initialized = vars.outputs_hash ~= nil and vars.outputs_hash[hash] ~= nil
            if not is_initialized then
                if output.type == 'smtp' then
                    smtp.create_sender(output)
                elseif output.type == 'kafka' then
                    local options = {}
                    if output.options ~= nil then
                        options = table.deepcopy(output.options)
                        cartridge_utils.table_setrw(options)
                    end

                    local opts = {
                        name = output.name,
                        topic = output.topic,
                        brokers = output.brokers,
                        options = options,
                        is_async = output.is_async,
                    }
                    local ok, err = kafka_client.add_producer(opts)
                    if not ok then
                        log.error('Failed to setup kafka producer for output %q: %s',
                            output.name, err)
                    end
                end
            end
        end
        vars.outputs = new_outputs
        vars.outputs_hash = new_outputs_hash
    end

    vars.routes = {}
    local cfg_routing = {}
    if tc_cfg.routing ~= box.NULL then
        cfg_routing = tc_cfg.routing
    end
    for _, route in pairs(cfg_routing) do
        config_error:assert(vars.outputs[route.output] ~= nil)
        vars.routes[route.key] = route.output
    end
end

local function stop()
    for _, module in pairs(INPUTS) do
        if type(module.stop) == 'function' then
            module.stop()
        end
    end
    stop_outputs(vars.outputs_hash)
    vars.outputs_hash = {}
    vars.outputs = {}
end

return {
    stop = stop,
    apply_config = apply_config,
    handle_request = handle_request,
    handle_output = handle_output,
    config = config,
}
