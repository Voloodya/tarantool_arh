local fio = require('fio')
local socket = require('socket')
local fiber = require('fiber')
local errors = require('errors')
local cartridge = require('cartridge')
local utils = require('common.utils')
local httpserver = require('connector.httpserver')
local config_error = errors.new_class('Invalid connector config')
local config_checks = require('common.config_checks').new(config_error)

local function count_sub_str(base, pattern)
    return select(2, string.gsub(base, pattern, ""))
end

local DEFAULT_INPUT_NAME = 'http'
local DEFAULT_INPUT_PATH = '/http'

local function get_service_by_port(port)
    local service_name = httpserver.get_httpd_service_name_for_port(port)
    return cartridge.service_get(service_name)
end

local function can_use_port(port)
    local service = get_service_by_port(port)
    if service ~= nil then
        return true
    end

    local sock = socket('AF_INET', 'SOCK_STREAM', 'tcp')
    sock:setsockopt('SOL_SOCKET', 'SO_REUSEADDR', 1)
    local ok = sock:bind('0.0.0.0', port)
    local err = sock:error()
    sock:close()
    if not ok then
        return false, err
    end
    return true
end

local function is_not_system_route(httpd, input)
    if httpd == nil then
        return true
    end
    for _, route in ipairs(httpd.routes) do
        -- route.name == nil -> it's a system route
        if route.path == input.path and route.name == nil then
            return false, string.format('attempt to redefine system route with input %q', input.name)
        end
    end
    return true
end

local function validate_http_input(field, input)
    config_checks:check_table_keys(field, input, {'name', 'type', 'routing_key', 'is_async', 'path', 'port', 'only'})
    config_checks:check_optional_luatype(field..'.routing_key', input.routing_key, 'string')
    config_checks:check_optional_luatype(field..'.path', input.path, 'string')
    local input_port_type = input.port ~= nil and type(input.port) or 'nil'
    config_checks:assert(utils.has_value({'nil', 'string', 'number'}, input_port_type),
        field..'.path must be string or number')

    if input.port ~= nil then
        local port = tonumber(input.port)
        config_checks:assert(port, field..'.port must be a valid number')
        config_checks:assert(1024 < port and port < 65535,
            'HTTP port for input %q expected to be in (1024; 65535)', input.name)
        local ok, err = can_use_port(port)
        config_checks:assert(ok, "Cannot bind port %q: %s", port, err)
    end
    if input.path ~= nil then
        config_checks:assert(input.path:startswith('/'), 'input %q path should starts with /', input.name)
        local httpd = get_service_by_port(input.port)
        config_checks:assert(is_not_system_route(httpd, input))
    end

    if input.name == DEFAULT_INPUT_NAME then
        config_checks:assert(input.path == nil or input.path == DEFAULT_INPUT_PATH,
            'cannot change path %q for default %q input', DEFAULT_INPUT_PATH, DEFAULT_INPUT_NAME)

        if input.port ~= nil then
            local default_port = cartridge.service_get('httpd').port
            config_checks:assert(input.port == nil or tostring(input.port) == cartridge.service_get('httpd').port,
            'cannot change port %q for default %q input', default_port, DEFAULT_INPUT_NAME)
        end
    end

end

local function validate_tarantool_protocol_input(field, input)
    config_checks:check_table_keys(field, input, {'name', 'type', 'routing_key', 'is_async', 'only'})
    config_checks:check_optional_luatype(field..'.routing_key', input.routing_key, 'string')
end

local function validate_soap_input(field, input)
    config_checks:check_luatype(field..'.wsdl', input.wsdl, 'string')
    config_checks:check_luatype(field..'.handlers', input.handlers, 'table')

    config_checks:check_optional_luatype(field..'.success_response_body', input.success_response_body, 'string')
    if input.error_response_body then
        config_checks:check_luatype(field..'.error_response_body', input.error_response_body, 'string')
        config_checks:assert(count_sub_str(input.error_response_body, "%%s") < 2,
                'too many "%s" in SOAP error response body template')
    end

    local seen_functions = {}
    for k, handler in pairs(input.handlers) do
        local handler_field = string.format('%s.handlers[%s]', field, k)
        config_checks:check_table_keys(handler_field, handler, {'function', 'routing_key'})

        config_checks:check_luatype(handler_field..'.function', handler['function'], 'string')
        config_checks:check_optional_luatype(handler_field..'.routing_key', handler['routing_key'], 'string')

        config_checks:assert(not seen_functions[handler['function']],
            '%s has duplicate handlers with function %q', field, handler['function'])
        seen_functions[handler['function']] = true
    end

    config_checks:check_table_keys(field, input,
            {'name', 'type', 'wsdl', 'success_response_body', 'error_response_body', 'handlers', 'is_async', 'only'})
end

local function validate_kafka_input(field, input)
    config_checks:check_luatype(field..'.brokers', input.brokers, 'table')
    config_checks:assert(#input.brokers > 0, '%s.brokers must be non empty table', field)
    config_checks:check_luatype(field..'.topics', input.topics, 'table')
    config_checks:assert(#input.topics > 0, '%s.topics must be non empty table', field)
    config_checks:check_luatype(field..'.group_id', input.group_id, 'string')
    config_checks:check_optional_luatype(field..'.routing_key', input.routing_key, 'string')
    config_checks:check_optional_luatype(field..'.workers_count', input.workers_count, 'number')
    if input.token_name ~= nil then
        config_checks:check_luatype(field..'.token_name', input.token_name, 'string')
    end
    if input.options ~= nil then
        config_checks:check_luatype(field..'.options', input.options, 'table')
    end
end

local ALLOWED_FILE_FORMATS = {
    csv = true,
    jsonl = true,
}

local function validate_only_attribute(field, input)
    if input.only == nil then
        return true
    end
    -- If input.only is defined it should be array-like table
    config_checks:assert(utils.is_array(input.only, true), '%s.only must be non-empty array', field)

    -- Each 'only' element should either has only one of 'alias' or 'uri' field
    -- Any of them should be string
    for i, v in ipairs(input.only) do
        local field_name = string.format('%s.only[%d]', field, i)
        config_checks:check_luatype(field_name, v, 'table')
        config_checks:check_table_keys(field_name, v, {'alias', 'uri'})
        config_checks:check_optional_luatype(field_name .. '.alias', v.alias, 'string')
        config_checks:check_optional_luatype(field_name .. '.uri', v.uri, 'string')
        config_checks:assert(
            (v.alias ~= nil and v.uri == nil) or
                (v.alias == nil and v.uri ~= nil),
            'Only one of %s.uri of %s.alias fields must be defined', field_name, field_name
        )
    end
end

local function validate_file_input(field, input)
    config_checks:check_luatype(field..'.format', input.format, 'string')
    config_checks:assert(ALLOWED_FILE_FORMATS[input.format],
        '%s.format "%s" not supported', field, input.format)
    config_checks:check_luatype(field..'.filename', input.filename, 'string')
    config_checks:check_table_keys(field, input,
        {'name', 'type', 'filename', 'format', 'routing_key', 'workdir', 'is_async', 'token_name', 'only'})
    config_checks:check_optional_luatype(field..'.routing_key', input.routing_key, 'string')
    config_checks:check_optional_luatype(field..'.workdir', input.workdir, 'string')
    if input.token_name ~= nil then
        config_checks:check_luatype(field..'.token_name', input.token_name, 'string')
    end

    if input.workdir then
        local workdir = fio.abspath(input.workdir)
        config_checks:assert(fio.path.is_dir(workdir), 'workdir "%s" must exist', workdir)

        local filename = ('.tdg.%d'):format(tonumber(fiber.time64()))
        local absname = fio.pathjoin(workdir, filename)
        local fh, err = fio.open(absname, {'O_WRONLY', 'O_CREAT', 'O_APPEND'}, tonumber('0644', 8))
        config_checks:assert(fh ~= nil,
            'must have rights to create files in "%s": %s', workdir, err)

        fh:close()
        fio.unlink(absname)
    end
end

local function validate_input(field, input)
    config_checks:check_luatype(field..'.name', input.name, 'string')
    config_checks:assert(#input.name:strip() > 0, '%s.name expected to be non-empty', field)
    config_checks:check_luatype(field..'.type', input.type, 'string')
    config_checks:check_optional_luatype(field..'.is_async', input.is_async, 'boolean')

    validate_only_attribute(field, input)

    if input.type == 'soap' then
        validate_soap_input(field, input)
    elseif input.type == 'http' then
        validate_http_input(field, input)
    elseif input.type == 'tarantool_protocol' then
        validate_tarantool_protocol_input(field, input)
    elseif input.type == 'kafka' then
        validate_kafka_input(field, input)
    elseif input.type == 'file' then
        validate_file_input(field, input)
    else
        config_checks:assert(false, '%s has unknown type %q', field, input.type)
    end
end

local function validate_http_input_paths(inputs)
    local visited_paths = {}
    local httpd = cartridge.service_get('httpd')
    local default_port = httpd.port
    for _, input in ipairs(inputs) do
        local path = input.path == box.NULL and DEFAULT_INPUT_PATH or input.path
        local port = input.port == box.NULL and default_port or tostring(input.port)
        visited_paths[port] = visited_paths[port] or {}
        config_checks:assert(visited_paths[port][path] == nil, 'duplicate HTTP input paths %q', path)
        visited_paths[port][path] = true
    end
end

local function nop(_)
    return true
end

local input_validators = {
    ['http'] = validate_http_input_paths,
    ['file'] = nop,
    ['kafka'] = nop,
}

local function validate_config(cfg)
    local connector_cfg = cfg['connector'] or {}
    config_checks:check_luatype('input', connector_cfg.input, 'table')

    local seen_inputs = {}
    local seen_input_names = {}
    for k, input in pairs(connector_cfg.input) do
        local field = string.format('input[%s]', k)

        config_checks:check_luatype(field, input, 'table')
        validate_input(field, input)

        seen_inputs[input.type] = seen_inputs[input.type] or {}
        config_checks:assert(seen_input_names[input.name] == nil,
            'There are two or more connectors with name %q', input.name)
        seen_input_names[input.name] = true
        table.insert(seen_inputs[input.type], input)
    end

    for input_type, inputs in pairs(seen_inputs) do
        local validator = input_validators[input_type]
        if validator ~= nil then
            validator(inputs)
        else
            config_checks:assert(#inputs <= 1, 'duplicate inputs of type %q', input_type)
        end
    end

    return true
end

return {
    validate = validate_config,
}
