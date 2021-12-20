local errors = require('errors')
local config_error = errors.new_class('Invalid connector.output config')
local config_checks = require('common.config_checks').new(config_error)

local function output_exists(cfg, output)
    if type(cfg) ~= 'table' then
        return false
    end

    local tc_cfg = cfg['connector']
    if type(tc_cfg) ~= 'table' then
        return false
    end

    if type(tc_cfg.output) ~= 'table' then
        return false
    end

    for _, current in pairs(tc_cfg.output) do
        if current.name == output then
            return true
        end
    end
end

local function validate_runner_output(field, output)
    config_checks:check_table_keys(field, output, {'name', 'type'})
end

local function validate_dummy_output(field, output)
    config_checks:check_table_keys(field, output, {'name', 'type'})
end

local available_http_client_options = {
    ca_file = 'string',
    ca_path = 'string',
    keepalive_idle = 'number',
    keepalive_interval = 'number',
    low_speed_limit = 'number',
    low_speed_time = 'number',
    max_header_name_len = 'number',
    follow_location = 'boolean',
    no_proxy = 'string',
    proxy = 'string',
    proxy_port = 'number',
    proxy_user_pwd = 'string',
    ssl_cert = 'string',
    ssl_key = 'string',
    timeout = 'number',
    unix_socket = 'string',
    verbose = 'boolean',
    verify_host = 'boolean',
    verify_peer = 'boolean',
    accept_encoding = 'string',
}

local function validate_http_output(field, output)
    config_checks:check_luatype(field..'.url', output.url, 'string')
    config_checks:check_luatype(field..'.format', output.format, 'string')
    if output.headers ~= nil then
        config_checks:assert(type(output.headers) == 'table', field..'.headers should be a table')
    end
    if output.options ~= nil then
        config_checks:assert(type(output.options) == 'table', field..'.options should be a table')
        for option, option_type in pairs(available_http_client_options) do
            config_checks:check_optional_luatype(field..'.options.'..option, output.options[option], option_type)
        end
        for key in pairs(output.options) do
            config_checks:assert(available_http_client_options[key] ~= nil,
                'Unexpected http client option: %s.options.%s', field, key)
        end
    end
    config_checks:assert(output.format == 'json',
        '%s: %q output format is not yet supported, use "json" instead', field, output.format)
    config_checks:check_table_keys(field, output, {'name', 'type', 'url', 'format', 'headers', 'options'})
end

local function validate_soap_output(field, output)
    config_checks:check_luatype(field..'.url', output.url, 'string')
    if output.headers ~= nil then
        config_checks:assert(type(output.headers) == 'table', field..'.headers should be a table')
    end
    config_checks:check_table_keys(field, output, {'name', 'type', 'url', 'headers'})
end

local function validate_kafka_output(field, output)
    config_checks:check_luatype(field..'.brokers', output.brokers, 'table')
    config_checks:assert(#output.brokers > 0, '%s.brokers must not be empty', field)
    config_checks:check_luatype(field..'.topic', output.topic, 'string')
    config_checks:check_optional_luatype(field..'.is_async', output.is_async, 'boolean')
    config_checks:check_optional_luatype(field..'.format', output.format, 'string')
    if output.format ~= nil then
        config_checks:assert(output.format == 'json' or output.format == 'plain', '%s.format must be json/plain', field)
    end
    if output.options ~= nil then
        config_checks:check_luatype(field..'.options', output.options, 'table')
    end
end

local function validate_smtp_output(field, output)
    config_checks:check_luatype(field..'.url', output.url, 'string')
    if output.timeout ~= nil then
        config_checks:check_luatype(field..'.timeout', output.timeout, 'number')
    end
    if output.from ~= nil then
        config_checks:check_luatype(field..'.from', output.from, 'string')
    end
    if output.subject ~= nil then
        config_checks:check_luatype(field..'.subject', output.subject, 'string')
    end
    if output.ssl_cert ~= nil then
        config_checks:check_luatype(field..'.ssl_cert', output.ssl_cert, 'string')
    end
    if output.ssl_key ~= nil then
        config_checks:check_luatype(field..'.ssl_key', output.ssl_key, 'string')
    end
    config_checks:check_table_keys(field, output, {
        'name', 'type', 'url', 'from', 'subject', 'timeout', 'ssl_cert', 'ssl_key'
    })
end

local function validate_output(field, output)
    config_checks:check_luatype(field..'.name', output.name, 'string')
    config_checks:assert(#output.name:strip() > 0, '%s.name expected to be non-empty', field)
    config_checks:check_luatype(field..'.type', output.type, 'string')

    if output.type == 'soap' then
        validate_soap_output(field, output)
    elseif output.type == 'runner' then
        validate_runner_output(field, output)
    elseif output.type == 'dummy' then
        validate_dummy_output(field, output)
    elseif output.type == 'http' then
        validate_http_output(field, output)
    elseif output.type == 'kafka' then
        validate_kafka_output(field, output)
    elseif output.type == 'smtp' then
        validate_smtp_output(field, output)
    else
        config_checks:assert(false, '%s has unknown type %q', field, output.type)
    end
end

local function validate_config(cfg)
    local tc_cfg = cfg['connector'] or {}
    config_checks:check_luatype('output', tc_cfg.output, 'table')

    local seen_outputs = {}
    for k, output in pairs(tc_cfg.output) do
        local field = string.format('output[%s]', k)

        config_checks:check_luatype(field, output, 'table')
        validate_output(field, output)

        config_checks:assert(not seen_outputs[output.name],
            'duplicate outputs with name %q', output.name)
        seen_outputs[output.name] = true
    end
end

return {
    output_exists = output_exists,
    validate_output = validate_output,
    validate = validate_config,
}
