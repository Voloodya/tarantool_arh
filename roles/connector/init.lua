local connector_config = require('connector.config')
local connector_server = require('connector.server')

local http_server = require('connector.httpserver')
local soap_server = require('connector.soapserver')

local odbc = require('common.odbc')

local function tenant_validate_config(cfg)
    local _, err = connector_config.validate(cfg)
    if err ~= nil then
        return nil, err
    end
    return true
end

local function tenant_apply_config(cfg, opts)
    local _, err = connector_server.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end
    return true
end

local function validate_config(cfg)
    local _, err = odbc.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function apply_config(_)
    return true
end

local function handle_http_request(obj, opts)
    return http_server.handle_http_request_obj(obj, nil, opts)
end

local function handle_soap_request(obj, opts)
    return soap_server.handle_soap_request_obj(obj, opts)
end

local function handle_output(output, data)
    return connector_server.handle_output(output, data)
end

return {
    validate_config = validate_config,
    apply_config = apply_config,

    -- Multitenancy
    tenant_validate_config = tenant_validate_config,
    tenant_apply_config = tenant_apply_config,

    -- rpc registry
    handle_http_request = handle_http_request,
    handle_soap_request = handle_soap_request,
    handle_output = handle_output,

    role_name = 'connector',
    implies_router = true,
    dependencies = {'cartridge.roles.vshard-router'},
}
