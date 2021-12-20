local module_name = 'connector.soapserver'

local cartridge = require('cartridge')
local http = require('common.http')
local checks = require('checks')
local errors = require('errors')
local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local tracing = require('common.tracing')
local soap = require('common.soap')

local soap_e = errors.new_class('soap_error')

local DEFAULT_RESPONSE_BODY_TEMPLATE =
    '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">' ..
        '<soap:Body>' ..
            '<NS2:ConnectResponse xmlns:NS2="http://tarantool.io/xmlsoap_ns2">' ..
                '<response>' ..
                    '<outParams>%s</outParams>' ..
                '</response>' ..
            '</NS2:ConnectResponse>' ..
        '</soap:Body>' ..
    '</soap:Envelope>'

vars:new('server')
vars:new('handlers')
vars:new('routing_keys')
vars:new('is_async', true)
vars:new('wsdl', nil)
vars:new('success_response_body',
        string.format(DEFAULT_RESPONSE_BODY_TEMPLATE, '<status>SUCCESS</status>'))
vars:new('error_response_body',
        string.format(DEFAULT_RESPONSE_BODY_TEMPLATE, '<status>ERROR</status><error>%s</error>'))

local function error_to_string(err)
    if type(err) == 'table' and err.err then
        return tostring(err.err)
    end
    return tostring(err)
end

local function add_handler(name, callback)
    checks("string", "string")

    log.info('Adding SOAP handler: %s -> %s', name, callback)

    vars.handlers[name] = callback
end

local function get_wsdl(req)
    if req.query ~= 'WSDL' then
        return { status = 404 }
    end

    if vars.wsdl == nil then
        return { status = 404, body = string.format("WSDL was not configured") }
    end

    return {
        status = 200,
        headers = {
            ['content-type'] = "text/xml; charset=utf-8"
        },
        body = vars.wsdl
    }
end

local function set_wsdl(wsdl)
    checks('string')
    vars.wsdl = wsdl
end

local function get_failed_response_body(err)
    return string.format(vars.error_response_body, tostring(err))
end

local function get_success_response_body(result)
    return string.format(vars.success_response_body, tostring(result))
end

local function set_success_response_body(response_body)
    checks('?string')
    vars.success_response_body = response_body or
            string.format(DEFAULT_RESPONSE_BODY_TEMPLATE, '<status>SUCCESS</status>')
end

local function set_error_response_body(response_body)
    checks('?string')
    vars.error_response_body = response_body or
            string.format(DEFAULT_RESPONSE_BODY_TEMPLATE, '<status>ERROR</status><error>%s</error>')
end

local function soap_error(message, code)
    code = code or 400
    return {
        status = code,
        headers = {
            ['content-type'] = "text/xml; charset=utf-8"
        },
        body = get_failed_response_body(message),
    }
end

local function handle_soap_request_obj(body, opts)
    local _, elem_name, elems = soap_e:pcall(soap.decode, body)
    if elems == nil then
        local err = error_to_string(elem_name)
        log.error('Bad soap request: %s', err)
        return soap_error(err)
    end

    local result, err
    if vars.handlers[elem_name] == nil then
        err = ("Soap request failed: handler is not specified for element '%s'"):format(elem_name)
        return soap_error(err)
    else
        local span = tracing.start_span('connector.handle_soap_request: %s', elem_name)
        opts = opts or {}
        if opts.is_async == nil then
            opts.is_async = vars.is_async
        end
        result, err = vars.server.handle_request(elems, vars.routing_keys[elem_name], opts)
        span:finish({ error = err })

        if err ~= nil then
            err = error_to_string(err)
            log.error('Soap request failed: %s', err)
            return soap_error(err, 500)
        end
    end

    return {
        status = 200,
        headers = {
            ['content-type'] = "text/xml; charset=utf-8"
        },
        body = get_success_response_body(result),
    }
end

local function handle_soap_request(req)
    local body = req:read()
    if body == nil then
        return soap_error('Expected a non-empty request body')
    end

    return handle_soap_request_obj(body)
end

local function setup(input)
    set_wsdl(input.wsdl)
    set_success_response_body(input.success_response_body)
    set_error_response_body(input.error_response_body)

    for _, handler in pairs(input.handlers) do
        local handler_function = handler['function']

        if handler.routing_key ~= nil then
            log.info('Adding SOAP input: %s -> %s', handler_function, handler.routing_key)
        else
            log.info('Adding SOAP input: %s', handler_function)
        end
        vars.routing_keys[handler_function] = handler.routing_key
        add_handler(handler_function, 'connector.server.handle_request')
    end
    vars.is_async = input.is_async

    return true
end

local function cleanup(_)
    vars.routing_keys = {}
    vars.is_async = true
    return true
end

local function init()
    vars.handlers = vars.handlers or {}
    vars.routing_keys = vars.routing_keys or {}

    local httpd = cartridge.service_get('httpd')
    http.add_route(httpd, { path = '/soap', method = 'POST', public = false },
        'connector.soapserver', 'handle_soap_request')
    http.add_route(httpd, { path = '/soap', method = 'GET', public = true },
        'connector.soapserver', 'get_wsdl')
    vars.server = require('connector.server')
end

return {
    init = init,
    stop = cleanup,
    handle_soap_request = handle_soap_request,
    handle_soap_request_obj = handle_soap_request_obj,
    get_wsdl = get_wsdl,
    add_handler = add_handler,
    setup = setup,
    cleanup = cleanup,
    set_wsdl = set_wsdl,
}
