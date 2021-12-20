local module_name = 'connector.httpserver'

local json = require('json')
local cartridge = require('cartridge')
local http_server = require('http.server')

local log = require('log.log').new(module_name)

local http = require('common.http')
local tracing = require('common.tracing')
local vars = require('common.vars').new(module_name)
local tenant = require('common.tenant')
local httpd_utils = require('common.httpd_utils')

vars:new_global('connector_server')
vars:new_global('http_servers', {
    --[name] = {
    --    port = ...,
    --    routing_key = ...,
    --    is_async = ...,
    --}
})
vars:new_global('is_initialized', false)

local DEFAULT_HTTP_ROUTE_NAME = 'http'
local DEFAULT_HTTP_ROUTE_PATH = '/http'

local function get_http_port_number()
    if tenant.is_default() then
        return httpd_utils.get_default_httpd().port
    end

    return httpd_utils.get_port_number()
end

local function error_to_string(err)
    if type(err) == 'table' and err.err then
        return tostring(err.err)
    end
    return tostring(err)
end

local function handle_http_request_obj(body, route_info, opts)
    local ok, obj = pcall(json.decode, body)
    if not ok then
        return {
            status = 400,
            body = obj,
        }
    end

    local span = tracing.start_span('connector.handle_http_request')

    local route_name = DEFAULT_HTTP_ROUTE_NAME
    if route_info ~= nil and route_info.name ~= nil then
        route_name = route_info.name
    end

    local routing_key = vars.http_servers[route_name].routing_key
    opts = opts or {}
    if opts.is_async == nil then
        opts.is_async = vars.http_servers[route_name].is_async
    end

    local result, err = vars.connector_server.handle_request(obj, routing_key, opts)
    span:finish({errors = err})

    if err ~= nil then
        err = error_to_string(err)

        log.error('Http request failed: %s', err)

        return {
            status = 500,
            body = err,
        }
    end

    return {
        status = 200,
        body = json.encode(result),
    }
end

local function handle_http_request(req, route_info, opts)
    local body = req:read()
    if body == nil then
        return {
            status = 400,
            body = 'Expected a non-empty request body',
        }
    end

    return handle_http_request_obj(body, route_info, opts)
end

local function get_httpd_service_name_for_port(port)
    local service_name
    if port == nil or port == httpd_utils.get_default_httpd().port then
        service_name = httpd_utils.DEFAULT_SERVICE_NAME
    else
        service_name = 'httpd-' .. port
    end
    return service_name
end

local function setup(input)
    if input.routing_key ~= nil then
        log.info('Adding HTTP input %q with key %q', input.name, input.routing_key)
    else
        log.info('Adding HTTP input %q', input.name)
    end

    if input.name == DEFAULT_HTTP_ROUTE_NAME then
        vars.http_servers[input.name].routing_key = input.routing_key
        vars.http_servers[input.name].is_async = input.is_async
        return true
    end

    vars.http_servers[input.name] = vars.http_servers[input.name] or {}
    vars.http_servers[input.name].routing_key = input.routing_key
    vars.http_servers[input.name].is_async = input.is_async

    local route_name = input.name
    local path = input.path == nil and DEFAULT_HTTP_ROUTE_PATH or input.path
    local port = input.port == nil and get_http_port_number() or tostring(input.port)

    local service_name = get_httpd_service_name_for_port(port)
    local httpd = cartridge.service_get(service_name)
    if httpd == nil then
        httpd = http_server.new('0.0.0.0', port,
            table.copy(httpd_utils.get_default_httpd().options)) -- inherit from default httpd
        cartridge.service_set(service_name, httpd)
        httpd:start()
        http.init(httpd)
    end

    vars.http_servers[input.name] = vars.http_servers[input.name] or {}
    vars.http_servers[input.name].routing_key = input.routing_key
    vars.http_servers[input.name].port = port

    http.remove_route(httpd, route_name)
    http.add_route(httpd, { public = false, path = path, method = 'POST', name = route_name },
        'connector.httpserver', 'handle_http_request')

    return true
end

local function init_default_http_input(httpd)
    http.add_route(httpd, {
        public = false, path = DEFAULT_HTTP_ROUTE_PATH, method = 'POST', name = DEFAULT_HTTP_ROUTE_NAME
    }, 'connector.httpserver', 'handle_http_request')
    vars.http_servers[DEFAULT_HTTP_ROUTE_NAME] = {is_async = true}
end

local function cleanup(name)
    if name == DEFAULT_HTTP_ROUTE_NAME then
        vars.http_servers[DEFAULT_HTTP_ROUTE_NAME] = {is_async = true}
        return true
    end

    local port = vars.http_servers[name].port
    local service_name = get_httpd_service_name_for_port(port)
    local httpd = cartridge.service_get(service_name)

    vars.http_servers[name] = nil
    http.remove_route(httpd, name)

    if next(httpd.routes) == nil then
        httpd:stop()
        cartridge.service_set(service_name, nil)
    end

    return true
end

local function init()
    local httpd = cartridge.service_get(httpd_utils.DEFAULT_SERVICE_NAME)
    vars.connector_server = require('connector.server')
    -- TODO: enable only for default tenant?
    -- Seems it's better to remove it at all.

    -- Init default http route
    if not vars.is_initialized then
        init_default_http_input(httpd)
        vars.is_initialized = true
    end
end

return {
    init = init,
    setup = setup,
    cleanup = cleanup,
    handle_http_request = handle_http_request,
    handle_http_request_obj = handle_http_request_obj,

    get_httpd_service_name_for_port = get_httpd_service_name_for_port,
}
