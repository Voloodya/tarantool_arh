local module_name = 'connector.tarantool_protocol_server'

local errors = require('errors')

local tracing = require('common.tracing')
local request_context = require('common.request_context')
local auth = require('common.admin.auth')

local vars = require('common.vars').new(module_name)
local log = require('log.log').new(module_name)

local tarantool_protocol_error = errors.new_class('tarantool_protocol_error', { capture_stack = false })

local TARANTOOL_PROTOCOL_FUNC_NAME = 'tarantool_protocol_process'

vars:new('server')
vars:new('routing_key')
vars:new('is_async')
vars:new('enabled', false)

local function handle_request(object, options)
    if vars.enabled ~= true then
        return nil, tarantool_protocol_error:new('Tarantool protocol is disabled for current tenant')
    end

    options = options or {}

    local context, err = request_context.parse_options(options)
    if err ~= nil then
        return nil, err
    end
    request_context.init(context)

    if not auth.authorize_with_token(options.token) then
        request_context.clear()
        return nil, tarantool_protocol_error:new('Tarantool protocol request failed: invalid token')
    end

    local span = tracing.start_span('connector.handle_tarantool_protocol_request')
    local routing_key = options.routing_key or vars.routing_key
    local rc, err = vars.server.handle_request(object, routing_key, {is_async = vars.is_async})
    if not rc then
        log.error('Tarantool protocol request failed: %s', err)
    end
    span:finish({ errors = err })

    request_context.clear()
    return rc, err
end

local function setup(input)
    if input.routing_key ~= nil then
        log.info('Adding tarantool_protocol input with key %s', input.routing_key)
    else
        log.info('Adding tarantool_protocol input')
    end

    vars.enabled = true
    vars.routing_key = input.routing_key
    vars.is_async = input.is_async

    return true
end

local function cleanup(_)
    log.info('Cleanup tarantool_protocol input')

    vars.routing_key = nil
    vars.enabled = false

    return true
end

local function init()
    vars.server = require('connector.server')
    rawset(_G, TARANTOOL_PROTOCOL_FUNC_NAME, handle_request)
end

return {
    init = init,
    setup = setup,
    cleanup = cleanup,
}
