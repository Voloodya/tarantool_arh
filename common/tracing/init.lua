local module_name = 'common.tracing'

local uuid = require('uuid')
local uri = require('uri')
local membership = require('membership')
local opentracing = require('opentracing')
local zipkin = require('zipkin.tracer')
local errors = require('errors')

local log = require('log.log').new(module_name)
local request_context = require('common.request_context')
local config_filter = require('common.config_filter')
local vars = require('common.vars').new(module_name)

local config_error = errors.new_class('tracing_config_error')

vars:new('enabled', false)
vars:new('component_name')
vars:new_global('url', {})

local DEFAULT_HOST = 0

local empty_function = function() end
local dummy_span = setmetatable({}, {
    __index = function() return empty_function end,
    __newindex = function() return empty_function end,
})

local function finish_child(self, opts)
    self.span:finish(opts)
    if self.parent_context ~= nil then
        local context = request_context.get()
        opentracing.map_inject(
            self.parent_context,
            context
        )
    end
end

local function child_span_newindex(self, name, value)
    self.span[name] = value
end

local function create_child_span(span, parent_context)
    return setmetatable({
        parent_context = parent_context,
        span = span,
        finish = finish_child
    }, {
        __index = span,
        __newindex = child_span_newindex,
    })
end


local function start_span(name, ...)
    if not vars.enabled then
        return dummy_span
    end

    local context = request_context.get()

    if context.sample ~= true or context.disable_tracing then
        return dummy_span
    end

    assert(name, 'span name should be specified')
    name = string.format(name, ...)

    local span
    if context.trace_id == nil then
        local request_id_str = context.id
        local request_id = uuid.fromstr(request_id_str)

        span = opentracing.start_span(name, {
            trace_id = string.hex(request_id:bin())
        })
        opentracing.map_inject(span:context(), context)
    else
        local span_context = opentracing.map_extract(context)

        -- Set child span context and then restore parent context in finish()
        local child_span = opentracing.start_span_from_context(span_context, name)
        opentracing.map_inject(child_span:context(), context)
        span = create_child_span(child_span, span_context)
    end

    span:set_component(vars.component_name)
    span:set_peer_ipv4(vars.url.ipv4)
    span:set_peer_port(tonumber(vars.url.service) or DEFAULT_HOST)

    return span
end

local function disable_tracing()
    local context = request_context.get()
    context.disable_tracing = true
end

local function enable_tracing()
    local context = request_context.get()
    context.disable_tracing = false
end

local function inject_http_headers(carrier)
    local context = request_context.get()
    if context.sample ~= true then
        return carrier
    end
    opentracing.http_inject(opentracing.map_extract(context), carrier)
end

local default_cfg = {
    base_url = 'localhost:9411/api/v2/spans',
    api_method = 'POST',
    report_interval = 10,
    spans_limit = 1e4,
}

local function fill_in_service_url()
    local myself = membership.myself()
    local alias = myself.payload and myself.payload.alias
    local service_uri = alias and ('%s@%s'):format(alias, myself.uri) or myself.uri
    vars.component_name = service_uri
    vars.url = uri.parse(service_uri)
end

local function apply_config(config)
    local cfg, err = config_filter.compare_and_set(config, 'tracing', module_name)
    if err ~= nil then
        return true
    end

    if cfg == nil then
        vars.enabled = false
        return true
    end

    local sampler = { sample = function() return true end }
    local tracer, err = zipkin.new({
            base_url = cfg.base_url or default_cfg.base_url,
            api_method = cfg.api_method or default_cfg.api_method,
            report_interval = cfg.report_interval or default_cfg.report_interval,
            spans_limit = cfg.spans_limit or default_cfg.spans_limit,
            on_error = function(err) log.error('Tracing error: %s', err) end,
        }, sampler)
    config_error:assert(tracer, err)
    opentracing.set_global_tracer(tracer)
    vars.enabled = true
    fill_in_service_url()
end

return {
    start_span = start_span,
    enable = enable_tracing,
    disable = disable_tracing,
    inject_http_headers = inject_http_headers,

    apply_config = apply_config,

    -- tests
    fill_in_service_url = fill_in_service_url,
}
