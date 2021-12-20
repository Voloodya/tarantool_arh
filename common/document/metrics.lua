local module_name = 'common.document.metrics'

local json = require('json')
local fiber = require('fiber')
local cartridge = require('cartridge')

local request_context = require('common.request_context')
local errors = require('errors')
local defaults = require('common.defaults')
local log = require('log.log').new(module_name)
local limit_error = errors.new_class('limit_error')
local document_key_def = require('common.document.key_def')

local function get()
    return fiber.self().storage.query_metrics
end

local function assert_scanned_limit(metrics, tuple)
    if metrics.scanned > metrics.scanned_limit then
        local pkey = metrics.pk_key_def:extract_key(tuple)
        local message = string.format('Hard limit for scan rows reached %d for space %q plan \'%s\' \
        on tuple with primary index %s',
            metrics.scanned_limit, metrics.space_name, json.encode(metrics.plan), pkey)
        log.info(message)
        limit_error:assert(false, message)
    end
end

local function assert_returned_limit(metrics, tuple)
    if metrics.returned > metrics.returned_limit then
        local pkey = metrics.pk_key_def:extract_key(tuple)
        local message = string.format('Hard limit for returned rows reached %d for space %q plan \'%s\' \
        on tuple with primary index %s',
            metrics.returned_limit, metrics.space_name, json.encode(metrics.plan), pkey)
        log.info(message)
        limit_error:assert(false, message)
    end
end

local function init(opts)
    opts = opts or {}

    local force_yield_limit = cartridge.config_get_readonly('force-yield-limit') or defaults.FORCE_YIELD_LIMIT

    local query_metrics = {
        scanned = 0,
        returned = 0,

        ignore_hard_limits = opts.ignore_hard_limits == true,
        scanned_limit = opts.scanned_limit or defaults.HARD_LIMITS_SCANNED,
        returned_limit = opts.returned_limit or defaults.HARD_LIMITS_SCANNED,
        space_name = opts.space_name,
        plan = opts.plan,
        pk_key_def = opts.pk_key_def,
        force_yield_limit = force_yield_limit,
    }

    if request_context.is_explain_enabled() then
        query_metrics.primary_space_scanned = 0
        query_metrics.history_space_scanned = 0
        query_metrics.primary_space_returned = 0
        query_metrics.history_space_returned = 0
    end

    fiber.self().storage.query_metrics = query_metrics
end

local function inc_scanned(tuple)
    local metrics = get()
    metrics.scanned = metrics.scanned + 1

    if metrics.ignore_hard_limits ~= true then
        assert_scanned_limit(metrics, tuple)
    elseif metrics.scanned % metrics.force_yield_limit == 0 then
        fiber.yield()
    end
end

local function inc_primary_space_returned()
    get().primary_space_returned = get().primary_space_returned + 1
end

local function inc_history_space_returned()
    get().history_space_returned = get().history_space_returned + 1
end

local function inc_returned_for_space_type(state)
    local space_name = document_key_def.get_iterator_space_name(state)
    if space_name:startswith('history') then
        inc_history_space_returned()
    else
        inc_primary_space_returned()
    end
end

local function inc_returned(iterator, tuple)
    local metrics = get()
    metrics.returned = metrics.returned + 1

    if metrics.ignore_hard_limits ~= true then
        assert_returned_limit(metrics, tuple)
    end

    if request_context.is_explain_enabled() then
        inc_returned_for_space_type(iterator.state)
    end
end

local function inc_primary_space_scanned()
    get().primary_space_scanned = get().primary_space_scanned + 1
end

local function inc_history_space_scanned()
    get().history_space_scanned = get().history_space_scanned + 1
end

local iterator_stat_scanned_inc = {
    primary = inc_primary_space_scanned,
    history = inc_history_space_scanned,
}

--[[
    Wrap tarantool iterator to count
    how many records were scanned for specific iterator type
--]]
local function wrap_iterator(iterator, space_type)
    if not request_context.is_explain_enabled() then
        return iterator
    end

    local iterator_gen = iterator.gen
    local function gen(...)
        local state, tuple = iterator_gen(...)
        if tuple ~= nil then
            iterator_stat_scanned_inc[space_type]()
        end
        return state, tuple
    end

    iterator.gen = gen
    return iterator
end

local function get_summary()
    local metrics = get()
    return {
        primary_space_scanned = metrics.primary_space_scanned,
        history_space_scanned = metrics.history_space_scanned,
        primary_space_returned = metrics.primary_space_returned,
        history_space_returned = metrics.history_space_returned,
    }
end

return {
    init = init,
    get = get_summary,
    inc_scanned = inc_scanned,
    inc_returned = inc_returned,

    wrap_iterator = wrap_iterator,
}
