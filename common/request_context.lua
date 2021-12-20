local checks = require('checks')
local fiber = require('fiber')
local uuid = require('uuid')

local utils = require('common.utils')

local CONTEXT = 'request_context'

local function get_storage_key()
    return CONTEXT
end

local function check_context_id(context_id)
    if context_id
        and (type(context_id) ~= 'string' or uuid.fromstr(context_id) == nil)
    then
        error('bad argument context.id')
    end
    return true
end

local function check_context_version(context_version)
    if context_version and not utils.is_unsigned(context_version) then
        error('bad argument context.version')
    end
    return true
end

local function init(context)
    checks('?table')

    local storage = fiber.self().storage

    if storage[CONTEXT] ~= nil then
        error('request_context.init() expects the context to be empty\n' .. debug.traceback())
    end

    context = table.deepcopy(context) or {}

    check_context_id(context.id)
    check_context_version(context.version)

    if context.id == nil then
        context.id = uuid.str()
    end

    if context.account == nil then
        local account = require('common.admin.account')
        account.init(context)
    end

    storage[CONTEXT] = context
end

local function set(context)
    checks('table')

    local storage = fiber.self().storage

    if type(context.id) ~= 'string' or uuid.fromstr(context.id) == nil then
        error('bad argument context.id')
    end

    storage[CONTEXT] = context
end

local function clear()
    local storage = fiber.self().storage

    if storage[CONTEXT] == nil then
        error('request_context.clear() expects the context to be set\n' .. debug.traceback())
    end

    storage[CONTEXT] = nil
end

local function get()
    local context = fiber.self().storage[CONTEXT]
    if context == nil then
        error('request_context.get() expects the context to be set\n' .. debug.traceback())
    end
    return context
end

local function is_empty()
    return fiber.self().storage[CONTEXT] == nil
end

local function parse_options(options)
    checks('table')

    local request_id = options['request-id']
    if request_id ~= nil then
        if not checkers.uuid_str(request_id) then
            return nil, string.format(
                "Malformed http header 'request-id': "
                .. "expected uuid as 36-byte hexadecimal string, "
                .. "got %q", options['request-id']
            )
        end
    end

    local version = options['version']
    if version ~= nil then
        version = tonumber64(version)
        if not checkers.uint64(version) then
            return nil, string.format(
                "Malformed http header 'version': "
                .. "expected an unsigned 64-bit integer, "
                .. "got %q", options['version']
            )
        end
    end

    local should_sample = options['should-sample'] or options['x-b3-sampled']
    if should_sample == '1' or should_sample == 'true' then
        should_sample = true
    else
        should_sample = nil
    end

    local explain = options['explain']
    if explain ~= nil and (explain == '1' or explain:lower() == 'true') then
        explain = true
    else
        explain = nil
    end

    local request_context = {
        id = request_id,
        version = version,
        sample = should_sample,
        explain = explain,
    }

    -- Trace from external systems
    if should_sample == true and options['x-b3-traceid'] ~= nil then
        local trace_id = options['x-b3-traceid']
        request_context.trace_id = trace_id
        request_context.span_id = options['x-b3-spanid']
        request_context.parent_span_id = options['x-b3-parentspanid']
    end

    return request_context
end

local function is_explain_enabled()
    local context = fiber.self().storage[CONTEXT]
    if context == nil then
        return false
    end

    return context.explain == true
end

-- Sometimes we need to pass some additional context
-- to apply/patch config. We use request context for such
-- purpose.
local OPTIONS_KEY = 'REQUEST_CONTEXT_OPTIONS'

local function get_options()
    if is_empty() then
        return nil
    end
    return get()[OPTIONS_KEY]
end

local function put_options(options)
    -- Normally we shouldn't save options if context is not initialized
    if is_empty() then
        return
    end
    get()[OPTIONS_KEY] = options
end

return {
    get = get,
    init = init,
    set = set,
    clear = clear,
    is_explain_enabled = is_explain_enabled,
    is_empty = is_empty,

    parse_options = parse_options,

    get_storage_key = get_storage_key,

    get_options = get_options,
    put_options = put_options,
}
