local checks = require('checks')
local errors = require('errors')

local fiber = require('fiber')

local timeout_error = errors.new_class('timeout_error')

local request_context = require('common.request_context')
local sandbox_executor = require('common.sandbox.executor')
local function_dispatcher = require('common.sandbox.dispatcher')

local INFINITE = nil
local IMMEDIATELY = 0

local function start(channel, fn, args, context)
    -- Need to copy as we should not change
    -- parent context
    request_context.set(table.deepcopy(context))
    local res = { sandbox_executor.call(fn, unpack(args, 1, table.maxn(args))) }
    request_context.clear()
    channel:put(res)
end

local function cancel_fibers(fibers)
    for _, f in ipairs(fibers) do
        if f:status() ~= 'dead' then
            f:cancel()
        end
    end
end

local function spawn(sandbox, func_name, args, options)
    checks('sandbox', 'string', 'table', { timeout = '?number' })

    local fn, err = function_dispatcher.get(sandbox, func_name, {protected = true})
    if not fn then
        return nil, err
    end

    local timeout = options and options.timeout or INFINITE

    local channel = fiber.channel()

    local fibers = {}

    local results = {}

    local context = request_context.get()
    for _, arg in ipairs(args) do
        local to_pass = arg

        if type(arg) ~= 'table' then
            to_pass = { arg }
        end

        table.insert(fibers,
            fiber.create(start, channel, fn, to_pass, context))
    end

    local start_time = fiber.time()

    for _ in ipairs(args) do
        local wait_time = INFINITE

        if timeout ~= INFINITE then
            local now = fiber.time()
            wait_time = timeout - (now - start_time)

            if wait_time <= 0 then
                wait_time = IMMEDIATELY
            end
        end

        local res = channel:get(wait_time)

        if res == nil then
            cancel_fibers(fibers)
            return nil, timeout_error:new('Timeout error')
        end

        table.insert(results, res)
    end

    return results
end

local function spawn_n(sandbox, func_name, func_num, options)
    checks('sandbox', 'string', 'number', { timeout = '?number' })
    assert(func_num > 0)

    local args = {}

    for _ = 1, func_num do
        table.insert(args, box.NULL)
    end

    return spawn(sandbox, func_name, args, options)
end

return {
    spawn = spawn,
    spawn_n = spawn_n
}
