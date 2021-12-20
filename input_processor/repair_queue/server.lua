local module_name = 'common.repair_queue.server'

local checks = require('checks')
local tracing = require('common.tracing')

local cartridge = require('cartridge')

local errors = require('errors')
local request_context = require('common.request_context')
local vars = require('common.vars').new(module_name)
local tenant = require('common.tenant')

local log = require('log.log').new(module_name)

local status = require('input_processor.repair_queue.statuses')

local storage = require('storage.repair_storage').new('tdg_input_repair')

local repair_queue_error = errors.new_class("repair_queue_error")

local DEFAULT_LIMIT = 25

vars:new('repair_all_fiber')

local function get(id)
    local span = tracing.start_span('repair_queue.get')
    local obj, err = storage:get(id)

    span:finish({error = err})
    if not obj and err then
        return nil, err
    end
    return obj
end

local function filter(from, to, reason, first, after)
    checks('?number|cdata', '?number|cdata', '?string', '?number', '?string')
    return storage:filter(from, to, reason, first or DEFAULT_LIMIT, after)
end

local function delete(id)
    local span = tracing.start_span('repair_queue.delete')
    local ok, err = storage:delete(id)
    span:finish({error = err})
    if not ok then
        return nil, err
    end
    return id
end

local function clear()
    local span = tracing.start_span('repair_queue.clear')
    local ok, err = storage:clear()
    span:finish({error = err})
    if not ok then
        return nil, err
    end
    return 'ok'
end

local function try_again(id)
    local obj, err = storage:get(id)
    if not obj then
        if err then
            return nil, err
        else
            return nil, repair_queue_error:new('Invalid object id: %q', id)
        end
    end

    local ok, err = storage:update_status(obj.id, status.IN_PROGRESS)
    if not ok then
        return nil, err
    end

    obj.context.repair = true

    local old_context
    if not request_context.is_empty() then
        old_context = request_context.get()
    end

    request_context.set(obj.context)

    local _, err = cartridge.rpc_call('runner', 'handle_input_object',
                                      {obj.object.object, {is_async = true}},
                                      {leader_only=true})

    if old_context then
        request_context.set(old_context)
    end

    if err ~= nil then
        return nil, err
    end

    return obj.id
end

local function try_again_all_impl()
    local cursor

    while true do
        local objects, err = cartridge.rpc_call('runner', 'repair_queue_filter',
                                                {nil, nil, nil, nil, cursor})
        if err ~= nil then
            log.error(err)
            return
        end

        if #objects == 0 then
            return
        end

        for _, obj in pairs(objects) do
            local _, err = try_again(obj.id)

            if err ~= nil then
                log.error(err)
            end
            cursor = obj.cursor
        end
    end
end

local function try_again_all()
    if vars.repair_all_fiber == nil or vars.repair_all_fiber:status() == 'dead' then
        vars.repair_all_fiber = tenant.fiber_new(try_again_all_impl)
    end
    return 'ok'
end

return {
    get = get,
    filter = filter,
    delete = delete,
    clear = clear,
    try_again = try_again,
    try_again_all = try_again_all
}
