local module_name = 'output_processor.list'

local checks = require('checks')
local log = require('log.log').new(module_name)

local storage = require('storage.repair_storage').new('tdg_output_repair')

local DEFAULT_LIMIT = 25

local function add(object, status, reason, context)
    checks('table', 'number', 'string', 'table')

    log.error('Adding a new object to the output_processor list with error: %s', reason)

    local _, err = storage:save(object, status, reason, context)
    if err then
        log.error(err)
        return nil, err
    end

    return context.id
end

local function processing_error(id, object, status, reason)
    checks('string', 'table', 'number', 'string')

    log.error('Output_Processor error: %s', reason)

    local _, err = storage:update_object(id, object, status, reason)
    if err ~= nil then
        log.error(err)
        return nil, err
    end

    return true
end

local function success(id)
    checks('string')

    local _, err = storage:delete(id)
    if err ~= nil then
        log.error(err)
        return nil, err
    end

    return true
end

local function get(id)
    return storage:get(id)
end

local function filter(from, to, reason, first, after)
    return storage:filter(from, to, reason, first or DEFAULT_LIMIT, after)
end

local function delete(id)
    return storage:delete(id)
end

local function clear()
    return storage:clear()
end

return {
    add = add,
    processing_error = processing_error,
    success = success,
    get = get,
    filter = filter,
    delete = delete,
    clear = clear,
}
