local module_name = 'storage.expiration.cold_storage'

local fiber = require('fiber')
local errors = require('errors')
local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local model_accessor = require('common.model_accessor')

local model_accessor_error = errors.new_class('Model accessor error')
vars:new('tasks')

local function start(type_name)
    if vars.tasks == nil then
        vars.tasks = {}
    end

    if vars.tasks[type_name] ~= nil then
        log.warn('Cold storage expiration task is already running for %q', type_name)
        return
    end

    vars.tasks[type_name] = true
    fiber.self():name('data_expiration_' .. tostring(type_name), {truncate = true})

    local _, err = model_accessor_error:pcall(model_accessor.run_expiration_task,
        type_name, 'cold_storage', nil)
    vars.tasks[type_name] = nil
    if err ~= nil then
        return nil, err
    end

    return true
end

return {
    start = start,
}
