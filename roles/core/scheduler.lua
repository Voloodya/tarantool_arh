local tasks_config = require('tasks.config')
local tasks_scheduler = require('tasks.scheduler.scheduler')
local tasks_scheduler_server = require('tasks.scheduler.server')

local function validate_config(cfg)
    return tasks_config.validate(cfg)
end

local function apply_config(cfg, opts)
    local _, err = tasks_scheduler_server.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function get_list()
    local rc, err = tasks_scheduler.get_list()
    return rc, err
end

local function start(name)
    return tasks_scheduler.start(name)
end

local function stop(id)
    return tasks_scheduler.stop(id)
end

local function forget(id)
    return tasks_scheduler.forget(id)
end

return {
    validate_config = validate_config,
    apply_config = apply_config,

    get_task_list = get_list,
    start_task = start,
    stop_task = stop,
    forget_task = forget,
}
