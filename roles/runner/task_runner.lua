local tasks_config = require('tasks.config')
local tasks_runner_server = require('tasks.runner.server')

local checks = require('checks')
local membership = require('membership')

local function init()
    tasks_runner_server.init()
end

local function tenant_validate_config(cfg)
    local _, err = tasks_runner_server.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end
    return tasks_config.validate(cfg)
end

local function tenant_apply_config(cfg)
    local _, err = tasks_runner_server.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end
    return true
end

local function start_job(id, name, args, context, replicaset_uuid)
    checks('string', 'string', '?table', '?table', 'string')
    local _, err = tasks_runner_server.start_job(id, name, args, context, replicaset_uuid)
    if err ~= nil then
        return nil, err
    end

    return membership.myself().uri
end

local function start_system_task(id, name, args, context)
    checks('string', 'string', '?table', '?table')
    local _, err = tasks_runner_server.start_system_task(id, name, args, context)
    if err ~= nil then
        return nil, err
    end

    return membership.myself().uri
end

local function start_task(id, name, args, context)
    checks('string', 'string', '?table', '?table')
    local _, err = tasks_runner_server.start_task(id, name, args, context)
    if err ~= nil then
        return nil, err
    end

    return membership.myself().uri
end

local function stop_task(id)
    checks('string')
    return tasks_runner_server.stop(id)
end

local function wait_task(id)
    checks('string')
    return tasks_runner_server.wait(id)
end

return {
    init = init,
    tenant_validate_config = tenant_validate_config,
    tenant_apply_config = tenant_apply_config,

    -- rpc registry
    start_task = start_task,
    stop_task = stop_task,
    wait_task = wait_task,
    start_job = start_job,
    start_system_task = start_system_task,
}
