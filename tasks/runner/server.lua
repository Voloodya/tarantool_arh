local module_name = 'task_runner.server'

local clock = require('clock')
local fiber = require('fiber')
local errors = require('errors')

local sandbox_registry = require('common.sandbox.registry')
local request_context = require('common.request_context')
local funcall = require('common.funcall')
local defaults = require('common.defaults')
local tenant = require('common.tenant')
local vars = require('common.vars').new(module_name)
local cartridge = require('cartridge')
local failover = require('cartridge.failover')

local config_error = errors.new_class('Invalid task_runner config')
local config_checks = require('common.config_checks').new(config_error)

local log = require('log.log').new(module_name)

local task_storage = require('storage.task_storage')
local job_storage = require('storage.jobs.storage')

local execution_policy = require('tasks.runner.execution_policy')
local statuses = require('tasks.statuses')

local task_runner_error = errors.new_class('task runner error')
local task_metrics = require('common.metrics.instruments.tasks')

vars:new('running')
vars:new('running_count', 0)
vars:new('tasks_kinds')

vars:new('running_count_threshold', defaults.RUNNING_COUNT_THRESHOLD)

local function get_current_leader_uri(replicaset_uuid)
    local topology_cfg = cartridge.config_get_readonly('topology')
    local leaders = failover.get_active_leaders()
    local leader_uuid = leaders[replicaset_uuid]
    return topology_cfg.servers[leader_uuid].uri
end

local function get_sandbox_function(name, args)
    local sandbox = vars.sandbox
    local fn, err = sandbox:dispatch_function(name, {protected = true})
    if not fn then
        return nil, string.format("Can't dispatch function %q: %s", name, err.str)
    end
    return function()
        return sandbox.call(fn, unpack(args, 1, table.maxn(args)))
    end
end

local function execute_function(run, context, result_channel, metrics, exec_policy, set_result_fn)
    request_context.set(context)
    task_metrics.start(metrics, exec_policy)
    local start_time = clock.monotonic()
    local res, err = run()
    local run_time = clock.monotonic() - start_time
    fiber.testcancel()
    local info = { finished = clock.time64() }
    if err == nil then
        info.status = statuses.COMPLETED
        info.result = tostring(res)
        task_metrics.succeed(metrics, exec_policy, run_time)
    else
        info.status = statuses.FAILED
        info.result = tostring(err)
        task_metrics.fail(metrics, exec_policy, run_time)
    end

    if set_result_fn ~= nil then
        set_result_fn(info)
    end

    result_channel:put(info)
end

local function start(exec_policy, id, run, context, metrics, save_result_fn)
    vars.running = vars.running or {}

    if vars.running[id] ~= nil then
        log.warn('Task with id %q is already running', id)
        return
    end

    local result_channel = fiber.channel(1)

    vars.running[id] = {
        result_channel = result_channel,
        fiber = tenant.fiber_new(execute_function, run, context, result_channel, metrics, exec_policy, save_result_fn)
    }

    vars.running[id].fiber:name('function:' .. id, { truncate = true })
    vars.running[id].labels = metrics
    vars.running_count = vars.running_count + 1
    if vars.running_count > vars.running_count_threshold then
        log.warn('Too many running functions. %d functions are running now', vars.running_count)
    end
end

local function start_task(id, name, args, context)
    local run, err = get_sandbox_function(name, args)
    if err ~= nil then
        return nil, err
    end

    local save_result_fn = function(info)
        task_storage.set_result(id, info.status, info.result)
    end

    local metrics = {
        name = name,
        kind = vars.tasks_kinds[name]
    }

    local _
    _, err = start(execution_policy.TASK, id, run, context, metrics, save_result_fn)
    if err ~= nil then
        return nil, err
    end
end

local function start_system_task(id, name, args, context)
    local _, err = funcall.exists(name)
    if err ~= nil then
        return nil, task_runner_error:new("Can't find function %q", name)
    end
    local run = function()
        return funcall.call(name, unpack(args, 1, table.maxn(args)))
    end
    log.verbose('Trying to run system task %q with id %q', name, id)

    local metrics = {name = name}

    local _
    _, err = start(execution_policy.SYSTEM, id, run, context, metrics)
    if err ~= nil then
        return nil, err
    end
end

local function start_job(id, name, args, context, replicaset_uuid)
    local run, err = get_sandbox_function(name, args)
    if err ~= nil then
        return nil, err
    end

    local save_result_fn = function(info)
        local leader_uri = get_current_leader_uri(replicaset_uuid)
        local ok, err = job_storage.push_result(leader_uri, id, info.status, info.result)
        if not ok then
            log.error('Failed to set job result on storage: %s', err)
        end
    end

    local metrics = {name = name}

    local _
    _, err = start(execution_policy.JOB, id, run, context, metrics, save_result_fn)
    if err ~= nil then
        return nil, err
    end
end

local function stop(id)
    local task = vars.running[id]
    if not task then
        return nil, task_runner_error:new("Can't find task with id %q", id)
    end

    if task.fiber:status() == 'dead' then
        return true
    end

    task.fiber:cancel()
    local info = {
        finished = clock.time64(),
        status = statuses.STOPPED,
        result = 'Canceled by user',
    }

    task_metrics.stop(task.labels)

    task.result_channel:put(info)

    return true
end

local function wait(id)
    local task = vars.running[id]
    if not task then
        return nil, task_runner_error:new("Can't find task with id %q", id)
    end

    local res = task.result_channel:get()

    -- if connection lost keep task so that the result can be read later
    if not box.session.peer(box.session.id()) then
        task.result_channel:put(res)
    else
        task.result_channel:close()
        vars.running[id] = nil
        vars.running_count = vars.running_count - 1
    end

    return res
end

local function validate_config(cfg)
    local task_runner_conf = cfg.task_runner

    if task_runner_conf == nil then
        return true
    end

    config_checks:check_luatype('task_runner', task_runner_conf, 'table')
    config_checks:check_optional_luatype('task_runner.running_count_threshold',
        task_runner_conf.running_count_threshold, 'number')

    return true
end

local function apply_config(cfg)
    local task_runner_conf = cfg.task_runner or {}

    if task_runner_conf.running_count_threshold ~= nil then
        vars.running_count_threshold = task_runner_conf.running_count_threshold
    else
        vars.running_count_threshold = defaults.RUNNING_COUNT_THRESHOLD
    end

    -- Cache tasks' kinds
    vars.tasks_kinds = {}
    if cfg.tasks ~= nil then
        for _, task in pairs(cfg.tasks) do
            vars.tasks_kinds[task['function']] = task.kind
        end
    end

    vars.sandbox = sandbox_registry.get('active')
end

local function init()
    task_storage.init()
    vars.running = {}
    vars.running_count = 0
end

return {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,
    start_task = start_task,
    start_system_task = start_system_task,
    start_job = start_job,
    stop = stop,
    wait = wait,
}
