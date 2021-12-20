local module_name = 'jobs'

local fiber = require('fiber')
local uuid = require('uuid')

local errors = require('errors')

local config_error = errors.new_class('Invalid jobs config')
local config_checks = require('common.config_checks').new(config_error)

local cartridge = require('cartridge')
local request_context = require('common.request_context')
local task = require('common.task')
local vars = require('common.vars').new(module_name)
local utils = require('common.utils')
local tenant = require('common.tenant')

local log = require('log.log').new(module_name)

local task_statuses = require('tasks.statuses')

local job_storage = require('storage.jobs.storage')
local job_repair_storage = require('storage.repair_storage').new('tdg_jobs_repair')
local statuses = require('storage.jobs.statuses')

local defaults = require('common.defaults')

vars:new('already_running', false)
vars:new('dispatcher_fiber')
vars:new('job_pushed')
vars:new('job_finished')
vars:new('job_observers')
vars:new('max_jobs_in_parallel', defaults.MAX_JOBS_IN_PARALLEL)

local PAUSE_BEFORE_RESTART_SEC = 60

local function cleanup_and_notify_dispatcher(id)
    job_storage.delete(id)
    vars.job_observers[id] = nil
    vars.job_finished:signal()
end

local function success(job)
    if job.context.job_repair then
        log.info('Repair job completed successfully')
        local ok, err = job_repair_storage:delete(job.id)
        if not ok then
            return nil, err
        end
    end

    return true
end

local function failure(job, info)
    if job.context.job_repair then
        log.warn('Repair job failure: %s', info.result)

        local ok, err = job_repair_storage:update_status(job.id, statuses.FAILURE_AGAIN_STATUS, info.result)
        if not ok then
            return nil, err
        end
    else
        log.warn('Adding a new job to the repair job list: %s', info.result)

        local res, err = job_repair_storage:save(
            job:tomap({ names_only = true }), statuses.FAILURE_STATUS, info.result, job.context)
        if res == nil then
            return nil, err
        end
    end

    return true
end

local function complete_job(job, info)
    if info.status == task_statuses.COMPLETED then
        log.verbose('Job %q with id %q has been finished: %s', job.name, job.id, info.result)
        local _, err = success(job)

        if err ~= nil then
            log.error('Impossible to handle result of successfully completed job with id %s: %s',
                job.id, err)
        end
    else
        log.warn('Job %q with id %q has been failed: %s', job.name, job.id, info.result)
        local _, err = failure(job, info)
        if err ~= nil then
            log.error('Impossible to handle result of failed job with id %s: %s', job.id, err)
        end
    end

    return cleanup_and_notify_dispatcher(job.id)
end

local function wait(job, uri)
    log.verbose('Waiting for completion of job %q with id %q running on %q', job.name, job.id, uri)

    vars.job_observers[job.id] = tenant.fiber_new(
        function()
            local info, err = cartridge.rpc_call('runner', 'wait_task', {job.id}, {uri = uri})
            if err ~= nil then
                log.error(tostring(err))
            end
            if info ~= nil then
                complete_job(job, info)
                return
            end

            local job = job_storage.get(job.id)
            if job.result ~= nil then
                complete_job(job, job)
                return
            end

            log.warn("Can't find job %q with id %q (maybe runner was crashed)", job.name, job.id)

            -- just reset the runner field so we will try again at the next iteration
            local ok, err = job_storage.set_runner(job.id, nil)
            if ok == nil then
                log.error(err)
            end

            cleanup_and_notify_dispatcher(job.id)
        end)

    vars.job_observers[job.id]:name('wait:' .. job.id, { truncate = true })
end

local function start_job(job, replicaset_uuid)
    log.info('Start job with id %q', job.id)
    local url, err = cartridge.rpc_call(
        'runner',
        'start_job',
        { job.id, job.name, job.args, job.context, replicaset_uuid },
        { prefer_local = false, leader_only = true })
    if url == nil then
        log.error(err)
        return
    end

    local ok, err = job_storage.set_runner(job.id, url)
    if ok == nil then
        log.error(err)
    end

    wait(job, url)
end

local function job_dispatcher_loop()
    while true do
        local iter, err = job_storage.get_iter()
        if iter == nil then
            log.error(err)
            local TIME_TO_RESTORE_AFTER_ERROR = 5
            fiber.sleep(TIME_TO_RESTORE_AFTER_ERROR)
            return
        end

        local param, job = iter(iter.param, iter.state)

        while param do
            local running = utils.table_count(vars.job_observers)
            if running >= vars.max_jobs_in_parallel then
                vars.job_finished:wait()
            end

            if job.runner == nil then
                log.verbose('Starting job %q with id = %q', job.name, job.id)
                start_job(job, box.info.cluster.uuid)
            else
                if vars.job_observers[job.id] == nil then
                    log.verbose('Getting result of job %q with id = %q run on %q',
                        job.name, job.id, job.runner)
                    wait(job, job.runner)
                end
            end

            param, job = iter(param, iter.state)
        end

        vars.job_pushed:wait(PAUSE_BEFORE_RESTART_SEC)
    end
end

local function start()
    if vars.already_running then
        return
    end

    vars.already_running = true
    vars.job_observers = vars.job_observers or {}
    vars.job_pushed = vars.job_pushed or fiber.cond()
    vars.job_finished = vars.job_finished or fiber.cond()

    log.info('Starting job dispatcher')

    job_storage.init()
    job_repair_storage:init()

    vars.dispatcher_fiber = task.start(
        'storage.jobs.jobs',
        'job_dispatcher_loop',
        { interval = 0 })

    log.info('Job dispatcher started')
end

local function apply_config(conf)
    log.info('Applying jobs config')

    local jobs_conf = conf.jobs

    if jobs_conf ~= nil and jobs_conf.max_jobs_in_parallel ~= nil then
        vars.max_jobs_in_parallel = jobs_conf.max_jobs_in_parallel
    else
        vars.max_jobs_in_parallel = defaults.MAX_JOBS_IN_PARALLEL
    end
end

local function validate_config(cfg)
    local jobs_conf = cfg.jobs

    if jobs_conf == nil then
        return true
    end

    config_checks:check_luatype('jobs', jobs_conf, 'table')
    config_checks:check_optional_luatype('jobs.max_jobs_in_parallel', jobs_conf.max_jobs_in_parallel, 'number')

    return true
end

local function stop()
    log.info('Stopping job dispatcher')

    task.stop(vars.dispatcher_fiber)
    vars.dispatcher_fiber = nil

    vars.already_running = false
end

local function is_enabled()
    return vars.dispatcher_fiber ~= nil
end

local function push_job(name, args)
    local old_context = request_context.get()

    local new_context = table.deepcopy(old_context)
    local new_id = uuid.str()
    new_context.id = new_id

    log.verbose('Try to save job %q from request %q', new_id, old_context.id)

    local res, err = job_storage.save(new_id, name, args, new_context)
    if res == nil then
        log.error(err)
        return
    end

    log.verbose('Job %q saved', new_id)

    vars.job_pushed:signal()

    return true
end

local function set_job_result(id, status, result)
    local res, err = job_storage.set_result(id, status, result)
    if res == nil then
        log.error(err)
        return
    end

    return true
end

local function push_job_again(job)
    log.verbose('Try to save job %q', job.id)

    local res, err = job_storage.save(job.id, job.name, job.args, job.context)
    if res == nil then
        return nil, err
    end

    log.verbose('Job %q saved', job.id)

    vars.job_pushed:signal()

    return true
end

return {
    is_enabled = is_enabled,

    start = start,
    stop = stop,

    apply_config = apply_config,
    validate_config = validate_config,
    push_job = push_job,
    set_job_result = set_job_result,
    push_job_again = push_job_again,

    job_dispatcher_loop = job_dispatcher_loop
}
