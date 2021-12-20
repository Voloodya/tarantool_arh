local module_name = 'scheduler'

local clock = require('clock')
local fiber = require('fiber')
local uuid = require('uuid')
local checks = require('checks')
local fun = require('fun')
local sorted_pairs = require('common.sorted_pairs')

local log = require('log.log').new(module_name)

local cron = require('common.cron')
local errors = require('errors')
local request_context = require('common.request_context')
local utils = require('common.utils')
local vars = require('common.vars').new(module_name)
local account = require('common.admin.account')
local account_provider = require('account_provider.account_provider')
local tenant = require('common.tenant')

local task_storage = require('storage.task_storage')

local kinds = require('tasks.kinds')
local statuses = require('tasks.statuses')
local states = require('tasks.scheduler.states')

local cartridge = require('cartridge')
local rpc = require('cartridge.rpc')
local cartridge_utils = require('cartridge.utils')

local scheduler_error = errors.new_class('scheduler error')
-- Require to allow use in fun.call
require('tasks.system.data_expiration')

vars:new('state', states.WAITING)
vars:new('need_to_check_runners', nil)
vars:new('task_observers')
vars:new('task_delayed')

vars:new('tasks')
vars:new('tasks_prev')

local FIELDS = {
    { name = 'id',          type = 'string' },
    { name = 'name',        type = 'string' },
    { name = 'kind',        type = 'string' },
    { name = 'schedule',    type = 'string' },
    { name = 'context',     type = 'map'    },
    { name = 'started',     type = 'number' },
    { name = 'finished',    type = 'number',    is_nullable = true },
    { name = 'status',      type = 'number',    is_nullable = true },
    { name = 'result',      type = 'string',    is_nullable = true },
    { name = 'runner',      type = 'string',    is_nullable = true },
    { name = 'launched_by', type = 'string',    is_nullable = true },
}

local ID_FIELD          = 1
local NAME_FIELD        = 2
local KIND_FIELD        = 3
local SCHEDULE_FIELD    = 4
local CONTEXT_FIELD     = 5
local STARTED_FIELD     = 6
local FINISHED_FIELD    = 7
local STATUS_FIELD      = 8
local RESULT_FIELD      = 9
local RUNNER_FIELD      = 10
local LAUNCHED_BY_FIELD = 11

local BASE_SPACE_NAME = 'tdg_scheduler_task_list'

local PAUSE_BEFORE_RESTART_SEC = 60
local PAUSE_BEFORE_STATE_UPDATE = 3

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    scheduler_error:assert(space, 'scheduler task list %q is not initialized', space_name)
    return space
end

local function init()
    if box.info.ro then
        log.error("Scheduler can't work on a read-only instance")
        return
    end

    cron.init()
    task_storage.init()

    local space_name = get_space_name()
    local space = box.space[space_name]
    if space ~= nil then
        return
    end

    box.begin()
    space = box.schema.space.create(space_name, { if_not_exists = true })

    space:format(FIELDS, { if_not_exists = true })

    space:create_index('id', {
        type = 'TREE',
        unique = true,
        if_not_exists = true,
        parts = {
            {field = 'id', type = 'string'},
        },
    })
    space:create_index('name', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {
            {field = 'name', type = 'string'},
            {field = 'started', type = 'number'},
        }
    })
    box.commit()
end

local function deinit()
    cron.deinit()
    for name, _ in pairs(cron.list()) do
        cron.remove(name)
    end

    for _, worker in pairs(vars.task_observers) do
        if worker:status() ~= 'dead' then
            fiber.kill(worker:id())
        end
    end
    vars.task_observers = {}

    for _, worker in ipairs(vars.task_delayed) do
        if worker:status() ~= 'dead' then
            fiber.kill(worker:id())
        end
    end
    vars.task_delayed = {}
    vars.tasks = {}
    vars.tasks_prev = {}
end

local function task_is_not_system(_, props)
    return not kinds.is_system(props.kind)
end

local function compare_tasks(x, y)
    -- Group by names
    if x.name ~= y.name then
        return x.name < y.name
    end

    -- If empty started field then it is a fake task for UI
    if x.started == '' then
        return true
    end
    if y.started == '' then
        return false
    end

    -- First in list = last in time
    return x.started > y.started
end

local function get_list()
    local space = get_space()

    local result = {}

    -- Hide system tasks from user
    local tasks = fun.iter(vars.tasks):filter(task_is_not_system):tomap()

    for name, props in sorted_pairs(tasks) do
        local is_running = false
        for _, task in space.index.name:pairs({name}) do
            local info = {
                id = task[ID_FIELD],
                name = name,
                kind = task[KIND_FIELD],
                schedule = task[SCHEDULE_FIELD],
                started = '',
                finished = '',
                status = statuses.to_string(task[STATUS_FIELD]),
                result = '',
                runner = '',
                launched_by = task[LAUNCHED_BY_FIELD] or '',
            }

            if task[STARTED_FIELD] ~= box.NULL then
                info.started = utils.nsec_to_iso8601_str(task[STARTED_FIELD])
            end

            if task[FINISHED_FIELD] ~= box.NULL then
                info.finished = utils.nsec_to_iso8601_str(task[FINISHED_FIELD])
            end

            if task[RESULT_FIELD] ~= box.NULL then
                info.result = task[RESULT_FIELD]
            end

            if task[RUNNER_FIELD] ~= box.NULL then
                info.runner = task[RUNNER_FIELD]
            end

            if task[STATUS_FIELD] == statuses.RUNNING then
                is_running = true
            end
            table.insert(result, info)
        end

        local info = {
            id = props.id or '',
            name = name,
            kind = props.kind or '',
            schedule = props.schedule or '',
            started = '',
            finished = '',
            status = '',
            result = '',
            runner = '',
            launched_by = props.launched_by or '',
        }

        if props.error ~= nil then
            info.status = statuses.to_string(statuses.FAILED)
            info.result = props.error
            table.insert(result, info)
        elseif props.kind == kinds.SINGLE_SHOT then
            info.status = statuses.to_string(statuses.DID_NOT_START)
            table.insert(result, info)
        elseif is_running == false then
            info.status = statuses.to_string(statuses.PENDING)
            table.insert(result, info)
        end
    end

    table.sort(result, compare_tasks)

    return result
end

local function delayed_task_exists(name)
    local task = vars.task_delayed[name]
    if task == nil or vars.task_delayed[name]:status() == 'dead' then
        return false
    end
    return true
end

local function do_later(func, name, id, opts)
    if vars.tasks[name] == nil then
        return
    end

    if delayed_task_exists(name) then
        return
    end

    local f = tenant.fiber_new(function(opts)
        opts = opts or {}
        local delay = opts.delay

        if delay == nil then
            delay = vars.tasks[name].pause_sec
        end

        if delay == nil then
            delay = PAUSE_BEFORE_RESTART_SEC
        end

        fiber.sleep(delay)

        local _, err = func(name, id)
        if err ~= nil then
            log.error(err)
        end
    end, opts)

    f:name('dolater:' .. name, { truncate = true })

    vars.task_delayed[name] = f
end

local start
local restart
local wait_runner_and_check_tasks

local function start_task_with_request_context(name)
    -- we must keep request_context if this is a task launched by the user
    -- otherwise we must launch the task by as a system user

    local ctx
    local tenant_uid
    if not request_context.is_empty() then
        ctx = request_context.get()
        tenant_uid = tenant.uid()
        request_context.clear()
    end

    request_context.init()
    account.set_anonymous(tenant_uid)

    local task = vars.tasks[name]
    if task ~= nil and task.run_as ~= nil then
        if task.run_as.user ~= nil then
            local user, err = account_provider.get_user_by_login(task.run_as.user)
            if err ~= nil then
                return nil, err
            end
            if user ~= nil then
                account.set_user(user)
            end
        end
        if account.is_empty() or account.is_unauthorized() or account.id() == nil then
            request_context.set(ctx)
            return nil, scheduler_error:new('Unable to authorize task as %q',
                task.run_as.user)
        end
    end

    local res, err = start(name)

    if ctx ~= nil then
        request_context.set(ctx)
    else
        request_context.clear()
    end

    if err ~= nil then
        log.error(err)
    end

    return res, err
end

local function start_continuous_task(name)
    log.verbose('Starting continuous task %q', name)
    return start_task_with_request_context(name)
end

local DEFAULT_KEEP = 10
local function clean_old_tasks(name)
    local space = get_space()
    local task_info = vars.tasks[name]
    local offset = task_info.keep or DEFAULT_KEEP

    local tuples = space.index.name:select({name}, {iterator = box.index.REQ, offset = offset})
    for _, tuple in ipairs(tuples) do
        space:delete({tuple.id})
    end
end

local function complete_task(name, id, info, need_to_clean_storage)
    checks('string', 'string', 'table', 'boolean')

    if info.status == statuses.FAILED then
        log.error('Task %q with id %q has been finished (%s): %s',
            name, id, statuses.to_string(info.status), info.result)
    else
        log.verbose('Task %q with id %q has been finished (%s)', name, id, statuses.to_string(info.status))
    end

    local space = get_space()
    space:update(id, {
        {'=', FINISHED_FIELD, info.finished or box.NULL },
        {'=', STATUS_FIELD, info.status },
        {'=', RESULT_FIELD, info.result or box.NULL } })

    if need_to_clean_storage then
        task_storage.delete(id)
    end

    vars.task_observers[id] = nil

    if vars.tasks[name] ~= nil and vars.tasks[name].kind == kinds.CONTINUOUS then
        do_later(start_continuous_task, name)
    end

    clean_old_tasks(name)
end

local function wait(uri, name, id)
    checks('string', 'string', 'string')

    if vars.task_observers[id] ~= nil and vars.task_observers[id]:status() ~= 'dead' then
        return
    end

    log.verbose('Waiting for completion of task %q with id %q running on %q', name, id, uri)

    vars.task_observers[id] = tenant.fiber_new(
        function ()
            local info, err = cartridge.rpc_call('runner', 'wait_task', {id}, {uri = uri})
            if err ~= nil then
                log.error(err)
            end
            if info ~= nil then
                complete_task(name, id, info, true)
                return
            end

            info = task_storage.get(id)
            if info ~= nil then
                complete_task(name, id, info, true)
                return
            end

            log.warn("Can't find result of task %q with id %q", name, id)

            vars.task_observers[id] = nil

            do_later(restart, name, id)
        end)

    vars.task_observers[id]:name('wait:' .. id, { truncate = true })
end

local function runner_call(name, id, context)
    if vars.state == states.WAITING then
        return nil, scheduler_error:new('Impossible to run task %q because no runners.', name)
    end

    local space = get_space()
    local task_info = vars.tasks[name]

    local fn_name, args, task_name

    if task_info.kind == kinds.PERIODICAL_SYSTEM then
        fn_name = 'start_system_task'
        args = task_info.args
        task_name = task_info.fun
    else
        fn_name = 'start_task'
        args = {}
        task_name = task_info['function']
    end

    local url, err = cartridge.rpc_call(
        'runner',
        fn_name,
        { id, task_name, args, context },
        { prefer_local = false, leader_only = true })

    if url == nil then
        log.error(err)
        -- If no runners, then run waiter for runner, else it's not rpc call error
        if #rpc.get_candidates('runner') == 0 then
            wait_runner_and_check_tasks()
        end
        return nil, err
    end

    space:update(id, {
        {'=', STARTED_FIELD, clock.time64() },
        {'=', STATUS_FIELD, statuses.RUNNING },
        {'=', RUNNER_FIELD, url } })

    wait(url, name, id)
end

local function try_to_wait(uri, name, id)
    if uri ~= nil then
        return wait(uri, name, id)
    end

    local runners = rpc.get_candidates('runner')
    for _, runner in ipairs(runners) do
        local rc, err = wait(runner, name, id)
        if err == nil then
            return rc
        end
    end

    local info = task_storage.get(id)
    if info ~= nil then
        complete_task(name, id, info, true)
        return true
    end

    return nil, scheduler_error:new('Task %q not found', name)
end

restart = function(name, id)
    checks('string', 'string')

    log.warn('Restart task %q with id %q', name, id)

    local task_info = vars.tasks[name]
    if not task_info then
        return nil, scheduler_error:new('Unknown task: %q', name)
    end

    if vars.task_observers[id] then
        return nil, scheduler_error:new('Task with %q id %q is already watching', name, id)
    end

    local space = get_space()

    local info = space:get(id)
    if info == nil then
        return nil, scheduler_error:new("Can't find restarted %q task with id %q", name, id)
    end

    runner_call(name, id, info[CONTEXT_FIELD])
    return id
end

start = function(name)
    checks('string')
    log.verbose('Start task %q', name)

    local task_info = vars.tasks[name]
    if not task_info then
        return nil, scheduler_error:new('Unknown task: %q', name)
    end

    local space = get_space()

    if task_info.kind ~= kinds.SINGLE_SHOT then
        local task = space.index.name:max({name})
        if task ~= nil then
            if statuses.is_running(task.status) then
                return nil, scheduler_error:new('Task %q already running', name)
            elseif task.status == statuses.PENDING then
                log.warn('Task %q already pending', name)
                return task.id
            end
        end
    end

    local id = uuid.str()

    if vars.task_observers[id] then
        return nil, scheduler_error:new('Task %q with id %q already watching', name, id)
    end

    local context = request_context.get()
    context.id = id
    request_context.set(context)

    space:insert({
        id,
        name,
        task_info.kind,
        task_info.schedule or '',
        context,
        clock.time64(),
        box.NULL,
        statuses.PENDING,
        box.NULL,
        box.NULL,
        account.name() or ''
    })

    runner_call(name, id, context)

    return id
end

local function stop(id)
    checks('string')

    local space = get_space()

    local task = space:get(id)
    if task == nil then
        return nil, scheduler_error:new("Can't find task with id %q", id)
    end

    local name = task[NAME_FIELD]
    if vars.tasks[name] == nil then
        return nil, scheduler_error:new("Can't find task info with name %q", name)
    end

    log.info('Stopping task %q with id %q', name, id)

    if task[STATUS_FIELD] == statuses.PENDING then
        -- if pending status then task not on runner, only in scheduler
        local info = {
            finished = clock.time64(),
            status = statuses.STOPPED,
            result = 'Canceled by user',
        }
        complete_task(name, id, info, true)
        return 'ok'
    elseif task[STATUS_FIELD] ~= statuses.RUNNING then
        return nil, scheduler_error:new('Task with id %q is not pending or running', id)
    end

    local runner = task[RUNNER_FIELD]

    if vars.task_observers[id] == nil then
        wait(runner, name, id)
    end

    local uri = task[RUNNER_FIELD]
    local res, err = cartridge.rpc_call('runner', 'stop_task', {id}, {uri = uri})
    if res == nil then
        log.error(err)
        return nil, err
    end

    return 'ok'
end

local function forget(id)
    checks('string')

    log.info('Forget task with id %q', id)

    local space = get_space()

    local task = space:get(id)
    if task == nil then
        return nil, scheduler_error:new("Can't find task with id %q", id)
    end

    if statuses.is_running(task[STATUS_FIELD]) then
        return nil, scheduler_error:new("Task %q with id %q still running", task[NAME_FIELD], id)
    end

    space:delete(id)

    return 'ok'
end

local function start_periodical_task(_, name)
    log.verbose('Starting periodical task %q', name)
    return start_task_with_request_context(name)
end

local function register_system_task(_, name, task)
    checks('?', 'string', { schedule = 'string', fun = 'string', args = '?table' })
    log.info('Registration of system task %q', name)
    cron.remove(name)
    task.args = task.args or {}
    vars.tasks[name] = task
    vars.tasks[name].kind = kinds.PERIODICAL_SYSTEM

    local ok, err = cron.add(name, task.schedule, 'tasks.scheduler.scheduler.start_periodical_task', {name})
    if not ok then
        log.error(err)
        if task.name ~= nil then
            vars.tasks[task.name] = nil
        end
        return nil, err
    end
    return true
end

local function stop_by_name(name)
    local space = get_space()
    local task = space.index.name:max({name})
    if task ~= nil then
        return stop(task.id)
    end
    return true
end

local function reload_tasks(old_task_cfg, new_task_cfg)
    local task_is_not_changed = {}

    old_task_cfg = fun.iter(old_task_cfg):filter(task_is_not_system):tomap()
    for name, old_task in pairs(old_task_cfg) do
        local new_task = new_task_cfg[name]
        local changed
        if new_task == nil then
            changed = string.format('Task %q has been deleted by user', name)
        elseif old_task.kind ~= new_task.kind then
            changed = string.format('Kind of %q has been changed (%s -> %s)', name, old_task.kind, new_task.kind)
        elseif old_task.schedule ~= new_task.schedule then
            changed = string.format('Schedule of %q has been changed (%s -> %s)', name,
                old_task.schedule, new_task.schedule)
        elseif utils.cmpdeeply(old_task.run_as, new_task.run_as) == false then
            local old_run_as = old_task.run_as and old_task.run_as.user or nil
            local new_run_as = new_task.run_as and new_task.run_as.user or nil
            changed = string.format('Run as of %q has been changed (%s -> %s)', name, old_run_as, new_run_as)
        else
            task_is_not_changed[name] = true
            -- Inherit an error and status if task is not changed
            new_task.error = old_task.error
            new_task.status = old_task.status
        end

        if changed ~= nil then
            log.info(changed)
            if old_task.kind == kinds.PERIODICAL then
                cron.remove(name)
            end
            stop_by_name(name)
        end
    end

    for name, task in pairs(new_task_cfg) do
        if task.kind == kinds.CONTINUOUS and task_is_not_changed[name] == nil then
            log.info('Registration of continuous task %q', name)
            local res, err = start_continuous_task(name)
            if res == nil then
                task.error = tostring(err)
            end
        elseif task.kind == kinds.PERIODICAL and task_is_not_changed[name] == nil then
            log.info('Registration of periodical task %q', name)
            cron.remove(name)
            local ok, err = cron.add(name, task.schedule,
                'tasks.scheduler.scheduler.start_periodical_task', { name })
            if not ok then
                log.error(err)
                task.error = tostring(err)
            end
        end
    end
end

local function check_tasks()
    if box.info.ro == true then
        return
    end

    local space = get_space()

    for _, task in space.index.id:pairs() do
        local id = task[ID_FIELD]
        local name = task[NAME_FIELD]
        local task_info = vars.tasks[name]
        if task_info then
            if task.status == statuses.PENDING then
                local info = task_storage.get(id)
                if info == nil then
                    local res, err = restart(name, id)
                    if res == nil then
                        log.error(err)
                        task_info.error = tostring(err)
                    end
                else
                    -- check runner field
                    local _, err = try_to_wait(task[RUNNER_FIELD], name, id)
                    if err ~= nil then
                        log.error(err)
                    end
                end
            elseif task.status == statuses.RUNNING then
                wait(task[RUNNER_FIELD], name, id)
            end
        end
    end
end

local allowed_expiration_strategies = {
    ['file'] = true,
    ['cold_storage'] = true,
}

local DATA_EXPIRATION_TASK_PREFIX = 'data_expiration_'
local function init_data_expiration(cfg)
    -- Cancel previous tasks
    local task_list = cron.list()
    for name in pairs(task_list) do
        if name:startswith(DATA_EXPIRATION_TASK_PREFIX) then
            cron.remove(name)
        end
    end

    for _, record in ipairs(cfg or {}) do
        if allowed_expiration_strategies[record.strategy] == true then
            register_system_task(nil, DATA_EXPIRATION_TASK_PREFIX .. record.type, {
                fun = 'tasks.system.data_expiration.start',
                schedule = record.schedule,
                args = {record.type},
            })
        end
    end
end

local function set_state(state)
    if vars.state ~= state then
        vars.state = state
        log.info('Scheduler status changed to ' .. state)
    end
end

wait_runner_and_check_tasks = function()
    -- Fiber already exists => signal to run check now
    if vars.need_to_check_runners ~= nil then
        vars.need_to_check_runners:signal()
        return
    end

    -- CV to run check before timeout
    vars.need_to_check_runners = fiber.cond()

    local f = tenant.fiber_new(function()
        while true do
            -- Check runners
            if #rpc.get_candidates('runner') > 0 then
                -- Remove CV
                vars.need_to_check_runners = nil
                -- Update state and run tasks
                set_state(states.ACTIVE)
                local _, err = scheduler_error:pcall(check_tasks)
                if err ~= nil then
                    log.error('Check tasks failed: %s', err)
                end
                local old_task_cfg = vars.tasks_prev or {}
                local new_task_cfg = vars.tasks or {}
                reload_tasks(old_task_cfg, new_task_cfg)
                vars.tasks_prev = new_task_cfg
                return
            end
            -- No runners => we are waiting
            set_state(states.WAITING)

            -- Pause before next check
            vars.need_to_check_runners:wait(PAUSE_BEFORE_STATE_UPDATE)
        end
    end)

    f:name('state_updater_fiber')
end

local function apply_config(cfg)
    checks('table')

    vars.tasks = vars.tasks or {}
    vars.task_observers = vars.task_observers or {}
    vars.task_delayed = vars.task_delayed or {}

    local tasks = {}
    if cfg.tasks ~= nil then
        tasks = table.deepcopy(cfg['tasks'])
    end
    local expiration = {}
    -- FIXME: Remove expiration
    if cfg.versioning ~= nil then
        expiration = table.deepcopy(cfg['versioning'])
    elseif cfg.expiration ~= nil then
        expiration = table.deepcopy(cfg['expiration'])
    end

    cartridge_utils.table_setrw(tasks)
    cartridge_utils.table_setrw(expiration)

    vars.tasks = tasks or {}

    -- start check tasks when runner will be available
    wait_runner_and_check_tasks()

    init_data_expiration(expiration)
end

return {
    -- needed for cron call
    start_periodical_task = start_periodical_task,

    init = init,
    deinit = deinit,
    apply_config = apply_config,

    get_list = get_list,

    start = start,
    stop = stop,
    register_system_task = register_system_task,
    forget = forget,

    allowed_expiration_strategies = allowed_expiration_strategies,
}
