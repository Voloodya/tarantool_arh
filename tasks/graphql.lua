local cartridge = require('cartridge')

local graphql = require('common.graphql')
local tenant = require('common.tenant')
local account_provider = require('account_provider.account_provider')

local types = require('graphql.types')

local function get_task_list(_, _)
    local res, err = cartridge.rpc_call('core', 'get_task_list', {},
                                      { leader_only = true })
    if err ~= nil then
        return nil, err
    end

    for _, task in ipairs(res) do
        if type(task.launched_by) == 'string' and #task.launched_by > 0 then
            local user = account_provider.get_user_by_login(task.launched_by)
            if user ~= nil then
                task.launched_by = user.username
            end
        end
    end

    return res
end

local function start_task(_, args)
    local res, err = cartridge.rpc_call(
        'core',
        'start_task',
        { args.name },
        { leader_only = true })
    return res, err
end

local function stop_task(_, args)
    local res, err = cartridge.rpc_call(
        'core',
        'stop_task',
        { args.id },
        { leader_only = true })
    return res, err
end

local function forget_task_result(_, args)
    local res, err = cartridge.rpc_call(
        'core',
        'forget_task',
        { args.id },
        { leader_only = true })
    return res, err
end

local function get_tasks_config_impl()
    local config = tenant.get_cfg_deepcopy('tasks') or {}
    local tasks = {}
    for name, section in pairs(config) do
        section.name = name
        table.insert(tasks, section)
    end
    return tasks
end

local function get_tasks_config(_, _)
    return get_tasks_config_impl()
end

local function strip_nulls(t)
    for k, v in pairs(t) do
        if v == nil then
            t[k] = nil
        end
    end
    return t
end

local function set_tasks_config(_, args)
    local cfg = {}
    if args['config'] == nil then
        args['config'] = {}
    end

    for _, task in ipairs(args['config']) do
        local name = task.name
        task.name = nil
        cfg[name] = strip_nulls(task)
    end

    local _, err = tenant.patch_config({tasks = cfg})
    if err ~= nil then
        return nil, err
    end
    return get_tasks_config_impl()
end

local function init()
    types.object {
        name = 'Task_list',
        description = 'A list of tasks',
        fields = {
            id = types.string.nonNull,
            name = types.string.nonNull,
            kind = types.string.nonNull,
            schedule = types.string.nonNull,
            started = types.string.nonNull,
            finished = types.string.nonNull,
            status = types.string.nonNull,
            result = types.string.nonNull,
            runner = types.string.nonNull,
            launched_by = types.string.nonNull,
        },
        schema = 'admin',
    }

    types.object {
        name = 'Task_config_section',
        description = 'A list of tasks',
        fields = {
            name = types.string.nonNull,
            kind = types.string.nonNull,
            schedule = types.string,
            pause_sec = types.long,
            ['function'] = types.string.nonNull,
            keep = types.long,
            run_as = types.object({
                name = 'Run_as',
                fields = {
                    user = types.string,
                }
            })
        },
        schema = 'admin',
    }

    local run_as = types.inputObject({
        name = 'Input_run_as',
        description = 'run task as',
        fields = {
            user = types.string,
        },
    })

    types.inputObject {
        name = 'Input_task_config_section',
        description = 'A list of tasks',
        fields = {
            name = types.string.nonNull,
            kind = types.string.nonNull,
            schedule = types.string,
            pause_sec = types.long,
            ['function'] = types.string.nonNull,
            keep = types.long,
            run_as = run_as,
        },
        schema = 'admin',
    }

    graphql.add_mutation_prefix('admin', 'tasks', 'Task management')
    graphql.add_callback_prefix('admin', 'tasks', 'Task management')

    graphql.add_callback({
        schema='admin',
        prefix='tasks',
        name='get_list',
        callback='tasks.graphql.get_task_list',
        kind=types.list('Task_list'),
    })

    graphql.add_mutation({
        schema='admin',
        prefix='tasks',
        name='start',
        callback='tasks.graphql.start_task',
        kind=types.string.nonNull,
        args={ name = types.string.nonNull },
    })

    graphql.add_mutation({
        schema='admin',
        prefix='tasks',
        name='stop',
        callback='tasks.graphql.stop_task',
        kind=types.string.nonNull,
        args={ id = types.string.nonNull },
    })

    graphql.add_mutation({
        schema='admin',
        prefix='tasks',
        name='forget',
        callback='tasks.graphql.forget_task_result',
        kind=types.string.nonNull,
        args={ id = types.string.nonNull },
    })

    graphql.add_callback({
        schema='admin',
        prefix='tasks',
        name='config',
        callback='tasks.graphql.get_tasks_config',
        kind=types.list('Task_config_section'),
    })

    graphql.add_mutation({
        schema='admin',
        prefix='tasks',
        name='config',
        callback='tasks.graphql.set_tasks_config',
        kind=types.list('Task_config_section'),
        args={ config = types.list('Input_task_config_section') },
    })
end

return {
    init = init,

    get_tasks_config = get_tasks_config,
    set_tasks_config = set_tasks_config,

    get_task_list = get_task_list,

    start_task = start_task,
    stop_task = stop_task,

    forget_task_result = forget_task_result,
}
