local module_name = 'common.task'

local fiber = require('fiber')
local all_vars = require('common.vars')
local log = require('log.log').new(module_name)
local checks = require('checks')
local vars = require('common.vars').new(module_name)
local tenant = require('common.tenant')
local errors = require('errors')

local task_error = errors.new_class('task_error')

vars:new('seq', 0)

local BASE_SPACE_NAME = 'tdg_common_tasks'

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_space()
    local space_name = get_space_name()
    return box.space[space_name]
end

local function init()
    local space_name = get_space_name()

    local space = box.space[space_name]

    if space ~= nil then
        return
    end

    if box.info.ro then
        return
    end

    box.begin()
    space = box.schema.space.create(space_name, {temporary=true})
    space:create_index('task_id', {unique = true, parts = {1, 'integer'}})
    space:create_index('running', {unique = false, parts = {2, 'boolean'}})

    space:format({
        {name = 'task_id', type = 'integer'},
        {name = 'running', type = 'boolean'},
        {name = 'fiber_id', type = 'integer'},
    })
    box.commit()
end

local function worker(module_name, function_name, options, task_id, ...)
    local vars = all_vars.new(module_name)

    local space = get_space()
    space:put({task_id, true, fiber.self():id()})

    if options.interval == nil then
        local fun = package.loaded[module_name][function_name]

        fun(...)

        space:put({task_id, false, fiber.self():id()})
        return
    end

    while true do
        local fun = package.loaded[module_name][function_name]

        local rc, res = xpcall(fun, debug.traceback, ...)

        if not rc then
            log.error(res)
        end

        local interval = options.interval

        if type(interval) == 'string' then
            interval = vars[interval]
        end

        fiber.sleep(interval)
    end
end

local function start(module_name, function_name, options, ...)
    checks("string", "string", "?table")

    options = options or {}
    init()

    task_error:assert(package.loaded[module_name] ~= nil,
        string.format("task.start(): module '%s' doesn't exist", module_name))

    task_error:assert(package.loaded[module_name][function_name] ~= nil,
        string.format("task.start(): Function '%s' doesn't exist in module '%s'", function_name, module_name))

    local space = get_space()
    local task_id = vars.seq
    vars.seq = vars.seq + 1

    local fiber_obj = tenant.fiber_new(worker, module_name, function_name, options, task_id, ...)
    fiber_obj:name(('task: %s.%s'):format(module_name, function_name), {truncate = true})
    if space:get({task_id}) == nil then
        space:put({task_id, true, fiber_obj:id()})
    end

    return task_id
end

local function stop(task_id)
    init()

    local space = get_space()
    local task = space:get({task_id})

    if task == nil then
        return
    end

    if not task.running then
        return
    end

    pcall(fiber.kill, task.fiber_id)

    space:put({task_id, false, task.fiber_id})
end

local function list()
    local space = get_space()

    local res = {}

    for _, tuple in space.index.running:pairs(true) do
        table.insert(res, tuple.task_id)
    end

    return res
end

return {
    init = init,
    start = start,
    stop = stop,
    list = list,
}
