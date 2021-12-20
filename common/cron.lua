local module_name = 'common.cron'

local fiber = require('fiber')
local clock = require('clock')

local log = require('log.log').new(module_name)

local checks = require('checks')
local errors = require('errors')
local cron = require('cron')

local vars = require('common.vars').new(module_name)
local tenant = require('common.tenant')

local funcall = require('common.funcall')

local cron_parser_error = errors.new_class("cron_parser_error")

vars:new('cron_task_registry')
vars:new('cron_fiber')
vars:new('cron_wakeup')
vars:new('cron_queue')

local WARN_IF_DELAY_SEC = 1

local function validate(expression)
    checks('string')
    local cron_struct, err = cron_parser_error:pcall(cron.parse, expression)
    if err ~= nil then
        return nil, err
    end
    return cron_struct
end

local function add(name, expression, function_name, args)
    checks('string', 'string', 'string', '?table')

    args = args or {}

    local cron_struct, err = validate(expression)
    if err ~= nil then
        log.error('Cron parsing error: %q', err)
        return nil, err
    end

    local next_time = cron.next(cron_struct)

    local newtask = {
        name = name,
        expression = expression,
        cron_struct = cron_struct,
        next_time = next_time,
        function_name = function_name,
        args = args,
    }

    vars.cron_task_registry[name] = newtask

    local newpos = 1
    for pos, task in ipairs(vars.cron_queue) do
        if task.next_time >= next_time then
            newpos = pos
            break
        end
    end
    table.insert(vars.cron_queue, newpos, newtask)
    if vars.cron_wakeup:has_readers() then
        vars.cron_wakeup:put(0)
    end

    return true
end

local function list()
    return vars.cron_task_registry
end

local function remove(name)
    checks('string')

    for pos, task in ipairs(vars.cron_queue) do
        if task.name == name then
            if task.fiber ~= nil and task.fiber:status() == 'suspended' then
                task.fiber:cancel()
            end
            table.remove(vars.cron_queue, pos)
            break
        end
    end

    vars.cron_task_registry[name] = nil
end

local function call(name, function_name, ...)
    local _, err = funcall.call(function_name, ...)
    if err ~= nil then
        log.warn('Task %q exit with error %q', name, err)
    end
end

local function cron_fiber()
    log.info('Cron fiber started')

    while true do

        local now = clock.time()

        for _, task in ipairs(vars.cron_queue) do
            if now >= task.next_time then
                task.next_time = cron.next(task.cron_struct)

                if task.fiber ~= nil
                and task.fiber:status() == 'suspended' then
                    log.warn('Task %q still running while cron triggers', task.name)
                else
                    if now - WARN_IF_DELAY_SEC > task.next_time then
                        log.warn('Cron triggers %q with delay %q',
                                 task.name, now - task.next_time)
                    end

                    task.fiber = tenant.fiber_new(
                        call,
                        task.name,
                        task.function_name,
                        unpack(task.args, 1, table.maxn(task.args)))
                    task.fiber:name('cron:' .. task.name, { truncate = true })
                end
            else
                break
            end
        end

        table.sort(vars.cron_queue,
                   function(left, right)
                       return left.next_time < right.next_time
                   end)

        local diff = 1
        if #vars.cron_queue > 0 then
            diff = vars.cron_queue[1].next_time - clock.time()
            if diff < 0 then
                diff = 0
            end
        end

        vars.cron_wakeup:get(diff)
    end
end

local function init()
    vars.cron_task_registry = vars.cron_task_registry or {}
    vars.cron_wakeup = vars.cron_wakeup or fiber.channel()
    vars.cron_queue = vars.cron_queue or {}
    if vars.cron_fiber == nil then
        vars.cron_fiber = tenant.fiber_new(cron_fiber)
        vars.cron_fiber:name('cron')
    end
end

local function deinit()
    if vars.cron_fiber ~= nil then
        vars.cron_fiber:cancel()
        vars.cron_fiber = nil
    end
end

return {
    add = add,
    list = list,
    remove = remove,
    validate = validate,

    init = init,
    deinit = deinit,
}
