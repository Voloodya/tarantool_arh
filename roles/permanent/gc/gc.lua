-- luacheck: ignore collectgarbage

local module_name = 'gc'

local fiber = require('fiber')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local config_filter = require('common.config_filter')

vars:new('forced', false)
vars:new('timeout', nil)
vars:new('worker', nil)
vars:new('period_sec', 2)
vars:new('steps', 20)

local function worker()
    while vars.forced do
        collectgarbage('step', vars.steps)
        vars.timeout:wait(vars.period_sec)
    end
end

local function stop_worker()
    vars.forced = false
    vars.timeout:signal()
    vars.worker = nil
end

local function apply_config(conf)
    -- just to update config segment in cache
    local _, err = config_filter.compare_and_set(conf, 'gc')
    if err ~= nil then
        return true
    end

    local gc = conf.gc
    if gc == nil or gc.forced == false then
        if vars.forced then
            log.info('Disabling forced garbage collection')
            stop_worker()
        end
        return
    end

    if vars.period_sec ~= gc.period_sec then
        vars.period_sec = gc.period_sec
        log.info('Garbage collection period changed to %f', vars.period_sec)
        if vars.worker then
            vars.timeout:signal()
        end
    end

    if vars.steps ~= gc.steps then
        vars.steps = gc.steps
        log.info('Garbage collection steps changed to %d', vars.steps)
    end

    if vars.worker == nil then
        log.info('Enabling forced garbage connection')
        vars.forced = true
        vars.timeout = fiber.cond()
        vars.worker = fiber.create(worker)
    end
end

return {
    apply_config = apply_config
}
