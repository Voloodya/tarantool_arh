local module_name = 'scheduler.server'
local vars = require('common.vars').new(module_name)

local log = require('log.log').new(module_name)
local scheduler = require('tasks.scheduler.scheduler')

vars:new('master', false)

local function apply_config(cfg, opts)
    if vars.master == true then
        if opts.is_master ~= true then
            -- shutdown system
            scheduler.deinit()
            vars.master = false
        end
    else
        if opts.is_master == true then
            scheduler.init()
            vars.master = true
            log.info('Scheduler changed')
        end
    end

    if vars.master == true then
        scheduler.apply_config(cfg)
    end
end

return {
    apply_config = apply_config,
}
