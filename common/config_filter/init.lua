local module_name = 'common.config_filter'

local log = require('log')
local checks = require('checks')
local utils = require('common.utils')

local vars = require('common.vars').new(module_name)
vars:new('prev_config')

-- for tests purposes
local function get_prev_config_hashs(module, section)
    local res = vars.prev_config
    if module ~= nil then
        res = vars.prev_config[module]
        if section ~= nil and res ~= nil then
            res = res[section]
        end
    end
    return res
end

local function get_updated(config, section, module, update_in_cache)
    checks("table", "string", "?string", "boolean")
    module = module or '__default'
    local hash
    if config[section] ~= nil then
        hash = utils.calc_hash(config[section])
    end
    if vars.prev_config == nil then
        vars.prev_config = {}
    end
    if vars.prev_config[module] == nil then
        vars.prev_config[module] = {}
    end

    if vars.prev_config[module][section] == hash then
        log.debug('no updates in config for section [%s] ', section)
        return nil, 'no updates in config for section ' .. section
    end

    if update_in_cache == true then
        vars.prev_config[module][section] = hash
        log.debug('config for section [%s] has been updated for module [%s]', section, module)
    end

    return config[section]
end

local function compare_and_get(config, section, module)
    return get_updated(config, section, module, false)
end

local function compare_and_set(config, section, module)
    return get_updated(config, section, module, true)
end

return {
    compare_and_get = compare_and_get,
    compare_and_set = compare_and_set,
    -- for tests
    get_prev_config_hashs = get_prev_config_hashs,
}
