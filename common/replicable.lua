local module_name = 'common.replicable'
local checks = require('checks')
local config_filter = require('common.config_filter')
local vars = require('common.vars').new(module_name)

vars:new('replicable')

local function apply_config(config)
    checks('table')
    local conf, err = config_filter.compare_and_set(config, 'output_processor', module_name)
    if err ~= nil then
        return true
    end
    vars.replicable = table.deepcopy(conf or {})
end

local function is_replicable(name)
    checks('string')
    local replicable = vars.replicable
    if replicable == nil then
        return false
    end
    return replicable[name] ~= nil
end

local function get_properties(name)
    checks('string')
    local replicable = vars.replicable
    if replicable == nil then
        return nil
    end
    return replicable[name]
end

return {
    apply_config = apply_config,
    is_replicable = is_replicable,
    get_properties = get_properties,
}
