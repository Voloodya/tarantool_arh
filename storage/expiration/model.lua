local module_name = 'storage.expiration.model'

local expiration_utils = require('storage.expiration.utils')

local vars = require('common.vars').new(module_name)

vars:new('expiration', nil)

local function strategy_to_delete_mode(strategy)
    if strategy == 'permanent' then
        return 'default'
    else
        return 'expiration'
    end
end

local function get_expiration_params(type_name)
    return vars.expiration and vars.expiration[type_name]
end

local function get_strategy(type_name)
    local expiration = get_expiration_params(type_name)
    return expiration and expiration.strategy
end

local function get_keep_version_count(type_name)
    local expiration = get_expiration_params(type_name)
    return expiration_utils.get_version_count(expiration)
end

local function get_lifetime_nsec(type_name)
    local expiration = get_expiration_params(type_name)
    return expiration_utils.get_lifetime_nsec(expiration)
end

local function get_delete_mode(type_name)
    local expiration = get_expiration_params(type_name)
    return expiration and expiration.delete_mode
end

local function apply_config(conf)
    -- FIXME: Remove expiration
    local cfg = conf['versioning'] or conf['expiration']
    if cfg == nil then
        return
    end

    local expiration = {}
    for _, expire_entry in ipairs(cfg) do
        if expire_entry.enabled == true then
            local strategy
            if expire_entry.strategy == nil then
                strategy = 'permanent'
            else
                strategy = expire_entry.strategy
            end

            expiration[expire_entry.type] = {
                enabled = expire_entry.enabled,
                strategy = strategy,
                keep_version_count = expire_entry.keep_version_count,
                delete_mode = strategy_to_delete_mode(strategy),
                lifetime_hours = expire_entry.lifetime_hours,
            }
        end
    end
    vars.expiration = expiration
end

return {
    apply_config = apply_config,

    strategy_to_delete_mode = strategy_to_delete_mode,

    get_expiration_params = get_expiration_params,

    get_strategy = get_strategy,
    get_keep_version_count = get_keep_version_count,
    get_lifetime_nsec = get_lifetime_nsec,
    get_delete_mode = get_delete_mode,
}
