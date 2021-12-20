local module_name = 'storage.maintenance.space_deleter'

local cartridge = require('cartridge')
local errors = require('errors')
local tenant = require('common.tenant')
local defaults = require('common.defaults')
local audit_log = require('audit.log').new(module_name)

local space_deleter_error = errors.new_class('space_deleter_error')


local function unlinked_space_list()
    local mdl, err = tenant.get_mdl()
    if err ~= nil then
        return nil, err
    end
    if mdl == nil then
        mdl = {}
    end

    local ddl, err = tenant.get_ddl()
    if err ~= nil then
        return nil, err
    end
    if ddl == nil then
        ddl = {}
    end

    local types_with_spaces = {}
    for _, model_type in ipairs(mdl) do
        if model_type.indexes ~= nil then
            types_with_spaces[model_type.name] = true
        end
    end

    local res = {}
    for name, _ in pairs(ddl) do
        if not types_with_spaces[name] then
            table.insert(res, name)
        end
    end

    return res
end

local function drop_unlinked_spaces(args)
    local approved_list, err = unlinked_space_list()
    if err ~= nil then
        return nil, err
    end

    local res = {}
    for _, name in ipairs(args.names or {}) do
        local found = false
        for _, approved in ipairs(approved_list) do
            if name == approved then
                found = true
                break
            end
        end

        if found  then
            res[name] = true
        else
            return nil, space_deleter_error:new('Can not delete space %s', name)
        end
    end

    local ddl, err = tenant.get_cfg_deepcopy('ddl')
    if err ~= nil then
        return nil, err
    end

    for _, name in ipairs(args.names or {}) do
        ddl[name] = nil
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local _, err = vshard.router.map_callrw('vshard_proxy.delete_spaces', {res}, {timeout = timeout})
    if err ~= nil then
        return nil, err
    end

    local _, err = tenant.patch_config_with_ddl({['ddl'] = ddl})
    if err ~= nil then
        return nil, err
    end

    return args.names
end

local function truncate_unlinked_spaces(args)
    local approved_list, err = unlinked_space_list()
    if err ~= nil then
        return nil, err
    end

    local res = {}
    for _, name in ipairs(args.names or {}) do
        local found = false
        for _, approved in ipairs(approved_list) do
            if name == approved then
                found = true
                break
            end
        end

        if found  then
            res[name] = true
        else
            return nil, space_deleter_error:new('Can not truncate space %s', name)
        end
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local _, err = vshard.router.map_callrw('vshard_proxy.truncate_spaces', {res}, {timeout = timeout})
    if err ~= nil then
        return nil, err
    end

    return args.names
end

local function clear_data()
    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local _, err = vshard.router.map_callrw('vshard_proxy.clear_data', {}, {timeout = timeout})
    if err ~= nil then
        return nil, err
    end

    return "ok"
end

local function drop_spaces()
    audit_log.warn('Notice! Removing all data spaces is dangerous operation and may cause various issues!')
    -- Map of types for vshard_proxy.delete_spaces
    local types_to_delete = {}

    local ddl, err = tenant.get_ddl()
    if err ~= nil then
        return nil, err
    end

    -- Get all data types from ddl
    for _, type in pairs(ddl) do
        types_to_delete[type.type_name] = true
    end

    -- Call vshard_proxy.delete_spaces for all data types
    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local _, err = vshard.router.map_callrw('vshard_proxy.delete_spaces', {types_to_delete}, {timeout = timeout})
    if err ~= nil then
        return nil, err
    end

    -- Drop model and ddl
    local _, err = tenant.patch_config_with_ddl({
        ['model.avsc'] = "",
        types = {__file = "model.avsc"},
        ['ddl'] = {}
    })
    if err ~= nil then
        return nil, err
    end

    return "ok"
end

return {
    unlinked_space_list = unlinked_space_list,
    drop_unlinked_spaces = drop_unlinked_spaces,
    truncate_unlinked_spaces = truncate_unlinked_spaces,
    clear_data = clear_data,
    drop_spaces = drop_spaces,
}
