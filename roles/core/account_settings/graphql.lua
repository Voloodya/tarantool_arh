local module_name = 'roles.core.account_settings.graphql'

local cartridge = require('cartridge')
local types = require('graphql.types')

local log = require('log.log').new(module_name)

local graphql = require('common.graphql')

local function get(_, args)
    local key = args.key
    if key == '' then
        return nil, 'key must be not empty'
    end

    local value, err = cartridge.rpc_call('core', 'account_settings_get', { key })
    if err ~= nil then
        log.error('Impossible to get settings value: %s', err)
        return args.default
    end
    if value == nil then
        return args.default
    end
    return value
end

local function put(_, args)
    local key = args.key
    if key == '' then
        return nil, 'key must be not empty'
    end

    return cartridge.rpc_call('core', 'account_settings_put', { key, args.value })
end

local function delete(_, args)
    local key = args.key
    if key == '' then
        return nil, 'key must be not empty'
    end

    return cartridge.rpc_call('core', 'account_settings_delete', { key })
end

local function init()
    graphql.add_callback_prefix('admin', 'settings', 'Account settings')
    graphql.add_mutation_prefix('admin', 'settings', 'Account settings')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'settings',
        name = 'get',
        callback = module_name .. '.get',
        kind = graphql.types.json,
        args = {
            key = types.string.nonNull,
            default = graphql.types.json,
        }
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'settings',
        name = 'put',
        callback = module_name .. '.put',
        kind = graphql.types.json,
        args = {
            key = types.string.nonNull,
            value = graphql.types.json,
        }
    })
    graphql.add_mutation({
        schema = 'admin',
        prefix = 'settings',
        name = 'delete',
        callback = module_name .. '.delete',
        kind = graphql.types.json,
        args = {
            key = types.string.nonNull,
        }
    })
end

return {
    get = get,
    put = put,
    delete = delete,

    init = init,
}
