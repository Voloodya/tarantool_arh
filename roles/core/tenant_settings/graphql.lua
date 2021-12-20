local module_name = 'roles.core.tenant_settings.graphql'

local cartridge = require('cartridge')
local types = require('graphql.types')

local log = require('log.log').new(module_name)

local graphql = require('common.graphql')

local function get(_, args)
    local key = args.key
    if key == '' then
        return nil, 'key must be not empty'
    end

    local value, err = cartridge.rpc_call('core', 'tenant_settings_get', { key })
    if err ~= nil then
        log.error('Impossible to get settings value: %s', err)
        return args.default
    end
    if value == nil then
        return args.default
    end
    return value
end

local function init()
    graphql.add_callback_prefix('admin', 'tenant_settings', 'Tenant settings')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'tenant_settings',
        name = 'get',
        callback = module_name .. '.get',
        kind = graphql.types.json,
        args = {
            key = types.string.nonNull,
            default = graphql.types.any_scalar,
        }
    })
end

return {
    get = get,

    init = init,
}
