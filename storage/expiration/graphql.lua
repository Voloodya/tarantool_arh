local cartridge = require('cartridge')
local cron = require('common.cron')
local defaults = require('common.defaults')
local graphql = require('common.graphql')
local types = require('graphql.types')

local function cleanup(_, args)
    local type_name = args.type

    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local _, err = vshard.router.map_callrw(
        'vshard_proxy.remove_old_versions_by_type',
        {type_name},
        {timeout = timeout}
    )
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function cron_syntax(_, args)
    local expr, err = cron.validate(args.expr)
    if not expr then
        return {
            is_valid = false,
            message = err,
        }
    end

    return {is_valid = true}
end

local function storage_dir_writable(_, args)
    local replicaset, err = vshard.router.routeall()
    if err ~= nil then
        return nil, err
    end

    for _, replica in pairs(replicaset) do
        local res, err = replica:callrw('vshard_proxy.is_dir_writable', {args.path})
        if err ~= nil then
            return nil, 'Replicaset ' .. replica.uuid .. ': ' .. err
        end
        if not res.is_writeable then
            return {
                is_writeable = false,
                message = 'Replicaset ' .. replica.uuid .. ': ' .. res.message
            }
        end
    end

    return {is_writeable = true}
end

local function init()
    graphql.add_mutation({
        schema = 'admin',
        name = 'expiration_cleanup',
        callback = 'storage.expiration.graphql.cleanup',
        kind = types.string.nonNull,
        args = {
            type = types.string.nonNull,
        }
    })

    graphql.add_callback_prefix('admin', 'checks', 'Validator for different data')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'checks',
        name = 'cron_syntax',
        callback = 'storage.expiration.graphql.cron_syntax',
        args = {
            expr = types.string.nonNull,
        },
        kind = types.object({
            name = 'cron_syntax_result',
            description = 'Result of cron syntax check',
            fields = {
                is_valid = types.boolean.nonNull,
                message = types.string,
            }
        }),
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'checks',
        name = 'storage_dir_writable',
        callback = 'storage.expiration.graphql.storage_dir_writable',
        args = {
            path = types.string.nonNull,
        },
        kind = types.object({
            name = 'storage_dir_writable_result',
            description = 'Result of dir writable check',
            fields = {
                is_writeable = types.boolean.nonNull,
                message = types.string,
            }
        }),
    })
end

return {
    cleanup = cleanup,
    init = init,
    cron_syntax = cron_syntax,
    storage_dir_writable = storage_dir_writable,
}
