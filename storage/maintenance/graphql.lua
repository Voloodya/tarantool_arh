local env = require('env')
local cartridge = require('cartridge')
local graphql = require('common.graphql')
local types = require('graphql.types')
local space_deleter = require('storage.maintenance.space_deleter')

local function get_aggregates()
    return cartridge.rpc_call('storage', 'get_aggregates')
end

local function unlinked_space_list(_, _)
    return space_deleter.unlinked_space_list()
end

local function drop_unlinked_spaces(_, args)
    return space_deleter.drop_unlinked_spaces(args)
end

local function truncate_unlinked_spaces(_, args)
    return space_deleter.truncate_unlinked_spaces(args)
end

local function clear_data(_, _)
    return space_deleter.clear_data()
end

local function drop_spaces(_, _)
    return space_deleter.drop_spaces()
end

local function spaces_len()
    local res, err = vshard.router.map_callrw('vshard_proxy.get_spaces_len')
    if err ~= nil then
        return nil, err
    end

    local res_dict = {}
    for _, storage_res in pairs(res) do
        storage_res = storage_res[1]
        for space_name, len in pairs(storage_res) do
            if res_dict[space_name] == nil then
                res_dict[space_name] = 0
            end
            res_dict[space_name] = res_dict[space_name] + len
        end
    end

    local result = {}
    for space_name, len in pairs(res_dict) do
        table.insert(result, {
            space_name = space_name,
            len = len,
        })
    end
    return result
end

local function init()
    types.object {
        name = 'Aggregate_list',
        description = 'A list of aggregates',
        fields = {
            name = types.string.nonNull
        },
        schema = 'admin',
    }

    graphql.add_callback({
        schema = 'admin',
        prefix = 'maintenance',
        name = 'unlinked_space_list',
        doc = 'Get unlinked spaces',
        args = {},
        kind = types.list(types.string),
        callback = 'storage.maintenance.graphql.unlinked_space_list',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'maintenance',
        name = 'drop_unlinked_spaces',
        doc = 'Drop unlinked spaces',
        args = { names = types.list(types.string) },
        kind = types.list(types.string),
        callback = 'storage.maintenance.graphql.drop_unlinked_spaces',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'maintenance',
        name = 'truncate_unlinked_spaces',
        doc = 'Truncate unlinked spaces',
        args = { names = types.list(types.string) },
        kind = types.list(types.string),
        callback = 'storage.maintenance.graphql.truncate_unlinked_spaces',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'maintenance',
        name = 'get_aggregates',
        doc = 'Get aggregate list',
        args = {},
        kind = types.list('Aggregate_list'),
        callback = 'storage.maintenance.graphql.get_aggregates',
    })

    types.object({
        name = 'Space_len',
        description = 'Length of space',
        fields = {
            space_name = types.string.nonNull,
            len = types.long.nonNull,
        },
        schema = 'admin',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'maintenance',
        name = 'spaces_len',
        doc = 'Length of spaces on storages',
        args = {},
        kind = types.list('Space_len'),
        callback = 'storage.maintenance.graphql.spaces_len',
    })

    if env.dev_mode then
        graphql.add_mutation({
            schema = 'admin',
            prefix = 'maintenance',
            name = 'clear_data',
            doc = 'Truncate storage spaces',
            kind = types.string,
            callback = 'storage.maintenance.graphql.clear_data',
        })

        graphql.add_mutation({
            schema = 'admin',
            prefix = 'maintenance',
            name = 'drop_spaces',
            doc = 'Drop all type spaces',
            kind = types.string,
            callback = 'storage.maintenance.graphql.drop_spaces',
        })
    end
end

return {
    get_aggregates = get_aggregates,
    unlinked_space_list = unlinked_space_list,
    drop_unlinked_spaces = drop_unlinked_spaces,
    truncate_unlinked_spaces = truncate_unlinked_spaces,
    clear_data = clear_data,
    drop_spaces = drop_spaces,
    spaces_len = spaces_len,

    init = init,
}
