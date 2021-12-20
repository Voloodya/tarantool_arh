local module_name = 'common.repair_queue.graphql' -- luacheck: ignore

local json = require('json')

local cartridge = require('cartridge')
local types = require('graphql.types')
local graphql = require('common.graphql')
local utils = require('common.utils')

local status = require('input_processor.repair_queue.statuses')

local function objToGraphql(obj)
    return {
        id = obj.id,
        time = utils.nsec_to_iso8601_str(obj.time),
        status = status.to_string(obj.status),
        reason = obj.reason,
        object = json.encode(obj.object),
        cursor = obj.cursor }
end

local function get_repair_list(_, args)
    if args.id then
        local obj, err = cartridge.rpc_call('runner', 'repair_queue_get',
                                          {args.id})
        if not obj and err then
            return nil, err
        end
        return obj and { objToGraphql(obj) } or {}
    end

    local result = {}

    local from = nil
    if args.from then
        from = utils.iso8601_str_to_nsec(args.from)
    end

    local to = nil
    if args.to then
        to = utils.iso8601_str_to_nsec(args.to)
    end

    local objects, err = cartridge.rpc_call('runner',
                                          'repair_queue_filter',
                                          {from, to, args.reason, args.first, args.after})

    if err ~= nil then
        error(err)
    end

    for _, obj in pairs(objects) do
        table.insert(result, objToGraphql(obj))
    end
    return result
end

local function delete_from_repair_queue(_, args)
    return cartridge.rpc_call('runner', 'repair_queue_delete',
                            {args.id},
                            {leader_only=true})
end

local function clear_repair_queue()
    return cartridge.rpc_call('runner', 'repair_queue_clear', {},
                            {leader_only=true})
end

local function try_again(_, args)
    return cartridge.rpc_call('runner', 'repair_queue_try_again',
                                 {args.id},
                                 {leader_only=true})
end

local function try_again_all()
    return cartridge.rpc_call('runner', 'repair_queue_try_all',
                            {},
                            {leader_only=true})
end

local function init()
    types.object {
        name = 'Repair_list',
        description = 'A list of objects into a repair queue',
        fields = {
            id = types.string.nonNull,
            time = types.string.nonNull,
            status = types.string.nonNull,
            reason = types.string.nonNull,
            object = types.string.nonNull,
            cursor = types.string.nonNull
        },
        schema = 'admin',
    }

    graphql.add_callback({
            schema = 'admin',
            name = 'repair_list',
            doc = 'Get repair list',
            args = {
                id = types.string,
                from = types.string,
                to = types.string,
                reason = types.string,
                first = types.long,
                after = types.string
            },
            kind = types.list('Repair_list'),
            callback = 'input_processor.repair_queue.graphql.get_repair_list',
    })

    graphql.add_mutation({
            schema = 'admin',
            name = 'delete_from_repair_queue',
            doc = 'Get repair list',
            args = {
                id = types.string.nonNull,
            },
            kind = types.string.nonNull,
            callback = 'input_processor.repair_queue.graphql.delete_from_repair_queue',
    })


    graphql.add_mutation(
        {schema = 'admin',
        name='clear_repair_queue',
        callback='input_processor.repair_queue.graphql.clear_repair_queue',
        kind=types.string.nonNull})

    graphql.add_mutation(
        {schema = 'admin',
         name='repair',
         callback='input_processor.repair_queue.graphql.try_again',
         kind=types.string.nonNull,
         args = { id = types.string.nonNull }})

    graphql.add_mutation(
        {schema = 'admin',
         name='repair_all',
         callback='input_processor.repair_queue.graphql.try_again_all',
         kind=types.string.nonNull})
end

return {
    get_repair_list = get_repair_list,
    delete_from_repair_queue = delete_from_repair_queue,
    clear_repair_queue = clear_repair_queue,
    try_again = try_again,
    try_again_all = try_again_all,
    init = init
}
