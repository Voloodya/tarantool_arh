local json = require('json')

local cartridge = require('cartridge')
local graphql = require('common.graphql')
local types = require('graphql.types')
local utils = require('common.utils')
local status = require('output_processor.statuses')

local function objectToGraphql(object)
    local errors = setmetatable({}, {__serialize = 'map'})
    local to_send = object.object.to_send
    for id, data in pairs(to_send) do
        errors[id] = data.error
        to_send[id].error = nil
    end

    local id = object.id
    local time = utils.nsec_to_iso8601_str(object.time)
    local reason = object.reason
    local cursor = object.cursor
    local tuple = object.object.tuple
    local type_name = object.object.type_name

    return {
        id = id,
        time = time,
        status = status.to_string(object.status),
        reason = reason,
        object = json.encode({[type_name] = tuple}),
        cursor = cursor,
        errors = json.encode(errors),
    }
end

local function output_processor_list_get(_, args)
    if args.id then
        local obj, err = cartridge.rpc_call('runner', 'output_processor_list_get', {args.id})
        if obj == nil and err ~= nil then
            return nil, err
        end
        return obj and { objectToGraphql(obj) } or {}
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

    local objects, err = cartridge.rpc_call('runner', 'output_processor_list_filter',
                                          {from, to, args.reason, args.first, args.after})

    if err ~= nil then
        return nil, err
    end

    for _, obj in pairs(objects) do
        table.insert(result, objectToGraphql(obj))
    end

    return result
end

local function output_processor_list_delete(_, args)
    return cartridge.rpc_call('runner', 'output_processor_list_delete',
                            {args.id},
                            {leader_only=true})
end

local function output_processor_list_clear(_, _)
    return cartridge.rpc_call('runner', 'output_processor_list_clear',
                            {},
                            {leader_only=true})
end

local function postprocess_again(_, args)
    return cartridge.rpc_call('runner', 'postprocess_again',
                            {args.id},
                            {leader_only=true})
end

local function postprocess_again_all(_, _)
    return cartridge.rpc_call('runner', 'postprocess_again_all',
                            {},
                            {leader_only=true})
end

local function init()
    types.object {
        name = 'Output_Processor_list',
        description = 'A list of replicable objects',
        fields = {
            id = types.string.nonNull,
            time = types.string.nonNull,
            status = types.string.nonNull,
            reason = types.string.nonNull,
            object = types.string.nonNull,
            errors = types.string.nonNull,
            cursor = types.string.nonNull,
        },
        schema = 'admin',
    }

    graphql.add_mutation_prefix('admin', 'output_processor', 'Output_Processor management')
    graphql.add_callback_prefix('admin', 'output_processor', 'Output_Processor management')

    graphql.add_callback(
        {schema='admin',
         prefix='output_processor',
        name='get_list',
        callback='output_processor.graphql.output_processor_list_get',
        kind=types.list('Output_Processor_list'),
        args={
            id = types.string,
            from = types.string,
            to = types.string,
            reason = types.string,
            first = types.long,
            after = types.string
    }})

    graphql.add_mutation(
        {schema='admin',
         prefix='output_processor',
        name='delete_from_list',
        callback='output_processor.graphql.output_processor_list_delete',
        kind=types.string.nonNull,
        args={ id = types.string.nonNull }})

    graphql.add_mutation(
        {schema='admin',
         prefix='output_processor',
        name='clear_list',
        callback='output_processor.graphql.output_processor_list_clear',
        kind=types.string.nonNull})

    graphql.add_mutation(
        {schema='admin',
         prefix='output_processor',
        name='try_again',
        callback='output_processor.graphql.postprocess_again',
        kind=types.string.nonNull,
        args={ id = types.string.nonNull }})

    graphql.add_mutation(
        {schema='admin',
         prefix='output_processor',
        name='try_again_all',
        callback='output_processor.graphql.postprocess_again_all',
        kind=types.string.nonNull})
end

return {
    output_processor_list_get = output_processor_list_get,
    output_processor_list_delete = output_processor_list_delete,
    output_processor_list_clear = output_processor_list_clear,
    postprocess_again = postprocess_again,
    postprocess_again_all = postprocess_again_all,
    init = init,
}
