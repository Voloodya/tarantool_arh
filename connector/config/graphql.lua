local types = require('graphql.types')
local graphql = require('common.graphql')
local config_inputs = require('connector.config.inputs')

local inputs_type = types.object {
    name = 'Inputs',
    fields = {
        name = types.string,
        type = types.string,
        options = types.string,
    }
}

local function graphql_inputs(_, _)
    return config_inputs.get_config_inputs()
end

local function graphql_update_input(_, args)
    local _, err = config_inputs.update_config_input(args.name, args.type, args.options, args.newName)
    if err ~= nil then
        return nil, err
    end

    args.name = args.newName
    args.newName = nil
    return args
end

local function graphql_delete_input(_, args)
    local _, err = config_inputs.delete_config_input(args.name)
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function graphql_add_input(_, args)
    local _, err = config_inputs.add_config_input(args.name, args.type, args.options)
    if err ~= nil then
        return nil, err
    end

    return args
end

local function init()
    graphql.add_callback({
        schema = 'admin',
        prefix = 'config',
        name = 'inputs',
        callback = 'connector.config.graphql.graphql_inputs',
        kind = types.list(inputs_type),
        args = {},
        doc = "Get connector inputs config",
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'config',
        name = 'input_update',
        callback = 'connector.config.graphql.graphql_update_input',
        kind = inputs_type,
        args = {
            name = types.string.nonNull,
            newName = types.string,
            type = types.string.nonNull,
            options = types.string,
        },
        doc = "Update connector input config",
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'config',
        name = 'input_delete',
        callback = 'connector.config.graphql.graphql_delete_input',
        kind = types.string.nonNull,
        args = { name = types.string.nonNull },
        doc = "Delete connector input config",
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'config',
        name = 'input_add',
        callback = 'connector.config.graphql.graphql_add_input',
        kind = inputs_type,
        args = {
            name = types.string.nonNull,
            type = types.string.nonNull,
            options = types.string,
        },
        doc = "Add connector input config",
    })
end

return {
    init = init,

    graphql_inputs = graphql_inputs,
    graphql_update_input = graphql_update_input,
    graphql_delete_input = graphql_delete_input,
    graphql_add_input = graphql_add_input,
}
