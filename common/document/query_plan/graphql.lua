local vars = require('common.vars').new('common.document.query_plan.graphql')
local types = require('graphql.types')
local common_graphql = require('common.graphql')

vars:new_global('schema')

local function init()
    if vars.schema ~= nil then
        return vars.schema
    end

    local filter_schema = types.object{
        name = 'FilterConditionSchema',
        fields = {
            name = types.string,
            comparator = types.string,
            values = types.list(common_graphql.types.any_scalar),
        },
        description = 'Filter conditions',
        schema = '__global__',
    }

    local replicaset_schema = types.object{
        name = 'ReplicasetStatSchema',
        fields = {
            uuid = types.string.nonNull,
            primary_space_scanned = types.long.nonNull,
            history_space_scanned = types.long.nonNull,
            primary_space_returned = types.long.nonNull,
            history_space_returned = types.long.nonNull,
        },
        description = 'Replicaset stats',
        schema = '__global__',
    }

    vars.schema = types.object{
        name = 'QueryPlanSchema',
        fields = {
            replicasets = types.list(replicaset_schema),
            first = types.long,
            scan_index = types.string,
            -- GraphQL has problems with huge numbers
            -- E.g. we can't return "-1ULL"
            version = types.string,
            versioned = types.boolean,
            iterator = types.string,
            scan_key = types.list(common_graphql.types.any_scalar),
            filters = types.list(filter_schema),
            range_scan = types.boolean,
            text = types.string,
        },
        description = 'Query plan',
        schema = '__global__',
    }

    return vars.schema
end

return {
    init = init,
}
