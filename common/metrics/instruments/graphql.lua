local metrics = require('metrics')

local buckets = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

local query_time = metrics.histogram(
    "tdg_graphql_query_time",
    "Graphql query execution time",
    buckets
)

local query_fail = metrics.counter(
    "tdg_graphql_query_fail",
    "Graphql query fail count"
)

local mutation_time = metrics.histogram(
    "tdg_graphql_mutation_time",
    "Graphql mutation execution time",
    buckets
)

local mutation_fail = metrics.counter(
    "tdg_graphql_mutation_fail",
    "Graphql mutation fail count"
)

local function get_selections_entity(obj, ent_list)
    for _, i in ipairs(obj.selectionSet.selections) do
        if i.name ~= nil and i.name.value ~= nil then
            table.insert(ent_list, i.name.value)
        end
    end
end

local function get_parsed_queries(query_set)
    local queries = {}

    for _, query in ipairs(query_set.definitions) do
        local operation = {
            type = query.operation,
            entities = {},
        }
        if query.name ~= nil then
            operation.name = query.name.value
        end
        get_selections_entity(query, operation.entities)
        table.insert(queries, operation)
    end
    return queries
end

local function write_query_time(time_delta, query_ast, query_scheme)
    if time_delta < 0 then
        time_delta = 0
    end
    local parsed_queries = get_parsed_queries(query_ast)
    for _, query in ipairs(parsed_queries) do
        for _, entity in ipairs(query.entities) do
            if query.type == "query" then
                query_time:observe(time_delta, {schema = query_scheme, entity = entity, operation_name = query.name})
            end
            if query.type == "mutation" then
                mutation_time:observe(time_delta, {schema = query_scheme, entity = entity, operation_name = query.name})
            end
        end
    end
end

local function write_fail(query_ast, query_scheme)
    local parsed_queries = get_parsed_queries(query_ast)
    for _, query in ipairs(parsed_queries) do
        for _, entity in ipairs(query.entities) do
            if query.type == "query" then
                query_fail:inc(1, {schema = query_scheme, entity = entity, operation_name = query.name})
            elseif query.type == "mutation" then
                mutation_fail:inc(1, {schema = query_scheme, entity = entity, operation_name = query.name})
            end
        end
    end
end

return {
    write_time = write_query_time,
    write_fail = write_fail,
}
