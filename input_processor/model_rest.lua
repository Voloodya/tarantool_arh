local module_name = 'common.model_rest'

local json = require('json')
local http_lib = require('http.lib')
local decimal = require('decimal')
local uuid = require('uuid')
local cartridge = require('cartridge')
local clock = require('clock')
local request_context = require('common.request_context')
local metrics_rest = require('common.metrics.instruments.rest')
local model = require('common.model')
local model_walker = require('common.model.walker')
local query_plan = require('common.document.query_plan')
local tenant = require('common.tenant')
local utils = require('common.utils')
local vars = require('common.vars').new(module_name)

vars:new('initialized')
vars:new('type_filter_resolve')
vars:new('types')

local function toboolean(value)
    value = value:lower()

    if value == 'true' or value == '1' then
        return true
    elseif value == 'false' or value == '0' then
        return false
    end
    return nil
end

local system_filters = {
    first = true,
    after = true,
    version = true,
    only_if_version = true,
    all_versions = true,
}

local function uri_unescape(str)
    local res = string.gsub(str, '%%([0-9a-fA-F][0-9a-fA-F])',
        function(c)
            return string.char(tonumber(c, 16))
        end
    )
    return res
end

-- Parse queries by hand in order to support arrays of values
-- E.g. "%2C,%2C" is [",", ","] but not ["", "", "", ""]
local function parse_query(req)
    if req.query == nil and string.len(req.query) == 0 then
        return {}
    end

    local params = http_lib.params(req.query)
    local pres = {}
    for k, v in pairs(params) do
        k = uri_unescape(k)
        if system_filters[k] == true then
            pres[k] = uri_unescape(v)
        else
            local parts = v:split(',')
            for i, part in ipairs(parts) do
                parts[i] = uri_unescape(part)
            end
            pres[k] = parts
        end
    end
    return pres
end

local function extract_filters(type_name, query_params)
    local type_filter_resolve = vars.type_filter_resolve[type_name]

    local result = {}
    for query_param_name, query_param_value in pairs(query_params) do
        local get_filter_fn = type_filter_resolve[query_param_name]
        if get_filter_fn ~= nil then
            local query_part, err = get_filter_fn(query_param_value)
            if err ~= nil then
                return nil, err
            end
            table.insert(result, query_part)
        else
            if system_filters[query_param_name] == nil then
                return nil, ('Unknown query type %q'):format(query_param_name)
            end
        end
    end

    return result
end

local function make_options(query_params)
    local first = query_params['first']
    if first ~= nil then
        first = tonumber64(first)
        if first == nil then
            return nil, '"first" expected to be a number'
        end
    end

    local after = query_params['after']

    local version = query_params['version'] or request_context.get().version
    if version ~= nil then
        version = tonumber64(version)
        if version == nil then
            return nil, '"version" expected to be a number'
        end
    end

    local only_if_version = query_params['only_if_version']
    if only_if_version ~= nil then
        only_if_version = tonumber64(only_if_version)
        if only_if_version == nil then
            return nil, '"only_if_version" expected to be a number'
        end
    end

    local all_versions = query_params['all_versions']
    if all_versions ~= nil then
        all_versions = toboolean(all_versions)
        if all_versions == nil then
            return nil, '"all_versions" expected to be boolean (0/1/true/false)'
        end
    end

    return {
        first = first,
        after = after,
        version = version,
        only_if_version = only_if_version,
        all_versions = all_versions,
    }
end

local function make_context(query_params)
    local routing_key = query_params['routing_key']
    return {
        routing_key = routing_key,
    }
end

local function find(req, type_name)
    if vars.types[type_name] == nil then
        return {
            status = 404,
            body = json.encode({error = ('Type %q is not found'):format(type_name)}),
        }
    end

    local query_params = parse_query(req)

    local filters, err = extract_filters(type_name, query_params)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    local options, err = make_options(query_params)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    local repository = tenant.get_repository()
    local result, err, meta = repository:find(type_name, filters, options)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    if request_context.is_explain_enabled() then
        local explain_entry = query_plan.explain(type_name, filters, options, meta)
        for i = 1, #result do
            result[i].__query_plan = explain_entry
        end
    end

    return {
        status = 200,
        body = json.encode(result),
    }
end


local function insert(req, type_name)
    local ok, obj = pcall(json.decode, req:read())
    if not ok then
        return {
            status = 400,
            body = json.encode({error = obj}),
        }
    end

    if vars.types[type_name] == nil then
        return {
            status = 404,
            body = json.encode({error = ('Type %q is not found'):format(type_name)}),
        }
    end

    local query_params = parse_query(req)

    local options, err = make_options(query_params)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    local repository = tenant.get_repository()
    local resp, err = repository:put(type_name, obj, options)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    return {
        status = 200,
        body = json.encode(resp[1]),
    }
end

local function update(req, type_name)
    local ok, data = pcall(json.decode, req:read())
    if not ok then
        return {
            status = 400,
            body = json.encode({error = data}),
        }
    end

    if vars.types[type_name] == nil then
        return {
            status = 404,
            body = json.encode({error = ('Type %q is not found'):format(type_name)}),
        }
    end

    local query_params = parse_query(req)
    local options, err = make_options(query_params)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    local updates = {}
    for field, value in pairs(data) do
        table.insert(updates, {'set', field, value})
    end

    local context = make_context(query_params)

    local filters, err = extract_filters(type_name, query_params)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    local repository = tenant.get_repository()
    local resp, err = repository:update(type_name, filters, updates, options, context)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    return {
        status = 200,
        body = json.encode(resp),
    }
end

local function delete(req, type_name)
    if vars.types[type_name] == nil then
        return {
            status = 404,
            body = json.encode({error = ('Type %q is not found'):format(type_name)}),
        }
    end

    local query_params = parse_query(req)
    local options, err = make_options(query_params)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    local filters, err = extract_filters(type_name, query_params)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    local repository = tenant.get_repository()
    local resp, err = repository:delete(type_name, filters, options)
    if err ~= nil then
        return {
            status = 400,
            body = json.encode({error = err}),
        }
    end

    return {
        status = 200,
        body = json.encode(resp),
    }
end

local rest_handlers = {
    GET = find,
    POST = insert,
    PUT = update,
    DELETE = delete,
}

local NANOSECONDS_IN_MILLISECONDS = 1000000ULL

local function rest_handler(req)
    local method = req.method
    -- This method is supposed to make responses on requests described in rest_handlers.
    -- If req.method is different from these values it means there are huge bugs in code
    if rest_handlers[method] == nil then
        error(string.format('HTTP method %q is not supported', method))
    end

    local type_name = req:stash('type_name')
    local exec_start_time = clock.monotonic64()

    local response = rest_handlers[method](req, type_name)
    local exec_time = (clock.monotonic64() - exec_start_time) / NANOSECONDS_IN_MILLISECONDS

    metrics_rest.update_metrics(response.status, exec_time, {type = type_name, method = method})

    return response
end

local function init()
    if vars.initialized == true then
        return
    end

    local httpd = cartridge.service_get('httpd')
    for method, _ in pairs(rest_handlers) do
        httpd:route(
            { path = '/data/:type_name', method = method, public = false },
            rest_handler
        )
    end

    vars.initialized = true
end

local fileds_suffixes = {
    _like = "LIKE",
    _ilike = "ILIKE"
}

local index_suffixes = {
    [""] = "==",
    _gt  = ">",
    _ge  = ">=",
    _lt  = "<",
    _le  = "<=",
}

local cast_type = {
    string = tostring,
    byte = tostring,
    boolean = toboolean,
    long = tonumber64,
    int = tonumber64,
    float = tonumber,
    double = tonumber,
    decimal = function(value)
        local ok, result = pcall(decimal.new, value)
        if not ok then
            return nil
        end
        return result
    end,
    uuid = uuid.fromstr,
    datetime = utils.iso8601_str_to_nsec,
    date = utils.date_str_to_nsec,
    time = utils.time_str_to_nsec,
    enum = tostring,
}

local function get_string_field_paths(type_entry)
    local string_field_paths = {}
    local collect_string_paths = model_walker.new(model_walker.build_callbacks({
        before_string = function (_, ctx)
            table.insert(string_field_paths, table.concat(ctx.path, '.'))
        end,
        before_array = function (_, _)
            return false
        end
    }, {with_path_decorators = true}))
    collect_string_paths(type_entry, {path = {}})
    return string_field_paths
end

local function resolve_filters(type_entry)
    local indexes = type_entry.indexes
    if indexes == nil then
        return
    end

    local filter_resolvers = {}

    for _, index in ipairs(indexes) do
        local part_types_cast = {}
        for _, part in ipairs(index.parts) do
            local field_type, err = model.get_field_type(type_entry, part)
            if err ~= nil then
                return nil, err
            end

            if field_type.type == 'array' then
                field_type = field_type.items
            end

            if field_type.type ~= nil then
                if field_type.logicalType == 'Decimal' then
                    field_type = 'decimal'
                elseif field_type.logicalType == 'DateTime' then
                    field_type = 'datetime'
                elseif field_type.logicalType == 'Date' then
                    field_type = 'date'
                elseif field_type.logicalType == 'Time' then
                    field_type = 'time'
                elseif field_type.logicalType == 'UUID' then
                    field_type = 'uuid'
                else
                    field_type = field_type.type
                end
            end

            local cast_fn = cast_type[field_type]
            if cast_fn == nil then
                return nil, ('Unknown type %q'):format(field_type)
            end
            table.insert(part_types_cast, cast_fn)
        end

        for suffix, operation in pairs(index_suffixes) do
            local index_name =  index.name
            local index_with_suffix = index_name .. suffix

            filter_resolvers[index_with_suffix] = function(query_parts)
                for i, query_part in ipairs(query_parts) do
                    if query_part == 'null' then
                        query_parts[i] = box.NULL
                    else
                        local cast_fn = part_types_cast[i]
                        if cast_fn == nil then
                            return nil, ('Values are out of range index length. ' ..
                                'Expected %d, got %d'):format(#part_types_cast, #query_parts)
                        end

                        query_part = cast_fn(query_part)
                        if query_part == nil then
                            return nil, ('Impossible cast part %d of query %q'):format(i, index_with_suffix)
                        end
                        query_parts[i] = query_part
                    end
                end
                return {index_name, operation, query_parts}
            end
        end
    end

    for _, field_path in ipairs(get_string_field_paths(type_entry)) do
        for suffix, operation in pairs(fileds_suffixes) do
            local field_name =  field_path:gsub('%.', '_')
            local field_with_suffix = field_name .. suffix

            filter_resolvers[field_with_suffix] = function(query_part)
                return {field_path, operation, tostring(query_part[1])}
            end
        end
    end

    for _, field in ipairs(type_entry.fields) do
        if model.is_string(field.type) then
            for suffix, operation in pairs(fileds_suffixes) do
                local field_name =  field.name
                local field_with_suffix = field_name .. suffix

                filter_resolvers[field_with_suffix] = function(query_part)
                    return {field_name, operation, tostring(query_part[1])}
                end
            end
        end
    end

    return filter_resolvers
end

local function apply_config(mdl)
    vars.types = {}
    vars.type_filter_resolve = {}
    local err
    for _, type in ipairs(mdl) do
        vars.types[type.name] = type
        vars.type_filter_resolve[type.name], err = resolve_filters(type)
        if err ~= nil then
            error(err)
        end
    end
end

return {
    init = init,
    apply_config = apply_config,
}
