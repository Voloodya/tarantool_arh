local module_name = 'common.graphql'

local checks = require('checks')
local json = require('json')
local errors = require('errors')
local digest = require('digest')
local clock = require('clock')
local cartridge = require('cartridge')

local http = require('common.http')
local log = require('log.log').new(module_name)

local types = require('graphql.types')
local execute = require('graphql.execute')
local schema = require('graphql.schema')
local parse = require('graphql.parse')
local validate = require('graphql.validate')
local funcall = require('cartridge.funcall')

local vars = require('common.vars').new(module_name)
local sandbox_registry = require('common.sandbox.registry')
local request_context = require('common.request_context')
local tracing = require('common.tracing')
local metrics = require("common.metrics.instruments.graphql")
local defaults = require("common.defaults")
local lru_cache = require('common.lru_cache')
local tenant = require('common.tenant')

vars:new_global('graphql_schema', {})
vars:new_global('model', {})
vars:new_global('callbacks', {})
vars:new_global('mutations', {})
vars:new_global('graphql_cache_size', defaults.GRAPHQL_QUERY_CACHE_SIZE)
vars:new_global('graphql_cache', {})
vars:new_global('auth')
vars:new_global('on_resolve_trigger')

vars:new_global('shared_types', {
    -- [target_uid] = {
    --  [owner_uid] = { query_fields = {}, mutation_fields = {} }
    --}
})


local graphql_error = errors.new_class('graphql internal error')

local function cache_reset(new_size)
    new_size = new_size or vars.graphql_cache_size
        or defaults.GRAPHQL_QUERY_CACHE_SIZE

    vars.graphql_cache = {}
    vars.graphql_cache_size = new_size
end

local function cache_set(schema_name, key, item)
    if vars.graphql_cache_size <= 0 then
        return true
    end

    if vars.graphql_cache[schema_name] == nil then
        vars.graphql_cache[schema_name] = lru_cache.new(vars.graphql_cache_size)
    end

    return vars.graphql_cache[schema_name]:set(key, item)
end

local function cache_get(schema_name, key)
    if vars.graphql_cache_size <= 0 then
        return nil
    end

    if vars.graphql_cache[schema_name] == nil then
        return nil
    end

    return vars.graphql_cache[schema_name]:get(key)
end

local function find_type(mdl, name)
    for _, mdl_type in ipairs(mdl) do
        if mdl_type.name == name then
            return table.deepcopy(mdl_type)
        end
    end
end

local function set_shared_types(mdl, cfg)
    -- Drop cache
    cache_reset()

    local owner_uid = tenant.uid()
    for target_uid, tenant_shared_types in pairs(vars.shared_types) do
        if tenant_shared_types[owner_uid] ~= nil then
            local query_fields = tenant_shared_types[owner_uid].query_fields or {}
            for export_name in pairs(query_fields) do
                vars.model[target_uid].query_fields[export_name] = nil
            end

            local mutation_fields = tenant_shared_types[owner_uid].mutation_fields or {}
            for export_name in pairs(mutation_fields) do
                vars.model[target_uid].mutation_fields[export_name] = nil
            end

            tenant_shared_types[owner_uid] = nil
            vars.graphql_schema[target_uid] = nil
        end
    end

    if cfg == nil then
        return
    end

    local model_graphql = require('common.model_graphql')
    for type_name, type_options in pairs(cfg) do
        local query_prefix = type_options.query_prefix
        local shared_type = find_type(mdl, type_name)
        local export_name = query_prefix .. type_name

        for target_uid, access_rights in pairs(type_options.tenants) do
            local values = model_graphql.model_to_graphql({shared_type}, target_uid, query_prefix)
            vars.shared_types[target_uid] = vars.shared_types[target_uid] or {}
            local target_owner_shared_types = {query_fields = {}, mutation_fields = {}}

            vars.model[target_uid] = vars.model[target_uid] or {}
            if access_rights.read == true then
                local query = values.query_fields[type_name]
                target_owner_shared_types.query_fields[export_name] = query
                vars.model[target_uid].query_fields = vars.model[target_uid].query_fields or {}
                vars.model[target_uid].query_fields[export_name] = query
            end

            if access_rights.write == true then
                local mutation = values.mutation_fields[type_name]
                target_owner_shared_types.mutation_fields[export_name] = mutation
                vars.model[target_uid].mutation_fields = vars.model[target_uid].mutation_fields or {}
                vars.model[target_uid].mutation_fields[export_name] = mutation
            end

            vars.shared_types[target_uid][owner_uid] = target_owner_shared_types
            vars.graphql_schema[target_uid] = nil
        end
    end
end

local function set_model(schema_name, query_fields, mutation_fields)
    checks("string", "table", "table")

    vars.model[schema_name] = {
        query_fields = query_fields,
        mutation_fields = mutation_fields,
    }

    local tenant_uid = tenant.uid()

    for _, tenant_shared_types in pairs(vars.shared_types[tenant_uid] or {}) do
        for export_name, export_type in pairs(tenant_shared_types.query_fields or {}) do
            vars.model[schema_name].query_fields[export_name] = export_type
        end

        for export_name, export_type in pairs(tenant_shared_types.mutation_fields or {}) do
            vars.model[schema_name].mutation_fields[export_name] = export_type
        end
    end

    vars.graphql_schema[schema_name] = nil
    cache_reset()
end

local function funcall_wrap(fun_name, operation, field_name, schema)
    return function(...)
        local trigger = vars.on_resolve_trigger
        if trigger ~= nil then
            trigger(operation, field_name, schema)
        end

        local res, err = funcall.call(fun_name, ...)
        if err ~= nil then
            error(err)
        end

        return res
    end
end

local function sandbox_call_wrap(fun_name, auth_callback)
    local sandbox = sandbox_registry.get('active')

    return function(...)
        if auth_callback ~= nil then
            local _, err = auth_callback()
            if err ~= nil then
                return nil, err
            end
        end
        local _, args = ...
        local span = tracing.start_span('sandbox.call_by_name: %s', fun_name)
        local res, err = sandbox:call_by_name(fun_name, args)
        span:finish({error = err})
        return res, err
    end
end

local function add_callback(opts)
    checks({
            schema = '?string',
            prefix = '?string',
            name = 'string',
            doc = '?string',
            args = '?table',
            kind = 'table|string',
            callback = 'string',
    })

    if opts.schema == nil then
        opts.schema = 'default'
    end

    vars.callbacks[opts.schema] = vars.callbacks[opts.schema] or {}

    if opts.prefix then
        local obj = vars.callbacks[opts.schema][opts.prefix]
        if obj == nil then
            error('No such callback prefix ' .. opts.prefix)
        end

        local oldkind = obj.kind
        oldkind.fields[opts.name] = {
            kind = opts.kind,
            arguments = opts.args,
            resolve = funcall_wrap(opts.callback, 'query', opts.prefix .. '.' .. opts.name, opts.schema),
            description = opts.doc,
        }

        obj.kind = types.object{
            name = oldkind.name,
            fields = oldkind.fields,
            description = oldkind.description,
        }
    else
        local callback = {
            kind = opts.kind,
            arguments = opts.args,
            resolve = funcall_wrap(opts.callback, 'query', opts.name, opts.schema),
            description = opts.doc,
        }

        vars.callbacks[opts.schema][opts.name] = callback
    end
    vars.graphql_schema[opts.schema] = nil
end

local function add_function_callback(opts)
    checks({
            schema="string",
            name="string",
            callback="string",
            kind="?table|?string",
            args="?table",
            doc="?string",
            auth_callback="?function",
    })

    local schema = opts.schema
    local name = opts.name
    local func = opts.callback
    local kind = opts.kind
    local arguments = opts.args
    local doc = opts.doc
    local auth_callback = opts.auth_callback
    if type(func) == 'string' then
        func = sandbox_call_wrap(func, auth_callback)
    end

    local callback = {
        kind=kind,
        arguments=arguments,
        resolve=func,
        description=doc,
    }
    vars.callbacks[schema] = vars.callbacks[schema] or {}
    vars.callbacks[schema][name] = callback

    vars.graphql_schema[schema] = nil
end

local function add_function_mutation(opts)
    checks({
            schema="string",
            name="string",
            callback="string",
            kind="?table|?string",
            args="?table",
            doc="?string",
            auth_callback="?function",
    })

    local schema = opts.schema
    local name = opts.name
    local func = opts.callback
    local kind = opts.kind
    local arguments = opts.args
    local doc = opts.doc
    local auth_callback = opts.auth_callback
    if type(func) == 'string' then
        func = sandbox_call_wrap(func, auth_callback)
    end

    local mutation = {
        kind=kind,
        arguments=arguments,
        resolve=func,
        description=doc,
    }
    vars.mutations[schema] = vars.mutations[schema] or {}
    vars.mutations[schema][name] = mutation

    vars.graphql_schema[schema] = nil
end

local function remove_callback(schema, name)
    checks("string", "string")

    vars.callbacks[schema] = vars.callbacks[schema] or {}
    vars.callbacks[schema][name] = nil

    vars.graphql_schema[schema] = nil
end

local function add_mutation(opts)
    checks({
            schema = '?string',
            prefix = '?string',
            name = 'string',
            doc = '?string',
            args = '?table',
            kind = 'table',
            callback = 'string',
    })

    if opts.schema == nil then
        opts.schema = 'default'
    end
    vars.mutations[opts.schema] = vars.mutations[opts.schema] or {}

    if opts.prefix then
        local obj = vars.mutations[opts.schema][opts.prefix]
        if obj == nil then
            error('No such mutation prefix ' .. opts.prefix)
        end

        local oldkind = obj.kind
        oldkind.fields[opts.name] = {
            kind = opts.kind,
            arguments = opts.args,
            resolve = funcall_wrap(opts.callback, 'mutation', opts.prefix .. '.' .. opts.name, opts.schema),
            description = opts.doc
        }

        obj.kind = types.object {
            name = oldkind.name,
            fields = oldkind.fields,
            description = oldkind.description,
        }
    else
        local mutation = {
            kind = opts.kind,
            arguments = opts.args,
            resolve = funcall_wrap(opts.callback, 'mutation', opts.name, opts.schema),
            description = opts.doc,
        }
        vars.mutations[opts.schema][opts.name] = mutation
    end

    vars.graphql_schema[opts.schema] = nil
end

local function remove_mutation(schema, name)
    checks("string", "string")

    vars.mutations[schema] = vars.mutations[schema] or {}
    vars.mutations[schema][name] = nil

    vars.graphql_schema[schema] = nil
end

local function add_callback_prefix(schema_name, prefix, doc)
    checks("string", "string", "?string")

    local kind = types.object{
        name='Api'..prefix,
        fields={},
        description=doc,
    }
    local obj = {kind=kind,
                 arguments={},
                 resolve=function(_, _)
                     return {}
                 end,
                 description=doc,}

    vars.callbacks[schema_name] = vars.callbacks[schema_name] or {}
    vars.callbacks[schema_name][prefix] = obj

    vars.graphql_schema[schema_name] = nil
    cache_reset()

    return obj
end

local function add_mutation_prefix(schema_name, prefix, doc)
    checks("string", "string", "?string")

    local kind = types.object{
        name='MutationApi'..prefix,
        fields={},
        description=doc,
    }
    local obj = {kind=kind,
                 arguments={},
                 resolve=function(_, _)
                     return {}
                 end,
                 description = doc}

    vars.mutations[schema_name] = vars.mutations[schema_name] or {}
    vars.mutations[schema_name][prefix] = obj

    vars.graphql_schema[schema_name] = nil
    cache_reset()

    return obj
end

local function secure_query(entry)
    if entry.__secured == nil then
        local original_resolve = entry.resolve
        entry.resolve = function(...)
            if original_resolve then
                return original_resolve(...)
            end
            return true
        end

        entry.__secured = true
    end
    return entry
end

local function secure_mutation(entry)
    if entry.__secured == nil then
        local original_resolve = entry.resolve
        entry.resolve = function(...)
            if original_resolve then
                return original_resolve(...)
            end
            return true
        end

        entry.__secured = true
    end
    return entry
end

local function get_schema(schema_name)
    checks("string")

    local existing_scheme = vars.graphql_schema[schema_name]
    if existing_scheme ~= nil then
        return existing_scheme
    end

    local fields = {}
    local mutations = table.copy(vars.mutations[schema_name] or {})
    local callbacks = table.copy(vars.callbacks[schema_name] or {})

    for name, fun in pairs(callbacks) do
        fields[name] = fun
    end

    local model = vars.model[schema_name] or {
        query_fields = {},
        mutation_fields = {},
    }

    for name, entry in pairs(model.query_fields) do
        fields[name] = secure_query(entry)
    end

    for name, entry in pairs(model.mutation_fields) do
        mutations[name] = secure_mutation(entry)
    end

    local root = {
        query = types.object {name = 'Query', fields=fields},
        mutation = types.object {name = 'Mutation', fields=mutations},
    }

    vars.graphql_schema[schema_name] = schema.create(root, schema_name)
    return vars.graphql_schema[schema_name]
end

local BAD_REQUEST = 400
local INTERNAL_SERVER_ERROR = 500

local function get_error_body(code, message)
    checks("number", "string")

    return json.encode({
        errors = {
            {
                message = message,
                extensions = {
                    code = code,
                }
            }
        }
    })
end

local NANOSECONDS_IN_MILLISECONDS = 1000000ULL
local CONTENT_TYPE = "application/json;charset=utf-8"

local function execute_graphql_internal(parsed, schema_name)
    schema_name = schema_name or parsed.schema or 'default'
    -- TODO: temporary "default" schema is tenant-specific
    if schema_name == 'default' then
        schema_name = tenant.uid()
    end

    local schema_obj
    local ast

    if parsed.query == nil or type(parsed.query) ~= "string" then
        return {
            code = BAD_REQUEST,
            body = "Body should have 'query' field",
        }
    end

    local operationName
    if parsed.operationName ~= nil and type(parsed.operationName) ~= "string" then
        return {
            code = BAD_REQUEST,
            body = "'operationName' should be string",
        }
    end

    if parsed.variables ~= nil and type(parsed.variables) ~= "table" then
        return {
            code = BAD_REQUEST,
            body = "'variables' should be a dictionary"
        }
    end

    if parsed.schema ~= nil and type(parsed.schema) ~= "string" then
        return nil, {
            code = BAD_REQUEST,
            body = "'schema' should be string",
        }
    end

    if parsed.operationName ~= nil then
        operationName = parsed.operationName
    end

    local variables = parsed.variables
    if variables == nil then -- cdata<char*> NULL == nil -- Warning do not remove!!
        variables = nil
    end
    local query = parsed.query

    schema_obj = get_schema(schema_name)

    local cachekey = digest.sha256_hex(query)
    ast = cache_get(schema_name, cachekey)
    if ast == nil then
        local err
        local span = tracing.start_span('graphql.parse')
        ast, err = graphql_error:pcall(parse.parse, query)
        span:finish({error = err})
        if err ~= nil then
            log.error("Graphql parsing failed: %s", err)
            return {
                code = BAD_REQUEST,
                body = err.err,
            }
        end

        local _, err = graphql_error:pcall(validate.validate, schema_obj, ast)

        if err ~= nil then
            log.error("Graphql validation failed: %s", err)
            return {
                code = BAD_REQUEST,
                body = err.err,
            }
        end

        cache_set(schema_name, cachekey, ast)
    end

    local rootValue = {}

    local span = tracing.start_span('graphql.execute_internal')
    local exec_start_time = clock.monotonic64()
    local data, err = graphql_error:pcall(execute.execute, schema_obj, ast, rootValue, variables, operationName)
    span:finish({error = err})

    if err ~= nil then
        log.error("Graphql execution failed: %s", err)
        metrics.write_fail(ast, schema_name)

        if not errors.is_error_object(err) then
            return {
                code = INTERNAL_SERVER_ERROR,
                body = err and (err.err or err.message) or tostring(err),
            }
        else
            return {
                code = err.code or INTERNAL_SERVER_ERROR,
                body = err and (err.err or err.message) or tostring(err),
            }
        end
    end

    local result = {data = data}
    metrics.write_time((clock.monotonic64() - exec_start_time) / NANOSECONDS_IN_MILLISECONDS, ast, schema_name)

    return {
        body = result,
    }
end

local function execute_graphql(req)
    local ok, parsed = pcall(req.json, req)

    local default_headers = {
        ['content-type'] = CONTENT_TYPE
    }

    if not ok then
        return {
            status = 200,
            headers = default_headers,
            body = get_error_body(BAD_REQUEST, parsed)
        }
    end

    local span = tracing.start_span('graphql.execute')
    local res, err = graphql_error:pcall(execute_graphql_internal, parsed, req.headers.schema)
    span:finish({error = err})

    local resp = {
        status = 200,
        headers = default_headers,
    }
    if err ~= nil then
        resp.body = get_error_body(INTERNAL_SERVER_ERROR, err.err)
    elseif res.code ~= nil then
        resp.body = get_error_body(res.code, res.body)
    else
        resp.body = res and res.body and json.encode(res.body)
    end

    return resp
end

local function execute_graphql_iproto(parsed, options)
    options = options or {}

    local context, err = request_context.parse_options(options)
    if err ~= nil then
        return nil, err
    end
    request_context.init(context)

    if not vars.auth.authorize_with_token(options.token) then
        request_context.clear()
        return nil, 'Access denied'
    end

    local result, err = graphql_error:pcall(execute_graphql_internal, parsed)
    request_context.clear()
    return result, err
end

local function init_iproto_graphql()
    rawset(_G, 'execute_graphql', execute_graphql_iproto)
end

local function init()
    cache_reset()

    local httpd = cartridge.service_get('httpd')
    http.add_route(httpd, { public = false, path = '/graphql', method = 'POST' },
        'common.graphql', 'execute_graphql')
    vars.auth = require('common.admin.auth')
end

local function parseNullLiteral(_)
    return box.NULL
end

local scalar_kind_to_parse = {
    int = types.long.parseLiteral, -- use long parser for int64 parsing
    long = types.long.parseLiteral,
    float = types.float.parseLiteral,
    string = types.string.parseLiteral,
    boolean = types.boolean.parseLiteral,
    null = parseNullLiteral,
}

-- special graphql type to process any scalar type argument
-- for example
--     types.list(types_any('arg'))
-- can be used for
--     1, 'test', 1.1, false, null
-- BUT NOT FOR nested objects
--     ["QWERTY", false, 123, 654.77]
--     [{a:123, b:456}, "QWERTY"]
local types_any_scalar = types.scalar {
    name = 'AnyScalar',
    description = 'Type to process any scalar type',
    serialize = function(value)
        if type(value) == 'table' then
            error('Not scalar kind ' .. type(value))
        end
        return value
    end,
    parseValue = function(value)
        if type(value) == 'table' then
            error('Not scalar kind ' .. type(value))
        end
        return value
    end,
    parseLiteral = function(lit)
        if scalar_kind_to_parse[lit.kind] then
            return scalar_kind_to_parse[lit.kind](lit)
        end
    end,
    isValueOfTheType = function(value)
        return type(value) ~= 'table'
    end,
}

local types_json = types.scalar{
    name = 'Json',
    description = 'Type to process any data in JSON format',
    serialize = function(value)
        return json.encode(value)
    end,
    parseValue = function(value)
        if type(value) == 'string' then
            return json.decode(value)
        end
        if value == nil then
            return value
        end
    end,
    parseLiteral = function(lit)
        if lit.kind == 'string' then
            return json.decode(lit.value)
        end
        if lit.value == nil then
            return lit.value
        end
    end,
    isValueOfTheType = function(value)
        return type(value) == 'string' or value == nil
    end,
}

local function on_resolve(trigger_new)
    checks('?function')
    vars.on_resolve_trigger = trigger_new
end

return {
    init = init,
    init_iproto_graphql = init_iproto_graphql,
    execute_graphql = execute_graphql,
    set_model = set_model,
    set_shared_types = set_shared_types,
    add_callback = add_callback,

    add_mutation = add_mutation,
    remove_callback = remove_callback,
    remove_mutation = remove_mutation,

    add_callback_prefix = add_callback_prefix,
    add_mutation_prefix = add_mutation_prefix,

    add_function_callback = add_function_callback,
    add_function_mutation = add_function_mutation,

    cache_reset = cache_reset,
    on_resolve = on_resolve,

    types = {
        any_scalar = types_any_scalar,
        json = types_json,
    },
}
