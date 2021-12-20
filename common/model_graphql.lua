local decimal = require('decimal')
local uuid = require('uuid')
local json = require('json')
local checks = require('checks')
local errors = require('errors')
local cartridge_utils = require('cartridge.utils')

local model = require('common.model')
local model_walker = require('common.model.walker')
local utils = require('common.utils')
local request_context = require('common.request_context')
local sandbox_registry = require('common.sandbox.registry')

local query_plan_graphql = require('common.document.query_plan.graphql')
local query_plan = require('common.document.query_plan')
local common_graphql = require('common.graphql')
local tenant = require('common.tenant')
local tenant_states = tenant.states

local types = require('graphql.types')
local model_graphql_error = errors.new_class("model_graphql_error")

local function make_options(args)
    local first = args['first']
    local after = args['after']
    local version = args['version'] or request_context.get().version
    local only_if_version = args['only_if_version']
    local if_not_exists = args['if_not_exists']
    local all_versions = args['all_versions']

    return {
        first = first,
        after = after,
        version = version,
        only_if_version = only_if_version,
        if_not_exists = if_not_exists,
        all_versions = all_versions,
    }
end

local field_suffixes = {"_like", "_ilike"}
local index_suffixes = {"", "_gt", "_ge", "_lt", "_le"}

local field_suffixes_to_operations = {
    _like = "LIKE",
    _ilike = "ILIKE"
}
local index_suffixes_to_operations = {
    [""] = "==",
    _gt  = ">",
    _ge  = ">=",
    _lt  = "<",
    _le  = "<=",
}

local function is_enum_type(field)
    return type(field) == 'table'
        and field.type == 'enum'
end

local function join_path(lhs, rhs)
    if lhs == '' or lhs == nil then
        return rhs
    end

    return lhs .. '.' .. rhs
end

local function is_versioning_enabled(type_entry)
    -- FIXME: Remove expiration
    local expiration_cfg = tenant.get_cfg_non_null('versioning', 'expiration')
    if expiration_cfg == nil then
        return false
    end

    for _, section in ipairs(expiration_cfg) do
        if section.type == type_entry.name then
            return section.enabled == true
        end
    end

    return true
end

local type_table = {
    boolean=types.boolean,
    int=types.int,
    long=types.long,
    float=types.float,
    double=types.float,
    bytes=types.string,
    string=types.string,
    decimal=types.string,
    uuid=types.string,
    any=common_graphql.types.json,
}

local PRIMITIVE_TYPES = {
    null = true,
    boolean = true,
    int = true,
    long = true,
    float = true,
    double = true,
    bytes = true,
    string = true,
    decimal = true,
    uuid = true,
    any = true,
}

local function is_primitive_type(type_entry)
    if type(type_entry) == 'string' then
        return PRIMITIVE_TYPES[type_entry]
    end

    return PRIMITIVE_TYPES[type_entry.type]
end

local function get_field_by_path(obj, path)
    local path_dict = string.split(path, ".")

    for _, v in ipairs(path_dict) do
        obj = obj[v]
        if obj == nil then
            return nil
        end
    end

    if type(obj) == "table" then
        return nil
    end

    return obj
end

local function gen_one_to_one_resolve(type_name, from_spec, to_spec)
    local tenant_uid = tenant.uid()
    return function (rootObject, args)
        local from = {}

        for _, entry in ipairs(from_spec) do
            table.insert(from, get_field_by_path(rootObject, entry))
        end

        if #from == 0 then
            return nil
        end

        local repository = tenant.get_repository({tenant = tenant_uid})
        -- since repository.get accepts only primary key as search value we have to use repository.find instead
        local filter = {{to_spec, '==', from}}
        local res, err = repository:find(type_name, filter, make_options(args))

        if res == nil then
            error(err)
        end

        if #res >= 0 then
            return res[1]
        end

        return nil
    end
end

local function gen_one_to_many_resolve(type_name, from_spec, to_spec, state)
    local type_entry
    for _, entry in ipairs(state.mdl) do
        if entry.name == type_name then
            type_entry = entry
            break
        end
    end

    local indexes = {}
    local ops = {}

    for _, index in ipairs(type_entry.indexes) do
        for key, op in pairs(index_suffixes_to_operations) do
            indexes[index.name..key] = index.name
            ops[index.name..key] = op
        end
    end

    local tenant_uid = tenant.uid()
    return function (rootObject, args)
        local from = {}

        for _, entry in ipairs(from_spec) do
            table.insert(from, get_field_by_path(rootObject, entry))
        end

        if #from == 0 then
            return nil
        end

        local request = {{to_spec, '==', from}}

        for _, tuple in ipairs(getmetatable(args).__index) do
            local key = tuple.name
            local value = tuple.value
            if key ~= 'first' and key ~= 'after'
            and key ~= 'version' and key ~= 'all_versions' then
                local op = ops[key]

                table.insert(request, {indexes[key], op, value})
            end
        end

        local repository = tenant.get_repository({tenant = tenant_uid})
        local res, err = repository:find(type_name, request, make_options(args))

        if res == nil then
            error(err)
        end

        return res
    end
end


local function get_index_by_name(type_entry, index_name)
    for _, index in ipairs(type_entry.indexes or {}) do
        if index.name == index_name then
            return index
        end
    end

    return nil
end

local function get_primary_index(type_entry)
    return type_entry.indexes and type_entry.indexes[1]
end

--[[
    Returns modified value with datetime string types replaced with unix epoch int
    Warning! For performance ``value`` is modified
]]
local function serialize_index_value(type_entry, index, value)
    local value_len = 1
    if type(value) == 'table' then
        value_len = #value
    end

    for pos, part in ipairs(index.parts) do
        if pos > value_len then
            break
        end

        local field_type = model.get_field_type(type_entry, part)
        local logical_type = field_type.logicalType

        if logical_type ~= nil then
            local source
            local is_multipart_index = type(value) == 'table'
            if is_multipart_index then
                source = value[pos]
            else
                source = value
            end

            local val
            if logical_type == 'DateTime'
                or logical_type == 'Date'
                or logical_type == 'Time'
            then
                val = utils.iso8601_str_to_nsec(source)
                if val == nil then
                    -- fallback to date
                    val = utils.date_str_to_nsec(source)
                end
                if val == nil then
                    -- fallback to time
                    val = utils.time_str_to_nsec(source)
                end
            elseif logical_type == 'Decimal' then
                val = decimal.new(source)
            elseif logical_type == 'UUID' then
                val = uuid.fromstr(source)
            end

            if val ~= nil then
                if is_multipart_index then
                    value[pos] = val
                else
                    return val
                end
            end
        end
    end
    return value
end

local function field_path_to_arg_name(field_path)
    return field_path:gsub('%.', '_')
end

local function get_index_type(type_entry, index_name)
    local index = get_index_by_name(type_entry, index_name)

    local resolved_type

    if #index.parts == 1 then
        local field_path = index.parts[1]
        local field_type, err = model.get_field_type(type_entry, field_path)
        if field_type == nil then
            return nil, err
        end

        field_type = model.strip_nullable(field_type)
        if field_type.indexes ~= nil then
            local parts = get_primary_index(field_type).parts

            if #parts > 1 then
                return nil, model_graphql_error:new(
                    "Type '%s' index '%s' refers to multipart index on other entity, which is prohibited",
                    type_entry.name, index_name)

            end

            field_type, err = model.get_field_type(field_type, parts[1])

            if field_type == nil then
                return nil, err
            end

            field_type = model.strip_nullable(field_type)
        end

        if type(field_type) == 'table' and
           field_type.type == 'array' then
            field_type = field_type.items
        end

        if model.is_date_time(field_type) then
            resolved_type = types.string
        elseif model.is_date(field_type) then
            resolved_type = types.string
        elseif model.is_time(field_type) then
            resolved_type = types.string
        elseif model.is_decimal(field_type) then
            resolved_type = types.string
        elseif model.is_uuid(field_type) then
            resolved_type = types.string
        elseif is_enum_type(field_type) then
            resolved_type = types.string
        else
            resolved_type = type_table[field_type.type or field_type]
        end

        if resolved_type == nil then
            return nil, model_graphql_error:new(
                "Type '%s' index '%s' has incorrect type: '%s'",
                type_entry.name, index_name, json.encode(field_type))
        end
    else
        resolved_type = types.list(common_graphql.types.any_scalar)
    end

    return resolved_type
end

local function generate_one_to_one_relation(type_entry, relation)
    local from_spec = relation.from_fields

    if type(from_spec) == 'string' then
        local index = get_index_by_name(type_entry, from_spec)

        from_spec = index.parts
    end

    local to_spec = relation.to_fields
    local to = relation.to

    if type(to_spec) ~= 'string' then
        return nil, model_graphql_error:new("one-to-one relation should point to index field")
    end

    local resolve = gen_one_to_one_resolve(to, from_spec, to_spec)

    local arguments = {}

    if is_versioning_enabled(type_entry) then
        arguments.version = types.long
    end

    local field = {
        kind = to,
        arguments = arguments,
        resolve = resolve,
        description = relation.doc,
    }

    return field
end

local function generate_one_to_many_relation(type_entry, relation, state)
    local from_spec = relation.from_fields

    if type(from_spec) == 'string' then
        local index = get_index_by_name(type_entry, from_spec)

        from_spec = index.parts
    end

    local to_spec = relation.to_fields
    local to = relation.to

    if type(to_spec) ~= 'string' then
        return nil, model_graphql_error:new("one-to-many relation should point to index field")
    end

    local resolve = gen_one_to_many_resolve(to, from_spec, to_spec, state)

    local arguments = {
        first = types.int,
        after = types.string,
    }

    if is_versioning_enabled(type_entry) then
        arguments.version = types.long
        arguments.all_versions = types.boolean
    end

    local to_type_entry
    for _, entry in ipairs(state.mdl) do
        if entry.name == to then
            to_type_entry = entry
            break
        end
    end

    for _, index in ipairs(to_type_entry.indexes) do
        local index_type, err = get_index_type(to_type_entry, index.name)

        if index_type == nil then
            return nil, err
        end

        for _, suffix in ipairs(index_suffixes) do
            arguments[index.name..suffix] = index_type
        end
    end

    local field = {
        kind = types.list(types.nonNull(to)),
        resolve = resolve,
        arguments = arguments,
        description = relation.doc,
    }

    return field
end

local function get_sandbox()
    local current_state = tenant.get_state()
    if current_state == tenant_states.CONFIG_APPLY
    or current_state == tenant_states.ACTIVE
    then
        return sandbox_registry.get('active')
    end

    return sandbox_registry.get('tmp')
end

local type_to_graphql = nil

local function gen_resolve_fun(fun, state)
    local function resolve(rootObject, args)
        local sandbox = get_sandbox()
        model_graphql_error:assert(sandbox ~= nil, 'sandbox instance must be registered')
        local res, err = sandbox:call_by_name(fun.ref, rootObject, args)
        if res == nil and err ~= nil then
            error(err)
        end
        return res
    end

    local arguments = {}

    for _, arg in ipairs(fun.arguments) do
        arguments[arg.name] = type_to_graphql('', arg.type, state)
    end

    local resolved_type = type_to_graphql('', fun.type, state)
    local field = {
        kind = resolved_type,
        resolve = resolve,
        arguments = arguments,
        description = fun.doc,
    }

    return field
end

local function is_system_field(field_name)
    return field_name:startswith('__')
end

local function get_input_type(state, converted, opts)
    opts = opts or {}
    if type(converted) == "string" then
        return converted .. "Input"
    end

    if converted.__type == 'NonNull' then
        return types.nonNull(get_input_type(state, converted.ofType, opts))
    end

    if converted.__type == 'List' then
        return types.list(get_input_type(state, converted.ofType, opts))
    end

    if converted.__type ~= 'Object' then
        return converted
    end

    local name = converted.name .. "Input"
    if opts.prefix ~= nil then
        name = opts.prefix .. name
    end

    if state[name] then
        return state[name]
    end

    local fields = {}
    for field_name, field in pairs(converted.fields) do
        if type(field.resolve) ~= 'function' and not is_system_field(field_name) then
            field = field.__type and { kind = field } or field
            fields[field_name] = {
                name = field_name,
                kind = get_input_type(state, field.kind, opts)
            }
        end
    end

    local input  = types.inputObject({
        name = name,
        fields = fields,
        schema = opts.schema,
    })

    state[name] = input

    return input
end

local function type_is_nullable(opts)
    if opts.is_nullable == true then
        return true
    elseif opts.auto_increment == true then
        return true
    elseif opts.default ~= nil then
        return true
    elseif opts.default_function ~= nil then
        return true
    end
    return false
end

type_to_graphql = function(root, type_entry, state, opts)
    opts = opts or {}

    if type_entry == 'null' then
        return nil, model_graphql_error:new('There is no "null" type in graphql')
    end

    if model.is_nullable_type(type_entry) or opts.is_nullable == true then
        type_entry = model.strip_nullable(type_entry)

        if type_is_nullable(opts) then
            opts = table.copy(opts)
            opts.is_nullable = nil
            opts.default = nil
            opts.default_function = nil
            opts.auto_increment = nil
        end

        local nested, err = type_to_graphql(root, type_entry, state, opts)
        if nested == nil then
            return nil, err
        end

        if type(nested) == 'string' then
            return nested
        end

        return nested.ofType
    end

    if is_primitive_type(type_entry) then
        local type_name = type_entry.type or type_entry

        local nested = type_table[type_name] or type_entry

        local err
        if type_is_nullable(opts) == true then
            nested, err = types.nullable(nested)
        else
            nested, err = types.nonNull(nested)
        end
        if err ~= nil then
            return nil, err
        end

        return nested
    end

    if type(type_entry) == 'string' then
        if opts.input == true then
            return types.nonNull(get_input_type(state, type_entry, opts))
        end

        local nested, err = types.nonNull(type_entry)

        if nested == nil then
            return nil, err
        end

        return nested
    end

    if model.is_union_type(type_entry) then
        state.union_count = state.union_count + 1

        local union_name = "_Union" .. tostring(state.union_count)

        local fields = {}
        local is_nullable = false

        for _, subtype in ipairs(type_entry) do
            if type(subtype) == 'string' then
                if subtype == 'null' then
                    is_nullable = true
                else
                    return nil, model_graphql_error:new(
                        "Unsupported type in union: %s", subtype)
                end
            else
                local nested, err = type_to_graphql(root, subtype, state, opts)
                if nested == nil then
                    return nil, err
                end

                fields[subtype.name] = nested.ofType
            end
        end

        local name = union_name
        if opts.prefix ~= nil then
            name = opts.prefix .. name
        end

        local union_record = types.object({
            name = name,
            fields = fields,
            schema = opts.schema,
        })

        if not is_nullable then
            union_record = types.nonNull(union_record)
        end

        return union_record
    end

    if type_entry.type == 'enum' then
        local existing = state.type_map[type_entry.name]

        if existing ~= nil then
            return types.nonNull(existing)
        end

        local values = {}

        for _, name in ipairs(type_entry.symbols) do
            values[name] = {value=name}
        end

        local name = type_entry.name
        if opts.prefix ~= nil then
            name = opts.prefix .. name
        end

        local enum_type = types.enum({
            name = name,
            values = values,
            schema = opts.schema,
        })

        state.type_map[type_entry.name] = enum_type
        return types.nonNull(enum_type)
    end

    if type_entry.type == 'array' then
        local nested, err = type_to_graphql(root, type_entry.items, state, opts)
        if nested == nil then
            return nil, err
        end

        return types.nonNull(types.list(nested))
    end

    if type_entry.name == nil then
        return nil, model_graphql_error:new('Invalid type %s', json.encode(type_entry))
    end

    if model.is_record(type_entry) then
        local existing = state.type_map[type_entry.name]

        if existing ~= nil then
            return types.nonNull(existing)
        end

        local fields = {}

        for _, field in ipairs(type_entry.fields or {}) do
            opts.auto_increment = field.auto_increment
            opts.default = field.default
            opts.default_function = field.default_function

            local subpath = join_path(root, field.name)
            local nested, err = type_to_graphql(subpath, field.type, state, opts)

            if nested == nil then
                return nil, err
            end

            local name = field.name
            if opts.prefix ~= nil then
                name = opts.prefix .. name
            end
            fields[field.name] = {
                name = name,
                description = field.doc,
                kind = nested,
            }
        end

        for _, fun in ipairs(type_entry.functions or {}) do
            local field, err = gen_resolve_fun(fun, state)
            if field == nil then
                return nil, err
            end

            fields[fun.name] = field
        end


        for _, relation in ipairs(type_entry.relations or {}) do
            if relation.count == 'one' then
                local field, err = generate_one_to_one_relation(type_entry, relation)
                if field == nil then
                    return nil, err
                end
                fields[relation.name] = field
            else
                local field, err = generate_one_to_many_relation(type_entry, relation, state)
                if field == nil then
                    return nil, err
                end
                fields[relation.name] = field
            end
        end

        if type_entry.indexes ~= nil then
            fields['cursor'] = types.string
            if is_versioning_enabled(type_entry) then
                fields['version'] = types.long
            end
            fields['__query_plan'] = types.resolve('QueryPlanSchema', '__global__')
        end

        local name = type_entry.name
        if opts.prefix ~= nil then
            name = opts.prefix .. name
        end

        local new_type = types.object({
            name = name,
            fields = fields,
            description = type_entry.doc,
            schema = opts.schema,
        })

        state.type_map[type_entry.name] = new_type

        return types.nonNull(new_type)
    end

    return nil, model_graphql_error:new("Unsupported avro type: %s", type_entry.type)
end

local system_args = {
    ['first'] = true,
    ['after'] = true,
    ['version'] = true,
    ['insert'] = true,
    ['update'] = true,
    ['delete'] = true,
    ['only_if_version'] = true,
    ['all_versions'] = true,
    ['if_not_exists'] = true,
}

local function args_to_request(type_entry, filter_operations, query_args)
    local request = {}

    for _, tuple in ipairs(getmetatable(query_args)['__index']) do
        local key = tuple.name
        if system_args[key] ~= true then
            local value = tuple.value
            local op = filter_operations[key]

            local index = get_index_by_name(type_entry, op.target_path)
            if index ~= nil then
                value = serialize_index_value(type_entry, index, value)
            end

            table.insert(request, {op.target_path, op.op, value})
        end
    end

    return request
end

local function delete_aggregate(type_entry, filter_operations, args, context)
    local request = args_to_request(type_entry, filter_operations, args)

    local repository = tenant.get_repository(context)
    local res, err = repository:delete(type_entry.name, request,
        make_options(args))

    if res == nil then
        error(err)
    end

    return res
end

local function update_aggregate(type_entry, filter_operations, args, context)
    local request = args_to_request(type_entry, filter_operations, args)
    local update_list = args.update

    local repository = tenant.get_repository(context)
    local res, err = repository:update(type_entry.name, request, update_list,
        make_options(args))

    if res == nil then
        error(err)
    end

    return res
end

local function find_aggregate(type_entry, request, args, context)
    local repository = tenant.get_repository(context)
    local res, err, meta = repository:find(type_entry.name, request, make_options(args))
    if res == nil then
        error(err)
    end

    return res, nil, meta
end


local function insert_aggregate(type_entry, args, context)
    local obj = args.insert
    local repository = tenant.get_repository(context)
    local res, err = repository:put(type_entry.name, obj, make_options(args))

    if res == nil then
        error(err)
    end

    return res
end

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

local function get_filter_operations(type_entry)
    local ops = {}

    for _, field_path in ipairs(get_string_field_paths(type_entry)) do
        for key, op in pairs(field_suffixes_to_operations) do
            local arg_name = field_path_to_arg_name(field_path)
            ops[arg_name..key] = {
                arg_name = arg_name,
                target_path = field_path,
                op = op
            }
        end
    end

    for _, index in ipairs(type_entry.indexes) do
        for key, op in pairs(index_suffixes_to_operations) do
            ops[index.name..key] = {
                arg_name = index.name,
                target_path = index.name,
                op = op
            }
        end
    end

    return ops
end


local function gen_resolve_aggregate(type_entry)
    local filter_operations = get_filter_operations(type_entry)

    local tenant_uid = tenant.uid()
    return function (_, args)
        local request = args_to_request(type_entry, filter_operations, args)
        local result, err, meta = find_aggregate(type_entry, request, args, {tenant = tenant_uid})
        if err ~= nil then
            return nil, err
        end

        if request_context.is_explain_enabled() then
            local type_name = type_entry.name
            local explain_entry = query_plan.explain(type_name, request, make_options(args), meta)
            if explain_entry['version'] ~= nil then
                explain_entry['version'] = tostring(explain_entry['version']):gsub("U*LL", "")
            end
            for i = 1, #result do
                result[i].__query_plan = explain_entry
            end
        end
        return result
    end
end

local function gen_mutate_aggregate(type_entry)
    local filter_operations = get_filter_operations(type_entry)

    local tenant_uid = tenant.uid()
    return function (_, args)
        local insert = args['insert']
        local delete = args['delete']
        local update = args['update']

        if insert ~= nil then
            return insert_aggregate(type_entry, args, {tenant = tenant_uid})
        elseif delete ~= nil then
            return delete_aggregate(type_entry, filter_operations, args, {tenant = tenant_uid})
        elseif update ~= nil then
            return update_aggregate(type_entry, filter_operations, args, {tenant = tenant_uid})
        end
    end
end

local function model_to_graphql(mdl, schema_name, prefix)
    checks('table', '?string', '?string')

    if schema_name == nil then
        schema_name = tenant.uid()
    end

    query_plan_graphql.init()

    local query_fields = {}
    local mutation_fields = {}
    local state = {type_map = {}, union_count = 0, mdl = mdl}
    local opts = {schema = schema_name, prefix = prefix}

    for _, type_entry in ipairs(mdl) do
        local converted, err = type_to_graphql('', type_entry, state, opts)
        if err ~= nil then
            return nil, err
        end

        local input
        if model.is_record(type_entry) then
            input = get_input_type(state, converted, opts)
        end

        if type_entry.indexes ~= nil then
            local arguments = {}

            if is_versioning_enabled(type_entry) then
                arguments.version = types.long
                arguments.all_versions = types.boolean
            end

            for _, field_path in ipairs(get_string_field_paths(type_entry)) do
                for _, suffix in ipairs(field_suffixes) do
                    arguments[field_path_to_arg_name(field_path)..suffix] = types.string
                end
            end

            for _, index in ipairs(type_entry.indexes) do
                local index_type, err = get_index_type(type_entry, index.name)
                if index_type == nil then
                    return nil, err
                end

                for _, suffix in ipairs(index_suffixes) do
                    arguments[index.name..suffix] = index_type
                end
            end

            local query_arguments = table.copy(arguments)
            query_arguments.first = types.int
            query_arguments.after = types.string

            query_fields[type_entry.name] = {
                kind = types.list(converted.ofType),
                arguments = query_arguments,
                resolve = gen_resolve_aggregate(type_entry),
            }

            local mutation_arguments = table.copy(arguments)

            if is_versioning_enabled(type_entry) then
                mutation_arguments.only_if_version = types.long
            end
            mutation_arguments.if_not_exists = types.boolean
            mutation_arguments.insert = input.ofType
            mutation_arguments.delete = types.boolean
            mutation_arguments.update = types.list(types.list(common_graphql.types.any_scalar.nonNull).nonNull)

            mutation_fields[type_entry.name] = {
                kind = types.list(converted.ofType),
                arguments = mutation_arguments,
                resolve = gen_mutate_aggregate(type_entry),
            }
        end
    end

    return {
        query_fields = query_fields,
        mutation_fields = mutation_fields,
        type_map = state.type_map,
    }
end

local function resolve_graphql_type(mdl, type_map, type_entry, opts)
    checks('table', 'table', 'string|table', '?table')
    opts = opts or {}
    opts.schema = opts.schema or tenant.uid()

    local state = {
        type_map = type_map,
        union_count = 0,
        mdl = mdl,
    }

    if type(type_entry) == 'table' then
        type_entry = table.deepcopy(type_entry)
        cartridge_utils.table_setrw(type_entry)
    end
    local normalized_type_entry = model.normalize_defined_type(type_entry)
    return type_to_graphql('', normalized_type_entry, state, opts)
end

local function validate(mdl)
    local schema_name = '__' .. tenant.uid() .. '_validate'

    local _, err = model_to_graphql(mdl, schema_name)

    local types_registry = types.get_env(schema_name)
    for k in pairs(types_registry) do
        types_registry[k] = nil
    end

    if err ~= nil then
        return nil, err
    end
    return true
end

return {
    model_to_graphql = model_to_graphql,

    resolve_graphql_type = resolve_graphql_type,

    get_input_type = get_input_type,

    validate = validate,
}
