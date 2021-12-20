local msgpack = require('msgpack')
local decimal = require('decimal')
local uuid = require('uuid')
local errors = require('errors')
local utils = require('common.utils')
local checks = require('checks')
local model_serializer = require('common.model_serializer')
local model_validation = require('common.model.validation')
local model = require('common.model')

local vars = require('common.vars').new('common.model_flatten')
local serialization_error = errors.new_class("serialization_error")

vars:new('formats')

local VALUE=1
local ARRAY=2
local RECORD=3
local NESTED_RECORD=4
local UNION=5
local DATE_TIME=6
local DATE=7
local TIME=8
local DECIMAL=9
local UUID=10

local function join_path(lhs, rhs)
    if lhs == '' or lhs == nil then
        return rhs
    end

    return lhs .. '.' .. rhs
end

local function build_unknown_enum_error(value, type_name, node_path)
    local err = serialization_error:new("Unknown enum value")
    err.enum_value = value
    err.enum_type = type_name
    err.node_path = node_path
    return err
end

local function field_id_by_name(format, type_name)
    if vars.formats[type_name] ~= nil then
        return vars.formats[type_name]
    end

    local res = {}

    for id, entry in ipairs(format) do
        res[entry.name] = id
    end

    vars.formats[type_name] = res
    return res
end

local function get_pk_fields(ddl, type_name)
    local ddl_entry = ddl[type_name]
    local format = field_id_by_name(ddl_entry.format, type_name)

    local res = {}
    local index = ddl_entry.indexes[1]
    for _, part in ipairs(index.parts) do
        local field_name = part.field
        local field_id = format[field_name]

        if field_name ~= 'version' then
            table.insert(res, field_id)
        end
    end

    return res
end

local function get_ddl_fields_num(ddl, type_name)
    return #ddl[type_name].format
end

local function get_affinity_fields(ddl, type_name)
    local ddl_entry = ddl[type_name]
    local affinity
    if ddl_entry.affinity ~= nil then
        affinity = ddl_entry.affinity
    else
        affinity = {}
        for _, part in ipairs(ddl_entry.indexes[1].parts) do
            table.insert(affinity, part.field)
        end
    end

    local format = field_id_by_name(ddl_entry.format, type_name)

    local res = {}
    for _, affname in ipairs(affinity) do
        local field_id = format[affname]

        if field_id == nil then
            serialization_error:assert(false, "affinity field %q for type %q not found", affname, type_name)
        end

        table.insert(res, field_id)
    end

    return res
end

-- returns indexes of affinity keys in primary key fields
local function get_pk_related_affinity_fields(pkey_ids, affinity_ids)
    -- in case affinity equals to primary key there is no need no stora such table
    if #pkey_ids == #affinity_ids then
        local same = true

        for i, v in ipairs(pkey_ids) do
            if v ~= affinity_ids[i] then
                same = false
                break
            end
        end

        if same == true then
            return nil
        end
    end

    local pk_id_positions = {}
    for i, v in ipairs(pkey_ids) do
        pk_id_positions[v] = i
    end

    local res = {}
    for _, v in ipairs(affinity_ids) do
        table.insert(res, pk_id_positions[v])
    end

    return res
end

local function get_record_schema(type_name, ddl, schema)
    local pk = get_pk_fields(ddl, type_name)
    local affinity = get_affinity_fields(ddl, type_name)
    return {RECORD, {
        type_name = type_name,
        tree = schema,
        pk = pk,
        ddl_fields_num = get_ddl_fields_num(ddl, type_name),
        affinity = affinity,
        -- this field in a way duplicates affinity, but having it we make repository.get method faster
        pk_related_affinity = get_pk_related_affinity_fields(pk, affinity),
    }}
end

local function gen_schema(ddl, path, root_type_name, type_entry)
    if type_entry == nil then
        return nil
    end

    local schema = {}

    local format = field_id_by_name(ddl[root_type_name].format, root_type_name)
    if model.is_primitive_type(type_entry) then
        local id = format[path]
        if id == nil then -- for nested values
            local path_parts = path:split('.')
            id = path_parts[#path_parts]
        end
        return {VALUE, {id=id}}
    end

    if model.is_date_time(type_entry) then
        return {DATE_TIME, {id=format[path]}}
    end

    if model.is_date(type_entry) then
        return {DATE, {id=format[path]}}
    end

    if model.is_time(type_entry) then
        return {TIME, {id=format[path]}}
    end

    if model.is_decimal(type_entry) then
        return {DECIMAL, {id=format[path]}}
    end

    if model.is_uuid(type_entry) then
        return {UUID, {id=format[path]}}
    end

    if model.is_union_type(type_entry)  then
        for _, union_type_entry in ipairs(type_entry) do
            local type_name = union_type_entry.name or union_type_entry
            local subpath = join_path(path, type_name)
            local subtype = gen_schema(ddl, subpath, root_type_name, union_type_entry)
            schema[type_name] = subtype
        end

        return {UNION, {tree=schema, id=format[path]}}
    end

    if model.is_array(type_entry) then
        local fieldnos
        if format[path] ~= nil then
            fieldnos = {}
            local path_dot = path .. '.'
            for fieldname, fieldno in pairs(format) do
                if fieldname == path or fieldname:startswith(path_dot) then
                    table.insert(fieldnos, fieldno)
                end
            end
            if #fieldnos < 2 then
                fieldnos = nil
            end
        end

        type_entry = type_entry.items
        local subtype_schema = gen_schema(ddl, path, root_type_name, type_entry)
        local schema = {ARRAY, {
            id = format[path],
            tree = subtype_schema,
            fieldnos = fieldnos,
        }}
        return schema
    end

    if type_entry.type == 'enum' then
        local symbols_map = {}
        for _, symbol in ipairs(type_entry.symbols) do
            symbols_map[symbol] = true
        end
        return {VALUE, {id=format[path], enum={
            name = type_entry.name, symbols = symbols_map
        }}}
    end

    if model.is_record(type_entry) then
        for _, field in ipairs(type_entry.fields) do
            local subtype = field.type
            local subpath = join_path(path, field.name)
            if model.is_record(subtype) then
                schema[field.name] = {NESTED_RECORD, {
                    id = format[subpath],
                    tree = gen_schema(ddl, subpath, root_type_name, subtype),
                }}
            else
                schema[field.name] = gen_schema(ddl, subpath, root_type_name, subtype)
            end
        end

        -- Only for root objects
        if model.is_record(type_entry) and path == '' then
            local version_field_id = format['version']
            if version_field_id ~= nil then
                schema['version'] = {VALUE, {id=version_field_id}}
            end
        end
    end

    return schema
end

local function gen_schema_root(ddl, type_entry)
    local type_name = type_entry.name
    local schema = gen_schema(ddl, '', type_name, type_entry)
    return get_record_schema(type_name, ddl, schema)
end

local flatten_table_rec
local function dispatch_node(res, tbl_node, schema_node, node_path)
    local entry_type = schema_node[1]
    local opts = schema_node[2]
    node_path = node_path or ''

    if type(tbl_node) == "table" and #schema_node == 0 then
        return flatten_table_rec(res, tbl_node, schema_node, node_path)
    else
        if entry_type == VALUE then
            if opts.enum ~= nil and opts.enum.symbols[tbl_node] == nil then
                return nil, build_unknown_enum_error(tbl_node, opts.enum.name, node_path)
            end
            res[opts.id] = tbl_node
        elseif entry_type == DATE_TIME then
            local iso8601_str = tbl_node

            if iso8601_str == nil then
                res[opts.id] = box.NULL
                return nil
            end

            local nsec, err = utils.iso8601_str_to_nsec(iso8601_str)
            if nsec == nil then
                return nil, err
            end
            res[opts.id] = nsec
        elseif entry_type == DATE then
            local date_str = tbl_node

            if date_str == nil then
                res[opts.id] = box.NULL
                return nil
            end

            local nsec, err = utils.date_str_to_nsec(date_str)
            if nsec == nil then
                res[opts.id] = box.NULL
                return nil, err
            end

            res[opts.id] = nsec
        elseif entry_type == TIME then
            local date_str = tbl_node
            if date_str == nil then
                res[opts.id] = box.NULL
                return nil
            end

            local nsec, err = utils.time_str_to_nsec(date_str)
            if nsec == nil then
                return nil, err
            end

            res[opts.id] = nsec
        elseif entry_type == DECIMAL then
            if tbl_node == nil then
                res[opts.id] = box.NULL
                return nil
            end

            local ok, val = pcall(decimal.new, tbl_node)

            if not ok then
                return nil, val
            end
            res[opts.id] = val
        elseif entry_type == UUID then
            if tbl_node == nil then
                res[opts.id] = box.NULL
                return nil
            end
            local val
            if uuid.is_uuid(tbl_node) then
                val = tbl_node
            else
                val = uuid.fromstr(tbl_node)
                if val == nil then
                    return nil, string.format('Impossible to convert %q to UUID', tbl_node)
                end
            end
            res[opts.id] = val
        elseif entry_type == UNION then
            local _, err = flatten_table_rec(res, tbl_node, schema_node, node_path)
            if err ~= nil then
                return nil, err
            end
        elseif entry_type == NESTED_RECORD then
            local _, err = flatten_table_rec(res, tbl_node, schema_node, node_path)

            if err ~= nil then
                return nil, err
            end
        elseif entry_type == ARRAY then
            if opts.fieldnos == nil then
                res[opts.id] = tbl_node
            elseif tbl_node ~= nil then
                res[opts.id] = {}
                for i, value in ipairs(tbl_node) do
                    if value ~= nil then
                        res[opts.id][i] = true

                        local tmp_res = {}
                        local _, err = dispatch_node(tmp_res, value, opts.tree, join_path(node_path, '*'))
                        if err ~= nil then
                            return nil, err
                        end

                        for indx, value in pairs(tmp_res) do
                            if res[indx] == nil then
                                res[indx] = {}
                            end
                            res[indx][i] = value
                        end
                    else
                        res[opts.id][i] = box.NULL
                    end
                end
            end
        end
    end
end

flatten_table_rec = function(res, tbl_node, schema_node, node_path)
    if schema_node[1] == RECORD then
        local subtable = {}

        for i=1, schema_node[2].ddl_fields_num do
            subtable[i] = msgpack.NULL
        end

        local _, err = flatten_table_rec(subtable, tbl_node, schema_node[2].tree, node_path)
        if err ~= nil then
            return nil, err
        end

        return subtable
    end

    if schema_node[1] == UNION then
        local union_opts = schema_node[2]

        local union_key = nil
        for key, _ in pairs(tbl_node) do
            union_key = key
        end

        res[union_opts.id] = union_key

        local schema_subtree = union_opts.tree[union_key]
        local tbl_subtree = tbl_node[union_key]

        return dispatch_node(res, tbl_subtree, schema_subtree, join_path(node_path, union_key))
    end

    if schema_node[1] == NESTED_RECORD then
        local vo_opts = schema_node[2]

        local schema_subtree = vo_opts.tree
        if tbl_node ~= nil then
            res[vo_opts.id] = true
        end

        return dispatch_node(res, tbl_node, schema_subtree, node_path)
    end

    for k, v in pairs(tbl_node) do
        local entry = schema_node[k]

        if entry == nil then
            return nil, serialization_error:new("Unknown key in schema: '%s'", k)
        end

        local _, err = dispatch_node(res, v, entry, join_path(node_path, k))
        if err ~= nil then
            return nil, err
        end
    end
end

local function validate(data, schema, type_name)
    local mdl = schema.__mdl[type_name]
    local ok, err = model_validation.validate_data(mdl, data)
    if not ok then
        return false, string.format('%s: %s', type_name, err)
    end
    return true
end

local function flatten_record(data, schema, type_name)
    checks("table", "table", "string")

    local serialize = schema.__serializers[type_name]
    if serialize == nil then
        return nil, serialization_error:new("Type not found in schema: %q", type_name)
    end

    local ok, tuple = pcall(serialize, data)
    if not ok then
        return nil, tuple
    end

    return tuple
end

local function flatten(data, schema, type_name)
    local tuple, err = flatten_record(data, schema, type_name)
    if err ~= nil then
        return nil, err
    end
    return {tuple}
end

local function unflatten_record(record, schema, type_name)
    local deserialize = schema.__deserializers[type_name]
    if deserialize == nil then
        return nil, serialization_error:new("Type not found in schema: '%s'", type_name)
    end

    return deserialize(record)
end

local function unflatten(record_tbl, schema, type_name)
    checks("table", "table", "string")

    local deserialize = schema.__deserializers[type_name]
    if deserialize == nil then
        return nil, serialization_error:new("Type not found in schema: '%s'", type_name)
    end

    local results = {}
    for _, record in ipairs(record_tbl) do
        local result, err = deserialize(record)
        if err ~= nil then
            return nil, err
        end

        table.insert(results, result)
    end

    return results
end

local function new(mdl, ddl)
    mdl = mdl or {}
    checks("table", "table")

    vars.formats = {}

    local err
    local serializers = {}
    for _, type_entry in ipairs(mdl) do
        if type_entry.indexes ~= nil then
            serializers[type_entry.name], err = gen_schema_root(ddl, type_entry)
            if err ~= nil then
                return nil, err
            end
        end
    end

    local types = {}
    for _, record in ipairs(mdl) do
        types[record.name] = record
    end

    local __serializers, __deserializers = model_serializer.new(mdl, ddl)
    serializers.__serializers = __serializers
    serializers.__deserializers = __deserializers
    serializers.__mdl = types
    return serializers
end

local SCALAR_TYPES = {
    [VALUE] = true,
    [DATE_TIME] = true,
    [DATE] = true,
    [TIME] = true,
    [DECIMAL] = true,
    [UUID] = true,
}

local function is_scalar_node(node)
    return SCALAR_TYPES[node[1]] == true
end

local function is_array_node(node)
    return node[1] == ARRAY
end

local function is_single_container_node(node)
    return node[1] == RECORD or node[1] == NESTED_RECORD or node[1] == UNION
end

return {
    VALUE=VALUE,
    ARRAY=ARRAY,
    RECORD=RECORD,
    NESTED_RECORD=NESTED_RECORD,
    UNION=UNION,
    DATE_TIME=DATE_TIME,
    DATE=DATE,
    TIME=TIME,
    DECIMAL=DECIMAL,
    UUID=UUID,

    new = new,
    validate = validate,
    flatten = flatten,
    flatten_record = flatten_record,
    dispatch_node = dispatch_node,
    unflatten = unflatten,
    unflatten_record = unflatten_record,

    is_scalar_node = is_scalar_node,
    is_array_node = is_array_node,
    is_single_container_node = is_single_container_node,
    field_id_by_name = field_id_by_name,

    build_unknown_enum_error = build_unknown_enum_error,
}
