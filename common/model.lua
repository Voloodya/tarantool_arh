local module_name = 'common.model'

local avro = require('avro_schema')
local avro_frontend = require('avro_schema.frontend')
local checks = require('checks')
local errors = require('errors')
local utils = require('common.utils')
local json = require('json')
local sandbox_registry = require('common.sandbox.registry')

local vars = require('common.vars').new(module_name)
vars:new('model')

local model_parsing_error = errors.new_class("model_parsing_error")
local model_to_graphql_error = errors.new_class("model_to_graphql_error")
local type_validation_error = errors.new_class("type_validation_error")
local model_index_error = errors.new_class("model_index_error")
local model_compatibility_error = errors.new_class('model_compatibility_error')
local config_filter = require('common.config_filter.init')

local SYSTEM_FIELDS = {'bucket_id', 'version'}

local validate_type_entry

local function is_nullable(field)
    if type(field) ~= 'table' then
        return false
    end

    return field.nullable == true
end

local function is_array(type_entry)
    return type_entry and type_entry.type == 'array' or false
end

local function is_record(type_entry)
    if type(type_entry) ~= 'table' then
        return false
    end

    return type_entry.type == 'record'
end

local function is_union_type(field)
    if type(field) ~= 'table' then
        return false
    end

    if not utils.is_array(field) then
        return false
    end

    if is_nullable(field) then
        return false
    end

    if #field == 1 and field[1] == 'null' then
        return false
    end

    return true
end

local function get_field_type(type_entry, path, types)
    types = types or {}
    local parts = string.split(path, '.', 1)

    local first = parts[1]
    local rest = parts[2]

    if is_array(type_entry) then
        type_entry = type_entry.items
    end

    if type(type_entry) == 'string' then
        if types[type_entry] ~= nil then
            type_entry = types[type_entry]
        else
            return nil, model_parsing_error:new("Unknown type: %s", type_entry)
        end
    end

    if is_record(type_entry) then
        for _, field in ipairs(type_entry.fields) do
            if rest == nil then
                if field.name == first then
                    return field.type
                end
            else
                local field_type = field.type
                if field.name == first then
                    return get_field_type(field_type, rest, types)
                end
            end
        end
    end

    return nil, model_parsing_error:new("Field '%s' not found in type '%s'",
                                        path, type_entry.name)
end

local function is_field_defined(types, type_entry, field_name)
    return get_field_type(type_entry, field_name, types) ~= nil
end

local function is_index_defined(type_entry, index_name)
    for _, index in ipairs(type_entry.indexes) do
        if type(index) == 'string' then
            if index == index_name then
                return true
            end
        elseif type(index) == 'table' then
            if index.name == index_name then
                return true
            end
        end
    end

    return false
end

local primitive_types = {
    ["null"] = true,
    ["boolean"] = true,
    ["int"] = true,
    ["long"] = true,
    ["float"] = true,
    ["double"] = true,
    ["bytes"] = true,
    ["string"] = true,
    ["any"] = true,
}

local function is_primitive_type(type_entry)
    if type(type_entry) == 'string' then
        return primitive_types[type_entry] == true
    elseif type(type_entry) == 'table' then
        return primitive_types[type_entry.type] == true and type_entry.logicalType == nil
    end

    return false
end

-- https://avro.apache.org/docs/1.8.1/spec.html#Logical+Types
local function is_primitive_or_derived_type(type_entry)
    if is_primitive_type(type_entry) then
        return true
    end

    return primitive_types[type_entry.type] == true
end

local function is_date_time(type_entry)
    if type(type_entry) ~= 'table' then
        return false
    end

    return type_entry.logicalType == 'DateTime'
end

local function is_string(type_entry)
    if type(type_entry) ~= 'table' then
        return type_entry == 'string'
    else
        return type_entry.logicalType == nil and type_entry.type == 'string'
    end
end

local function is_date(type_entry)
    if type(type_entry) ~= 'table' then
        return false
    end

    return type_entry.logicalType == 'Date'
end

local function is_time(type_entry)
    if type(type_entry) ~= 'table' then
        return false
    end

    return type_entry.logicalType == 'Time'
end

local function is_decimal(type_entry)
    if type(type_entry) ~= 'table' then
        return false
    end

    return type_entry.logicalType == 'Decimal'
end

local function is_uuid(type_entry)
    if type(type_entry) ~= 'table' then
        return false
    end

    return type_entry.logicalType == 'UUID'
end

local function is_any(type_entry)
    if type(type_entry) ~= 'table' then
        return type_entry == 'any'
    end
    return is_any(type_entry.type)
end

local function is_system_field(name)
    for _, system_name in ipairs(SYSTEM_FIELDS) do
        if system_name == name then
            return true
        end
    end
    return false
end

local function index_part_path(type_entry, field_path, types)
    local field_path_parts = field_path:split('.')

    local found_arr = false

    local current_path = {}
    local index_prefix = {}
    local index_postfix = {}
    local last_type = nil
    for _, part in ipairs(field_path_parts) do
        table.insert(current_path, part)
        if not found_arr then
            table.insert(index_prefix, part)
        else
            table.insert(index_postfix, part)
        end

        local field_type
            = get_field_type(type_entry, table.concat(current_path, '.'), types)

        if is_array(field_type) then
            if found_arr == true then
                return nil, model_index_error:new('Nested arrays for multikey index is not supported')
            end
            found_arr = true
        end

        last_type = field_type
    end

    if not found_arr then
        index_postfix = nil
    end

    return {
        prefix = index_prefix,
        postfix = index_postfix,
        type = last_type,
    }
end

local function validate_function(types, type_entry, fun)
    if type(type_entry) == 'string' then
        if types[type_entry] == nil then
            return nil, type_validation_error:new('Type not defined: %s',
                                                  tostring(type_entry))
        end
        type_entry = types[type_entry]
    end

    if type(fun) ~= 'table' then
        return nil, type_validation_error:new(
            'Function definition should be a table in type \'%s\'',
                                             type_entry.name)
    end

    if fun.name == nil or type(fun.name) ~= 'string' then
        return nil, type_validation_error:new(
            "Unnamed function in type '%s'",
            type_entry.name)
    end

    if fun.type == nil then
        return nil, type_validation_error:new(
            "Function '%s' in type '%s' doesn't have field 'type'",
            fun.name, type_entry.name)
    end

    if fun.ref == nil then
        return nil, type_validation_error:new(
            'Function "%s" in type "%s" doesn\'t have field "ref"',
            fun.name, type_entry.name)
    end

    if type(fun.ref) ~= 'string' then
        return nil, type_validation_error:new(
            'Function "%s" in type "%s" must have string reference, got "%s"',
            fun.name, type_entry.name, type(fun.ref))
    end

    local sandbox = assert(sandbox_registry.get('tmp'),
        'Sandbox not registered when validating a function')
    local fn, err = sandbox:dispatch_function(fun.ref)
    if not fn then
        return nil, type_validation_error:new(
            'Function "%s" in type "%s" must be correctly defined in config: %s',
            fun.ref,
            type_entry.name,
            err
        )
    end

    for _, argument in ipairs(fun.arguments or {}) do
        if argument.name == nil or type(argument.name) ~= 'string' then
            return nil, type_validation_error:new(
                "Unnamed function argument in type '%s' function '%s'",
                type_entry.name, fun.name)
        end

        if argument.type == nil then
            return nil, type_validation_error:new(
                "Argument '%s' of function '%s' in type '%s' doesn't have type",
                argument.name, fun.name, type_entry.name)
        end

        local res, err = validate_type_entry(types, argument.type, {})
        if res == nil then
            return nil, err
        end
    end

    return true
end

local function validate_affinity(types, type_entry, affinity)
    local checknames
    if type(affinity) == 'string' then
        checknames = {affinity}
    elseif utils.is_array(affinity) then
        checknames = affinity
    else
        return nil, type_validation_error:new(
            "Type %q has invalid affinity. Expected field name or index name, ", type_entry.name)
    end

    local primary_parts = {}
    local index = type_entry.indexes[1]
    if type(index) == 'string' then
        primary_parts[index] = true
    elseif type(index) == 'table' then
        for _, part in ipairs(index.parts) do
            primary_parts[part] = true
        end
    end

    local lostnames = {}
    for _, affname in ipairs(checknames) do
        if is_field_defined(types, type_entry, affname) then
            if primary_parts[affname] ~= true then
                table.insert(lostnames, affname)
            end
        elseif is_index_defined(type_entry, affname) then
            for _, index in ipairs(type_entry.indexes) do
                if index.name == affname then
                    for _, part in ipairs(index.parts) do
                        if primary_parts[part] ~= true then
                            table.insert(lostnames, affname)
                        end
                    end
                    break
                end
            end
        else
            return nil, type_validation_error:new(
                "Type %q has invalid affinity. Expected field name or index name %q", type_entry.name, affname)
        end
    end
    if #lostnames > 0 then
        return nil, type_validation_error:new(
            "Type %q has invalid affinity. Following fields are not found in primary key %q",
            type_entry.name, table.concat(lostnames, ','))
    end

    return true
end

local function validname(name)
    return string.gsub(name, '[_A-Za-z][_0-9A-Za-z]*', '-') == '-'
end

local function validate_indexes(types, type_entry, fields)
    if type_entry.indexes ~= nil then
        if type(type_entry.indexes) ~= 'table' then
            return nil, type_validation_error:new(
                "'indexes' of '%s' should be a table. got: '%s'",
                type_entry.name,
                type(type_entry.indexes))
        end

        if not utils.is_array(type_entry.indexes) then
            return nil, type_validation_error:new(
                "'indexes' of '%s' should be an array. Got a dictionary.",
                type_entry.name,
                type(type_entry.indexes))
        end

        if type_entry.indexes[1] == nil then
            return nil, type_validation_error:new('At least one index should be specified for %q',
                type_entry.name)
        end

        local scanned_indexes = {}
        for index_no, index in ipairs(type_entry.indexes) do
            if type(index) ~= 'string' and type(index) ~= 'table' then
                return nil, type_validation_error:new(
                    "indexes entries of '%s' should be either a table or a string, got: %s",
                    type_entry.name,
                    type(index))
            end

            if type(index) == 'string' then
                if not is_field_defined(types, type_entry, index) then
                    return nil, type_validation_error:new(
                        "index '%s' for type '%s' refers to a missing field",
                        index,
                        type_entry.name)
                end
            end

            if type(index) == 'table' then
                if index.name == nil then
                    return nil, type_validation_error:new(
                        "index of '%s' must have a name",
                        type_entry.name)
                end

                if index.parts == nil then
                    return nil, type_validation_error:new(
                        "index parts of index '%s' for type '%s' are not defined",
                        index.name, type_entry.name)
                end

                if fields[index.name] ~= nil and (#index.parts ~= 1 or index.name ~= index.parts[1]) then
                    return nil, type_validation_error:new(
                        "index '%s' for type '%s' is clashing with one of its fields",
                        index.name,
                        type_entry.name)
                end

                if type(index.parts) ~= "table" then
                    return nil, type_validation_error:new(
                        "index parts of index '%s' for type '%s' should be a table, got: %s",
                        index.name,
                        type_entry.name,
                        type(index.parts))
                end

                if #index.parts == 0 then
                    return nil, type_validation_error:new(
                        "index parts of index '%s' for type '%s' should not be empty",
                        index.name,
                        type_entry.name)
                end

                for _, part in ipairs(index.parts) do
                    if type(part) ~= 'string' then
                        return nil, type_validation_error:new(
                            "index parts of index '%s' for type '%s' should be strings. got: %s",
                            index.name,
                            type_entry.name,
                            type(part))
                    end

                    if not is_field_defined(types, type_entry, part) then
                        return nil, type_validation_error:new(
                            "index part '%s' of index '%s' for type '%s' is not present in the list of fields",
                            part,
                            index.name,
                            type_entry.name)
                    end
                end

                -- Collations
                if index.collation ~= nil then
                    local collations = {'binary', 'case_sensitivity', 'case_insensitivity'}
                    if not utils.has_value(collations, index.collation) then
                        return nil, type_validation_error:new(
                            "index collation %q of index %q for type %q invalid, valid values %q",
                            index.collation, index.name, type_entry.name, table.concat(collations, ','))
                    end
                end

                -- Hints
                if index.use_hint ~= nil then
                    if type(index.use_hint) ~= 'boolean' then
                        return nil, type_validation_error:new(
                            "use_hint option expected to be a boolean for index %q of type %q",
                            index.name, type_entry.name)
                    end
                end
            end

            local index_name = index

            if type(index) == 'table' then
                index_name = index.name
            end

            if not validname(index_name) then
                return nil, type_validation_error:new("bad index name: %q", index.name)
            end

            if scanned_indexes[index_name] ~= nil then
                return nil, type_validation_error:new(
                            "index name '%s' for type '%s' conflicts with other index for the same type",
                            index_name,
                            type_entry.name)
            end

            scanned_indexes[index_name] = index

            local index_parts = index.parts or {index}

            for _, part in ipairs(index_parts) do
                local field_type, err = get_field_type(type_entry, part, types)
                if field_type == nil then
                    return nil, err
                end

                local base_field_type = field_type
                if is_nullable(field_type) then
                    base_field_type = base_field_type.type
                end

                if is_any(base_field_type) then
                    return nil, type_validation_error:new('Field %q with type "any" can not be indexed', part)
                end

                local _, err = index_part_path(type_entry, part, types)
                if err ~= nil then
                    return nil, err
                end

                if index_no == 1 then
                    if is_nullable(field_type) then
                        return nil, type_validation_error:new(
                            "Primary index '%s' for type '%s' refers to nullable field '%s'",
                            index_name,
                            type_entry.name,
                            part)
                    end
                end
            end
        end
    end
    return true
end

local function validate_relations(types, type_entry)
    if type_entry.relations == nil then
        return true
    end

    local field_names = {}
    for _, field in ipairs(type_entry.fields or {}) do
        field_names[field.name] = true
    end

    if type(type_entry.relations) ~= 'table' then
        return nil, type_validation_error:new(
            "'relations' of '%s' should be a table. got: '%s'",
            type_entry.name,
            type(type_entry.relations))
    end


    for _, relation in ipairs(type_entry.relations) do
        if type(relation) ~= 'table' then
            return nil, type_validation_error:new(
                "relation entries of '%s' should be tables, got: %s",
                type_entry.name,
                type(relation))
        end

        if relation.name == nil then
            return nil, type_validation_error:new(
                "relations of '%s' must have a name",
                type_entry.name)
        end

        if not validname(relation.name) then
            return nil, type_validation_error:new("bad relation name: %q", relation.name)
        end

        if field_names[relation.name] then
            return nil, type_validation_error:new(
                "relation name clashing '%s' of type '%s'",
                relation.name,
                type_entry.name)
        end

        if relation.count == nil then
            return nil, type_validation_error:new(
                "'count' property of relation '%s' for type '%s' is not defined",
                relation.name,
                type_entry.name)
        end

        if not utils.has_value({"one", "many"}, relation.count) then
            return nil, type_validation_error:new(
                "'count' property of relation '%s' for type '%s' should either be 'one' or 'many', got: '%s'",
                relation.name,
                type_entry.name,
                relation.count)
        end

        if relation.from_fields == nil then
            return nil, type_validation_error:new(
                "'from_fields' property of relation '%s' for type '%s' is not defined",
                relation.name,
                type_entry.name)
        end

        if relation.to_fields == nil then
            return nil, type_validation_error:new(
                "'to_fields' property of relation '%s' for type '%s' is not defined",
                relation.name,
                type_entry.name)
        end

        if relation.to == nil then
            return nil, type_validation_error:new(
                "'to' property of relation '%s' for type '%s' is not defined",
                relation.name,
                type_entry.name)
        end

        if types[relation.to] == nil then
            return nil, type_validation_error:new(
                "'to' property of relation '%s' for type '%s' points to type '%s' which is not defined",
                relation.name,
                type_entry.name,
                relation.to)
        end

        local to_type = types[relation.to]

        if to_type.indexes == nil or type(to_type.indexes) ~= 'table' then
            return nil, type_validation_error:new(
                "'indexes' of type '%s' must be defined, since it's a target "..
                "of relation '%s' for type '%s'",
                relation.to,
                relation.name,
                type_entry.name)
        end

        local from_indexes = {}
        local to_type_indexes = {}

        for _, index in ipairs(type_entry.indexes or {}) do
            if type(index) == 'string' then
                from_indexes[index] = index
            else
                from_indexes[index.name] = index
            end
        end

        for _, index in ipairs(to_type.indexes) do
            if type(index) == 'string' then
                to_type_indexes[index] = index
            else
                to_type_indexes[index.name] = index
            end
        end

        local from_fields = relation.from_fields
        local from_pure_fields = {}
        if type(from_fields) == 'string' then
            from_fields = {from_fields}
        end

        local to_fields = relation.to_fields
        local to_pure_fields = {}
        if type(to_fields) == 'string' then
            to_fields = {to_fields}
        end

        for _, from_field in ipairs(from_fields) do
            if is_field_defined(types, type_entry, from_field) then
                table.insert(from_pure_fields, from_field)
            elseif from_indexes[from_field] ~= nil then
                for _, val in ipairs(from_indexes[from_field].parts) do
                    table.insert(from_pure_fields, val)
                end
            end
            if not is_field_defined(types, type_entry, from_field) and
            from_indexes[from_field] == nil then
                return nil, type_validation_error:new(
                    "source field '%s' of relation '%s' for type '%s' should be defined "..
                        "either as field or as index",
                    from_field, relation.name, type_entry.name)
            end
        end

        for _, to_field in ipairs(to_fields) do
            if to_type_indexes[to_field] == nil then
                return nil, type_validation_error:new(
                    "relation '%s' of type '%s' should point to index field",
                    relation.name, type_entry.name)
            end

            if to_type_indexes[to_field] ~= nil then
                if to_type_indexes[to_field].parts == nil then
                    table.insert(to_pure_fields, to_field)
                else
                    for _, val in ipairs(to_type_indexes[to_field].parts) do
                        table.insert(to_pure_fields, val)
                    end
                end
            end
            if not is_field_defined(types, to_type, to_field) and
            to_type_indexes[to_field] == nil then
                return nil, type_validation_error:new(
                    "destination field '%s' of relation '%s' for type '%s' should be defined "..
                        "either as field or as index on '%s'",
                    to_field, relation.name, type_entry.name, to_type.name)
            end
        end

        if #from_pure_fields ~= #to_pure_fields then
            return nil, type_validation_error:new(
                "columns for relation %s are not equal",
                relation.name)
        end

        local type_diff = {}
        local from_types = {}
        local to_types = {}

        for _, field in ipairs(from_pure_fields) do
            local fieldtype = get_field_type(type_entry, field, types)
            if type(fieldtype) == 'table' and fieldtype.type == 'array' then
                fieldtype = fieldtype.items
            end
            table.insert(from_types, fieldtype.type or fieldtype)
        end
        for _, field in ipairs(to_pure_fields) do
            local fieldtype = get_field_type(to_type, field, types)
            table.insert(to_types, fieldtype.type or fieldtype)
        end

        if not utils.cmpdeeply(to_types, from_types, type_diff) then
            return nil, type_validation_error:new(
                "relation '%s' for type '%s' to '%s' fields type mismatch %s",
                relation.name, type_entry.name, to_type.name,
                json.encode(type_diff))
        end

    end
    return true
end

local complex_types = {
    array = true,
    record = true,
    enum = true,
}

local function strip_nullable(type_entry)
    if type(type_entry) == 'string' then
        return type_entry
    end

    -- {"type": "string", "is_nullable": true} -> "string"
    -- {"type": "record", "is_nullable": true} -> {"type": "record", "is_nullable": false}
    if type_entry.type ~= nil and complex_types[type_entry.type] == nil and type_entry.logicalType == nil then
        return type_entry.type
    end

    if is_nullable(type_entry) then
        type_entry = table.copy(type_entry)
        type_entry.nullable = false
        type_entry.default = nil
        type_entry.default_function = nil
        type_entry.auto_increment = nil
    end

    return type_entry
end

validate_type_entry = function(types, type_entry, visited)
    if visited == nil then
        visited = {}
    end

    type_entry = strip_nullable(type_entry)
    if type(type_entry) == 'string' then
        if types[type_entry] ~= nil then
            return true
        elseif is_primitive_type(type_entry) then
            return true
        else
            return nil, type_validation_error:new("Unknown type: '%s'", type_entry)
        end
    end

    if type_entry.logicalType ~= nil then
        if is_record(type_entry) then
            return nil, type_validation_error:new('Logical types are prohibited for records')
        else
            local allowed_logical_types = {'Date', 'Time', 'DateTime', 'Decimal', 'UUID'}
            if not utils.has_value(allowed_logical_types, type_entry.logicalType) then
                local type_name
                if type_entry.name ~= nil then
                    type_name = type_entry.name
                elseif type_entry.type == 'array' then
                    type_name = type_entry.items
                else
                    type_name = json.encode(type_entry)
                end
                return nil, type_validation_error:new(
                    "logicalType of '%s' should be one of: '%s', got: '%s'",
                    type_name,
                    table.concat(allowed_logical_types, ', '),
                    type_entry.logicalType)
            end
        end
    end

    if is_array(type_entry) then
        return validate_type_entry(types, type_entry.items, visited)
    end

    local type_name = type_entry.name or type_entry
    if visited[type_name] then
        return nil, type_validation_error:new("Recursive usage of %s", json.encode(type_name,
            {encode_deep_as_nil = true, encode_max_depth = 3}))
    end
    visited[type_name] = true

    local fields = {}
    if type_entry.fields ~= nil then
        for _, field in ipairs(type_entry.fields) do
            fields[field.name] = field
        end
    end

    if is_record(type_entry) then
        local ns = type_entry.namespace
        if ns ~= nil then
            local type_name = type_entry.name:gsub(ns .. '.', '')
            return nil, type_validation_error:new(
                'Schema for type %q contains "namespace" (%s) field that is not supported', type_name, ns)
        end

        if type_entry.indexes ~= nil then
            for _, system_fieldname in ipairs(SYSTEM_FIELDS) do
                if fields[system_fieldname] ~= nil then
                    return nil, type_validation_error:new(
                        'Type %q must not have field with system name %q',
                        type_entry.name, system_fieldname)
                end
            end
        end
    end

    local _, err = validate_indexes(types, type_entry, fields)
    if err ~= nil then
        return nil, err
    end

    local _, err = validate_relations(types, type_entry)
    if err ~= nil then
        return nil, err
    end

    for _, field in ipairs(type_entry.fields or {}) do
        if field.logicalType ~= nil then
            return nil, type_validation_error:new(
                    'logicalType for field %q of type %q specified in bad way. ' ..
                    'Expected {..., "type": {"type": "string", "logicalType": "<logicalType>"}}, got: %s',
                    field.name,
                    type_entry.name,
                    json.encode(field)
                )
        end

        if field.auto_increment then
            if type(field.auto_increment) ~= "boolean" then
                return nil, type_validation_error:new(
                    "property auto_increment of field '%s' for type '%s' should be boolean, got: %s",
                    field.name,
                    type_entry.name,
                    type(field.auto_increment)
                )
            end

            if field.type ~= 'long' then
                return nil, type_validation_error:new(
                    "field '%s' for type '%s' must have type 'long' as it's set as auto-increment",
                    field.name,
                    type_entry.name
                )
            end

            if field.default ~= nil then
                return nil, type_validation_error:new(
                    "field '%s' for type '%s' must have not default value as it's set as auto-increment",
                    field.name,
                    type_entry.name
                )
            end

            if field.default_function ~= nil then
                return nil, type_validation_error:new(
                    "field '%s' for type '%s' must have not default_function as it's set as auto-increment",
                    field.name,
                    type_entry.name
                )
            end
        end

        local field_type = field.type
        if field_type.type == 'array' then
            local res, err = validate_type_entry(types, field_type, visited)
            if res == nil then
                return nil, err
            end

            field_type = field_type.items
        end

        if is_union_type(field_type) then
            for _, union_type in ipairs(field.type) do
                if union_type ~= 'null' and not is_record(union_type) then
                    return nil, type_validation_error:new(
                        "Unsupported type in union: %s", json.encode(union_type))
                else
                    local res, err = validate_type_entry(types, union_type, visited)
                    if res == nil then
                        return nil, err
                    end
                end
            end
        end

        if field.default_function ~= nil then
            if type(field.default_function) ~= 'string' then
                return nil, type_validation_error:new(
                    "field '%s' has default_function parameter with wrong type: got '%s', expected 'string'",
                    field.name,
                    type(field.default_function)
                )
            end

            local sandbox = assert(sandbox_registry.get('tmp'),
                'Sandbox not registered when validating a function')
            local fn, err = sandbox:dispatch_function(field.default_function, {protected = true})
            if not fn then
                return nil, type_validation_error:new(
                    "Unknown default_function '%s' specified for field '%s': %s",
                    field.default_function,
                    field.name,
                    err.str
                )
            end
        end

        if type(field_type) == 'table' then
            local res, err = validate_type_entry(types, field_type, visited)
            if res == nil then
                return nil, err
            end
        end
    end

    for _, fun in ipairs(type_entry.functions or {}) do
        local res, err = validate_function(types, type_entry, fun)
        if res == nil then
            return nil, err
        end
    end

    -- Affinity validation
    if is_record(type_entry) and type_entry.affinity ~= nil then
        if type_entry.indexes == nil then
            return nil, type_validation_error:new("Affinity is allowed only for types with indexes")
        end

        local res, err = validate_affinity(types, type_entry, type_entry.affinity)
        if res == nil then
            return nil, err
        end
    end
    visited[type_name] = nil

    return true
end

local function normalize_indexes(type_entry)
    if type_entry.indexes == nil then
        return type_entry
    end

    local indexes = {}
    for _, index in ipairs(type_entry.indexes) do
        if type(index) == 'string' then
            table.insert(indexes, { name = index, parts = { index } })
        else
            table.insert(indexes, index)
        end
    end
    type_entry.indexes = indexes

    return type_entry
end

local function expand_fields(type_entry, fields)
    if type(fields) == 'table' then
        if #fields ~= 1 then
            return fields
        else
            fields = fields[1]
        end
    end

    if type_entry.indexes ~= nil then
        for _, index in ipairs(type_entry.indexes) do
            if fields == index.name then
                return fields
            end
        end
    end

    return { fields }
end

local function normalize_relations(types, type_entry)
    if type_entry.relations == nil then
        return type_entry
    end

    for _, relation in ipairs(type_entry.relations) do
        relation.from_fields = expand_fields(type_entry, relation.from_fields)
        local type_to = types[relation.to]
        relation.to_fields = expand_fields(type_to, relation.to_fields)
    end

    return type_entry
end

local function normalize_affinity(types, type_entry)
    if type_entry.affinity == nil then
        return type_entry
    end

    if type(type_entry.affinity) == 'string' then
        type_entry.affinity = { type_entry.affinity }
    end

    local new_affinity = {}
    for _, aff_name in ipairs(type_entry.affinity) do
        if is_field_defined(types, type_entry, aff_name) then
            table.insert(new_affinity, aff_name)
        elseif is_index_defined(type_entry, aff_name) then
            for _, index in ipairs(type_entry.indexes) do
                if index.name == aff_name then
                    for _, parts in ipairs(index.parts) do
                        table.insert(new_affinity, parts)
                    end
                end
            end
        else
            return nil, type_validation_error:new(
                'No such field or index %q for affinity of %q', aff_name, type_entry.name)
        end
    end
    type_entry.affinity = new_affinity

    return type_entry
end

local function is_nullable_defined(field)
    if type(field) ~= 'table' then
        return false
    end

    if not utils.is_array(field) then
        return false
    end

    if #field ~= 2 then
        return false
    end

    if field[1] ~= 'null' then
        return false
    end

    return true
end

--[[
    Historically we use ['null', type] definition for nullable types
    (e.g. ['null', 'string'] means nullable string).
    This function converts user-defined type to our internal
    representation: ['null', 'string'] -> {type = 'string', nullable = true}
--]]
local function normalize_defined_type(type_entry)
    if type(type_entry) ~= 'table' then
        return type_entry
    end

    -- Union
    if utils.is_array(type_entry) and not is_nullable_defined(type_entry) then
        for k, v in ipairs(type_entry) do
            type_entry[k] = normalize_defined_type(v)
        end
        return type_entry
    end

    if is_nullable_defined(type_entry) then
        if type(type_entry[2]) == 'string' then
            return {nullable = true, type = type_entry[2]}
        elseif type(type_entry[2]) == 'table' then
            if type_entry[2].type == 'array' then
                type_entry = {
                    nullable = true,
                    type = 'array',
                    items = type_entry[2].items,
                }
            elseif type_entry[2].type == 'record' then
                type_entry = {
                    nullable = true,
                    type = 'record',
                    fields = type_entry[2].fields,
                    name = type_entry[2].name,
                }
            elseif type_entry[2].logicalType ~= nil then
                type_entry = type_entry[2]
                type_entry.nullable = true
                return type_entry
            end
        end
    elseif is_nullable_defined(type_entry.type) then
        type_entry.type = normalize_defined_type(type_entry.type)
        return type_entry
    end

    local xtype = type_entry.type and type_entry.type.type or type_entry.type
    if type(type_entry.type) == 'table' then
        type_entry.type = normalize_defined_type(type_entry.type)
        return type_entry
    end

    if xtype ~= nil and xtype == 'array' then
        type_entry.items = normalize_defined_type(type_entry.items)
    elseif xtype ~= nil and xtype == 'record' then
        type_entry.fields = normalize_defined_type(type_entry.fields)
    end

    return type_entry
end

local function normalize_functions(type_entry)
    if type_entry.functions == nil then
        return type_entry
    end

    local err
    for _, func in ipairs(type_entry.functions) do
        func.type, err = normalize_defined_type(func.type)
        if err ~= nil then
            return nil, err
        end

        if func.arguments ~= nil then
            for _, arg in ipairs(func.arguments) do
                arg.type = normalize_defined_type(arg.type)
            end
        end
    end

    return type_entry
end

local function normalize_model(model)
    local types = {}

    for _, type_entry in ipairs(model) do
        types[type_entry.name] = type_entry
    end

    for _, type_entry in ipairs(model) do
        local res, err = normalize_indexes(type_entry)
        if res == nil then
            return nil, err
        end

        res, err = normalize_functions(type_entry)
        if res == nil then
            return nil, err
        end
    end

    for _, type_entry in ipairs(model) do
        local res, err = normalize_relations(types, type_entry)
        if res == nil then
            return nil, err
        end
    end

    for _, type_entry in ipairs(model) do
        local res, err = normalize_affinity(types, type_entry)
        if res == nil then
            return nil, err
        end
    end

    return model
end

local function validate_root_type(type_entry)
    if is_record(type_entry) and type_entry.logicalType ~= nil then
        return nil, model_to_graphql_error:new("Root type '%s' have a logicalType %s. Expected simple record",
            type_entry.name, type_entry.logicalType)
    end

    return true
end

local function validate_schema(type_list)
    if not utils.is_array(type_list) then
        return nil, model_to_graphql_error:new("Root of the model should be an array")
    end

    local types = {}

    for _, type_entry in ipairs(type_list) do
        if type_entry.name == nil then
            return nil, model_to_graphql_error:new("Type entry doesn't have 'name' attribute defined")
        end

        types[type_entry.name] = type_entry
    end

    for _, type_entry in pairs(types) do
        local res, err
        res, err = validate_root_type(type_entry)
        if res == nil then
            return nil, err
        end

        res, err = validate_type_entry(types, type_entry, {})
        if res == nil then
            return nil, err
        end
    end

    return true
end

local function fold_nullable(mdl)
    if type(mdl) ~= 'table' then
        return mdl
    end

    -- Union
    if utils.is_array(mdl) and not is_nullable_defined(mdl) then
        for k, v in ipairs(mdl) do
            mdl[k] = fold_nullable(v)
        end
        return mdl
    end

    if is_nullable_defined(mdl) then
        if type(mdl[2]) == 'string' then
            return mdl[2]..'*'
        elseif type(mdl[2]) == 'table' then
            if mdl[2].type == 'array' then
                mdl = {type = 'array*', items = mdl[2].items}
            elseif mdl[2].type == 'record' then
                mdl = {type = 'record*', fields = mdl[2].fields, name = mdl[2].name}
            elseif mdl[2].logicalType ~= nil then
                mdl = mdl[2]
                mdl.type = mdl.type .. '*'
                return mdl
            end
        end
    elseif is_nullable_defined(mdl.type) then
        mdl.type = fold_nullable(mdl.type)
        return mdl
    end

    local xtype = mdl.type and mdl.type.type or mdl.type
    if type(mdl.type) == 'table' then
        mdl.type = fold_nullable(mdl.type)
        return mdl
    end

    if xtype ~= nil and xtype:startswith('array') then
        mdl.items = fold_nullable(mdl.items)
    elseif xtype ~= nil and xtype:startswith('record') then
        mdl.fields = fold_nullable(mdl.fields)
    end

    return mdl
end

local function cleanup_model_entry(record, visited)
    if type(record) ~= 'table' then
        return
    end

    if record.name ~= nil then
        if visited[record.name] then
            return
        end
        visited[record.name] = true
    end

    if is_union_type(record.type) then
        for _, union_type_entry in ipairs(record.type) do
            cleanup_model_entry(union_type_entry, visited)
        end
    end

    if record.type == 'record' then
        record.indexes = nil
        record.relations = nil
        for _, field in ipairs(record.fields) do
            cleanup_model_entry(field, visited)
        end
    end

    if record.type == 'array' then
        record.indexes = nil
        record.relations = nil
        cleanup_model_entry(record.items, visited)
    end

    -- {"type": {"type": "string", "nullable": true}, "logicalType": ...} ->
    -- {"type": {"type": "string", "nullable": true, "logicalType": ...}}
    if type(record.type) == 'table' and record.logicalType ~= nil then
        record.type.logicalType = record.logicalType
        record.logicalType = nil
    end

    if type(record.type) == 'table' then
        cleanup_model_entry(record.type, visited)
    end
end

-- Drop "indexes"/"relations" entries after types are inlined
local function cleanup_model(model)
    for _, record in ipairs(model) do
        if is_record(record) then
            for _, field in ipairs(record.fields) do
                cleanup_model_entry(field, {})
            end
        end
    end
end

local function load_table(fields)
    fields = table.deepcopy(fields)

    if not utils.is_array(fields) then
        return nil, model_parsing_error:new("Root of the model should be an array")
    end

    local options = {
        preserve_in_ast = {
            "relations", "indexes", "functions",
            "logicalType", "auto_increment",
            "affinity", "default_function", "default",
            "doc", "namespace",
        },
        preserve_in_fingerprint = {
            "relations", "indexes", "functions",
            "logicalType", "auto_increment",
            "affinity", "default_function", "default",
            "namespace",
        },
        utf8_enums = true
    }

    local preprocessed_fields, err = fold_nullable(fields)
    if preprocessed_fields == nil then
        return nil, err
    end

    -- Allow to load empty model
    if next(preprocessed_fields) == nil then
        return {}
    end

    local ok, schema_handle = avro.create(preprocessed_fields, options)
    if not ok then
        return nil, model_parsing_error:new("Can't parse model: %s", tostring(schema_handle))
    end

    local model = avro_frontend.create_schema(preprocessed_fields, options)
    cleanup_model(model)

    local res, err = validate_schema(model)
    if res == nil then
        return nil, err
    end

    setmetatable(model, {avro_handle=schema_handle})

    local normalized, err = normalize_model(model)

    if normalized == nil then
        return nil, err
    end

    return normalized
end

local function load_string(str)
    checks("?string")
    str = str ~= box.NULL and str or ''
    if string.strip(str) == '' then
        return {}
    end

    local ok, fields

    ok, fields = pcall(json.decode, str)

    if not ok then
        return nil, model_parsing_error:new("Can't parse model: %s", tostring(fields))
    end

    return load_table(fields)
end

local types_compatibility = {
    -- It's possible to extend "int" to "long" but not vice versa
    ['int'] = {
        ['long'] = true,
        ['any'] = true,
    },
    ["boolean"] = {
        ['any'] = true,
    },
    ["long"] = {
        ['any'] = true,
    },
    ["float"] = {
        ['any'] = true,
    },
    ["double"] = {
        ['any'] = true,
    },
    ["string"] = {
        ['any'] = true,
    },
    ["enum"] = {
        ['any'] = true,
        ['string'] = true,
    },
}

local sformat = string.format
local function check_subtype_compatibility(old_subtype, new_subtype, path, result)
    local old_xtype = type(old_subtype) == 'string' and old_subtype or old_subtype.type
    local new_xtype = type(new_subtype) == 'string' and new_subtype or new_subtype.type

    if type(old_subtype) == 'table' and old_subtype.logicalType ~= nil then
        old_xtype = old_subtype.logicalType
    end

    if type(new_subtype) == 'table' and new_subtype.logicalType ~= nil then
        new_xtype = new_subtype.logicalType
    end

    if (old_xtype ~= new_xtype) and (types_compatibility[old_xtype] == nil or
        types_compatibility[old_xtype][new_xtype] ~= true) then
        local field_path = table.concat(path, '.')

        table.insert(result, sformat('Field %q had type %q that is not compatible with %q',
            field_path, old_xtype, new_xtype))
    end

    if is_primitive_type(old_subtype) or (type(old_subtype) == 'table' and old_subtype.logicalType ~= nil) then
        return
    end

    if old_subtype.type == 'record' then
        if new_subtype.type == old_subtype.type then
            local oldfieldmap = {}
            for _, field in ipairs(old_subtype.fields) do
                oldfieldmap[field.name] = field
            end

            local newfieldmap = {}
            for _, field in ipairs(new_subtype.fields) do
                newfieldmap[field.name] = field
            end

            for _, old_field in pairs(oldfieldmap) do
                local new_field = newfieldmap[old_field.name]

                table.insert(path, old_field.name)
                if new_field == nil then
                    table.insert(result, sformat('Field %q is missed', table.concat(path, '.')))
                else
                    check_subtype_compatibility(old_field.type, new_field.type, path, result)
                end

                table.remove(path)
            end
        end
    elseif old_xtype == 'enum' then
        if new_xtype == 'enum' then
            local new_symbols = {}
            for _, sym in pairs(new_subtype.symbols) do
                new_symbols[sym] = true
            end

            local name = new_subtype.name
            for _, sym in pairs(old_subtype.symbols) do
                if new_symbols[sym] == nil then
                    table.insert(result, sformat('New enum definition %q does not contain symbol %q',
                        name, sym, table.concat(path, '.')))
                end
            end
        end
    elseif old_subtype.type == 'array' then
        if new_subtype.type == old_subtype.type then
            table.insert(path, '*')
            check_subtype_compatibility(old_subtype.items, new_subtype.items, path, result)
            table.remove(path)
        end
    elseif is_union_type(old_subtype) then
        local old_union_map = {}
        for _, union_subtype in ipairs(old_subtype) do
            if type(union_subtype) == 'table' then
                old_union_map[union_subtype.name] = union_subtype
            end
        end

        local new_union_map = {}
        for _, union_subtype in ipairs(new_subtype) do
            if type(union_subtype) == 'table' then
                new_union_map[union_subtype.name] = union_subtype
            end
        end

        for name, old_union_subtype in pairs(old_union_map) do
            local new_union_subtype = new_union_map[name]
            if new_union_subtype == nil then
                table.insert(result, sformat('Union type %q is missed in %q', name, table.concat(path, '.')))
            else
                table.insert(path, name)
                check_subtype_compatibility(old_union_subtype, new_union_subtype, path, result)
                table.remove(path)
            end
        end
    else
        error(sformat('Unexpected type %s', json.encode(old_subtype)))
    end
end

local function check_type_compatibility(old_type, new_type, old_is_versioning_enabled, new_is_versioning_enabled)
    local result = {}

    -- Hints dynamically changed
    if new_type.indexes == nil or
        old_type.indexes[1].name ~= new_type.indexes[1].name or
            utils.cmpdeeply(old_type.indexes[1].parts, new_type.indexes[1].parts) == false then
        table.insert(result, 'Primary key was changed')
    end

    if old_is_versioning_enabled ~= new_is_versioning_enabled then
        table.insert(result, sformat('Versioning is switched from %q to %q',
            old_is_versioning_enabled, new_is_versioning_enabled))
    end

    check_subtype_compatibility(old_type, new_type, {}, result)
    return result
end

local function make_versioning_map(versioning_cfg)
    if versioning_cfg == nil then
        return {}
    end

    local result = {}
    for _, section in ipairs(versioning_cfg) do
        result[section.type] = section
    end
    return result
end

local function are_models_compatible(old_model, old_expiration, new_cfg)
    checks('?table', '?table', '?table')

    if old_model == nil then
        return {}
    end

    local old_versioning = make_versioning_map(old_expiration)

    sandbox_registry.set_cfg('tmp', new_cfg)
    local new_model, err = load_string(new_cfg['types'] or '')
    if err ~= nil then
        return nil, err
    end

    -- FIXME: Remove expiration
    local new_expiration = new_cfg['versioning'] or new_cfg['expiration']
    local new_versioning = make_versioning_map(new_expiration)

    local new_types = {}
    for _, t in ipairs(new_model) do
        new_types[t.name] = t
    end

    local result = {}
    for _, old_type in ipairs(old_model) do
        if old_type.indexes ~= nil then
            local new_type = new_types[old_type.name]
            if new_type ~= nil then
                local old_type_versioning = old_versioning[old_type.name]
                local new_type_versioning = new_versioning[new_type.name]

                local old_is_versioning_enabled = false
                if old_type_versioning ~= nil and old_type_versioning.enabled ~= nil then
                    old_is_versioning_enabled = old_type_versioning.enabled
                end

                local new_is_versioning_enabled = false
                if new_type_versioning ~= nil and new_type_versioning.enabled ~= nil then
                    new_is_versioning_enabled = new_type_versioning.enabled
                end

                local incompatibilities = check_type_compatibility(old_type, new_type,
                    old_is_versioning_enabled, new_is_versioning_enabled)
                if next(incompatibilities) ~= nil then
                    result[new_type.name] = incompatibilities
                end
            end
        end
    end
    return result
end

local function validate_config(cfg)
    local tenant = require('common.tenant')
    local old_mdl = tenant.get_mdl()
    -- FIXME: Remove expiration
    local old_expiration = tenant.get_cfg_non_null('versioning', 'expiration')
    local compatibility, err = are_models_compatible(old_mdl, old_expiration, cfg)
    if err ~= nil then
        return nil, err
    end

    if next(compatibility) ~= nil then
        local err = model_compatibility_error:new('Models are not compatible: %s',
            json.encode(compatibility))
        return nil, err
    end
    return true
end

local function apply_config(cfg)
    checks("table")
    local _, types_err = config_filter.compare_and_set(cfg, 'types', module_name)
    if types_err ~= nil then
        return vars.model
    end

    local model, err = load_string(cfg['types'] or '')
    if model == nil then
        return nil, err
    end
    vars.model = model
    return model
end

return {
    load_string = load_string,
    validate_type_entry = validate_type_entry,
    get_field_type = get_field_type,
    normalize_defined_type = normalize_defined_type,

    strip_nullable = strip_nullable,
    is_nullable_type = is_nullable,
    is_nullable_defined = is_nullable_defined,
    is_primitive_type = is_primitive_type,
    is_primitive_or_derived_type = is_primitive_or_derived_type,
    is_record = is_record,
    is_array = is_array,
    is_union_type = is_union_type,

    -- types
    is_string = is_string,
    is_date_time = is_date_time,
    is_date = is_date,
    is_time = is_time,
    is_decimal = is_decimal,
    is_uuid = is_uuid,
    is_any = is_any,

    is_system_field = is_system_field,
    are_models_compatible = are_models_compatible,

    index_part_path = index_part_path,
    validate_config = validate_config,
    apply_config = apply_config,
    -- for test purposes
    load = load_table,
}
