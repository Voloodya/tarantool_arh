local model = require('common.model')
local utils = require('common.utils')
local errors = require('errors')
local checks = require('checks')
local tenant = require('common.tenant')

local json = require('json')

local model_ddl_error = errors.new_class('model_ddl_error')

local INTERNAL_FIELDS = {
    version = true,
    bucket_id = true,
}

local collation_map = {
    binary = nil,
    case_sensitivity = 'unicode',
    case_insensitivity = 'unicode_ci',
}

local function join_path(lhs, rhs)
    if lhs == '' or lhs == nil then
        return rhs
    end

    return lhs .. '.' .. rhs
end

local function fields_by_name(tbl)
    local res = {}

    if tbl == nil then
        return res
    end

    for _, entry in ipairs(tbl) do
        res[entry.name] = entry
    end

    return res
end

local function get_space_prefix()
    return 'storage_'
end

local function get_history_space_prefix()
    return 'history_storage_'
end

local function expiration_space_prefix()
    return 'expiration_storage_'
end

local function get_vinyl_space_prefix()
    return 'vinyl_storage_'
end

local function get_space_name(type_name, tenant_prefix)
    return tenant_prefix .. get_space_prefix() .. type_name
end

local function get_history_space_name(type_name, tenant_prefix)
    return tenant_prefix .. get_history_space_prefix() .. type_name
end

local function get_expiration_space_name(type_name, tenant_prefix)
    return tenant_prefix .. expiration_space_prefix() .. type_name
end

local function get_vinyl_space_name(type_name, tenant_prefix)
    return tenant_prefix .. get_vinyl_space_prefix() .. type_name
end

local type_map = {
    boolean='boolean',
    int='integer',
    long='integer',
    float='number',
    double='number',
    bytes='string',
    string='string',
    decimal='decimal',
    uuid='uuid',
    any='any',
}

local function avro_type_to_tarantool(type_entry)
    local type_name = type_entry.type or type_entry
    if model.is_primitive_type(type_entry) then
        local res = type_map[type_name]

        if res ~= nil then
            return res
        end
    end

    return nil, model_ddl_error:new("Unsupported avro type: '%s'", type_name)
end

local function get_field_type(type_entry)
    if model.is_primitive_type(type_entry) then
        local type_name = type_entry.type or type_entry
        return avro_type_to_tarantool(type_name)
    end

    if model.is_date_time(type_entry) then
        return 'integer'
    end

    if model.is_date(type_entry) then
        return 'integer'
    end

    if model.is_time(type_entry) then
        return 'integer'
    end

    if model.is_decimal(type_entry) then
        return 'decimal'
    end

    if model.is_uuid(type_entry) then
        return 'uuid'
    end

    if type_entry.type == 'enum' then
        return 'string'
    end
end

local function apply_type(format, types, path, root_type_name, type_entry, opts)
    opts = opts or {}

    local tarantool_type, err = get_field_type(type_entry)
    if err ~= nil then
        return nil, err
    end

    if tarantool_type ~= nil then
        if opts.is_array then
            tarantool_type = 'array'
        end
        table.insert(format, {name = path, type = tarantool_type, is_nullable = true})
        return true
    end

    if model.is_any(type_entry) then
        local field_type = 'any'
        if opts.is_array then
            field_type = 'array'
        end
        table.insert(format, {name=path, type=field_type, is_nullable=true})
        return true
    end

    if model.is_array(type_entry) then
        table.insert(format, {name=path, type='array', is_nullable=true})
        return true
    end

    if type(type_entry) == 'string' then
        if types[type_entry] == nil then
            return nil, model_ddl_error:new("Unknown type: '%s'", type_entry)
        end
        return apply_type(format, types, path, root_type_name, types[type_entry], opts)
    end

    if model.is_union_type(type_entry) then
        local union_field_type = 'string'
        if opts.is_array == true then
            union_field_type = 'array'
        end

        table.insert(format, {name=path, type=union_field_type, is_nullable=true})

        for _, union_type_entry in ipairs(type_entry) do
            if type(union_type_entry) == 'string' then
                if union_type_entry ~= 'null' then
                    return nil, model_ddl_error:new(
                        "Unsupported type in union: %s", union_type_entry)
                end
            else
                local field_path = join_path(path, union_type_entry.name)
                local res, err = apply_type(format, types, field_path, root_type_name, union_type_entry, opts)
                if res == nil then
                    return nil, err
                end
            end
        end

        return true
    end

    if not model.is_record(type_entry) then
        return nil, model_ddl_error:new("Unexpected avro type: '%s', path: '%s'",
                                        type_entry.type, path)
    end

    for _, field in ipairs(type_entry.fields or {}) do
        local subtype = field.type
        local subpath = join_path(path, field.name)

        local save_opts = opts
        if subtype.type == 'array' then
            if opts.is_array == true then
                table.insert(format, {name=subpath, type='array', is_nullable=true})
                goto continue
            end
            opts = {is_array = true}
            subtype = subtype.items
        end

        if model.is_record(subtype) then
            local record_root_type = 'boolean'
            if opts.is_array == true then
                record_root_type = 'array'
            end

            table.insert(format, {name=subpath, type=record_root_type, is_nullable=true})
        end

        local res, err = apply_type(format, types, subpath, root_type_name, subtype, opts)
        if res == nil then
            return nil, err
        end
        opts = save_opts
        ::continue::
    end

    -- Don't consider primary index of non-root type
    if path ~= '' then
        return true
    end

    local parts_of_primary_index = {}
    if type_entry.indexes ~= nil then
        local primary_index = type_entry.indexes[1] or {}

        local parts = {primary_index}
        if type(primary_index) == 'table' then
            parts = primary_index.parts or {}
        end

        for _, part in ipairs(parts) do
            local field_path = join_path(path, part)
            table.insert(parts_of_primary_index, field_path)
        end
    end

    if #parts_of_primary_index > 0 then
        for _, entry in ipairs(format) do
            if utils.has_value(parts_of_primary_index, entry.name) then
                entry.is_nullable = false
            end
        end
    end

    return true
end

local function apply_indexes(type_entry, types, enable_versioning)
    if type(type_entry) ~= 'table' then
        return nil, model_ddl_error:new("Wrong type of root type entry: '%s'", type(type_entry))
    end
    assert(type_entry.indexes ~= nil)

    local indexes = {}
    local primary_index = type_entry.indexes[1]
    local not_nullable_fields = {}
    for _, part_name in ipairs(primary_index.parts) do
        not_nullable_fields[part_name] = true
    end

    local pkey_parts = {}

    for i, index in ipairs(type_entry.indexes) do
        local is_primary = i == 1
        local parts = {}

        if type(index) == 'table' then
            utils.append_table(parts, index.parts)
        else
            table.insert(parts, index)
        end

        local index_name = index.name
        if index_name == nil then
            if #parts == 1 then
                index_name = parts[1]
            else
                return nil, model_ddl_error:new("Index name of type '%s' is not defined", type_entry.name)
            end
        end

        local result_parts = {}

        local has_multikey_part = false
        for _, field_path in ipairs(parts) do
            local err
            local field_type = nil
            local part_path = nil
            if not INTERNAL_FIELDS[field_path] then
                field_type, err = model.get_field_type(type_entry, field_path, types)
                if err ~= nil then
                    return nil, err
                end
                if type(field_type) == 'table' and field_type.type == 'array' then
                    field_type = field_type.items
                end

                local index_path, err = model.index_part_path(type_entry, field_path, types)
                if err ~= nil then
                    return nil, err
                end

                if index_path.postfix ~= nil then
                    if has_multikey_part == true then
                        return nil, model_ddl_error:new('Index %q contains several multikey parts', index.name)
                    end

                    part_path = '[*]'
                    has_multikey_part = true
                end
            end

            local collation = nil
            field_type = get_field_type(field_type)
            if field_type == 'string' then
                collation = collation_map[index.collation]
            end

            table.insert(result_parts, {
                field = field_path,
                type = field_type,
                is_nullable = not_nullable_fields[field_path] == nil,
                collation = collation,
                path = part_path,
            })
        end

        if is_primary == true and enable_versioning == true then
            table.insert(result_parts, {
                field = 'version',
                type = 'unsigned',
                is_nullable = false,
            })
        end

        if is_primary then
            pkey_parts = result_parts
        end

        -- Add primary key fields at end of secondary non-unique index to speed-up fully-specified key search
        if not is_primary then
            for _, pkey_part in ipairs(pkey_parts) do
                local found = false
                for _, exist_part in ipairs(result_parts) do
                    if exist_part.field == pkey_part.field then
                        found = true
                        break
                    end
                end
                if not found then
                    table.insert(result_parts, pkey_part)
                end
            end
        end

        local hint = index.use_hint
        if hint == true and has_multikey_part == true then
            return nil, model_ddl_error:new('Multikey index %q can not use hint', index.name)
        end

        table.insert(indexes, {
            name = index_name,
            parts = result_parts,
            unique = is_primary,
            hint = hint,
        })
    end
    return indexes
end

-- The order is following:
--  1. Primary key fields
--  2. bucket_id
--  3. Rest of fields in alphabetical order
local function sort_format(fields, type_entry)
    local rev_format = {}
    for i, field in ipairs(fields) do
        rev_format[field.name] = i
    end

    fields = table.copy(fields)

    local format = {}
    for _, part in ipairs(type_entry.indexes[1].parts) do
        local num = rev_format[part]
        table.insert(format, fields[num])
        fields[num] = nil
    end

    local version_num = rev_format['version']
    if version_num ~= nil then
        table.insert(format, fields[version_num])
        fields[version_num] = nil
    end

    local bucket_id_num = rev_format['bucket_id']
    table.insert(format, fields[bucket_id_num])
    fields[bucket_id_num] = nil

    local rest = {}
    for _, field in pairs(fields) do
        table.insert(rest, field)
    end
    table.sort(rest, function(lhs, rhs) return lhs.name < rhs.name end)

    for _, field in ipairs(rest) do
        table.insert(format, field)
    end

    return format
end

local function add_bucket_id_index(indexes)
    table.insert(indexes, {
        name = 'bucket_id',
        parts = {{field = 'bucket_id', is_nullable = false, type = 'unsigned'}},
        unique = false,
    })
end

local function generate_record_ddl_impl(type_entry, types, is_versioning_enabled)
    checks('table', 'table', '?boolean')
    local format = {}
    local ddl_entry_name = type_entry.name
    local res, err = apply_type(format, types, "", ddl_entry_name, type_entry)
    if res == nil then
        return nil, err
    end

    table.insert(format, {name='bucket_id', type='unsigned', is_nullable=false})

    if is_versioning_enabled == true then
        table.insert(format, {name='version', type='unsigned', is_nullable=false})
    end

    format = sort_format(format, type_entry)

    local tenant_prefix = tenant.prefix()
    local space_name = get_space_name(ddl_entry_name, tenant_prefix)
    local indexes, err = apply_indexes(type_entry, types, false)
    if err ~= nil then
        return nil, err
    end
    add_bucket_id_index(indexes)

    local history_space_name
    local expiration_space_name
    local vinyl_space_name
    local history_indexes
    local expiration_indexes
    if is_versioning_enabled == true then
        history_indexes, err = apply_indexes(type_entry, types, true)
        if err ~= nil then
            return nil, err
        end
        add_bucket_id_index(history_indexes)

        local primary_index = table.deepcopy(history_indexes[1])
        primary_index.type = 'HASH'
        expiration_indexes = {primary_index}
        add_bucket_id_index(expiration_indexes)

        history_space_name = get_history_space_name(ddl_entry_name, tenant_prefix)
        expiration_space_name = get_expiration_space_name(ddl_entry_name, tenant_prefix)
        vinyl_space_name = get_vinyl_space_name(ddl_entry_name, tenant_prefix)
    end

    local affinity
    -- affinity is keys for sharding objects
    if type_entry.affinity ~= nil then
        if type(type_entry.affinity) == 'string' then
            affinity = {type_entry.affinity}
        else
            affinity = type_entry.affinity
        end
    end

    local ddl = {
        type_name = type_entry.name,
        format = format,
        indexes = indexes,
        space_name = space_name,
        affinity = affinity,
    }

    if is_versioning_enabled == true then
        ddl.history_indexes = history_indexes
        ddl.history_space_name = history_space_name
        ddl.expiration_indexes = expiration_indexes
        ddl.expiration_space_name = expiration_space_name
        ddl.vinyl_space_name = vinyl_space_name
    end
    return ddl
end

local function collect_types_map(mdl)
    local types = {}
    for _, t in ipairs(mdl) do
        types[t.name] = t
    end
    return types
end

local function generate_record_ddl(type_name, mdl, is_versioning_enabled)
    local types = collect_types_map(mdl)
    local type_entry = types[type_name]

    if type_entry.indexes ~= nil then
        local ddl, err = generate_record_ddl_impl(type_entry, types, is_versioning_enabled)
        if err ~= nil then
            return nil, err
        end
        return ddl
    end
end

local function generate_ddl(mdl, versioning)
    versioning = versioning or {}
    local types = collect_types_map(mdl)

    local ddl = {}
    local err
    for _, type_entry in ipairs(mdl) do
        if type_entry.indexes ~= nil then
            local type_versioning = versioning[type_entry.name]

            local is_versioning_enabled = false
            if type_versioning ~= nil and type_versioning.enabled ~= nil then
                is_versioning_enabled = type_versioning.enabled
            end

            local name = type_entry.name
            ddl[name], err = generate_record_ddl_impl(type_entry, types, is_versioning_enabled)
            if err ~= nil then
                return nil, err
            end
        end
    end

    return ddl
end

-- Hints dynamically changed
local function indexes_equal(lhs, rhs)
    return lhs.name == rhs.name and
        lhs.unique == rhs.unique and
        utils.cmpdeeply(lhs.parts, rhs.parts)
end

local function validate_indexes(type_name, old_indexes, new_indexes)
    if old_indexes ~= nil and #old_indexes > 0 then
        if new_indexes == nil or #new_indexes == 0 then
            return nil, model_ddl_error:new("Attempt to remove primary index for type '%s'", type_name)
        end

        local old_primary_index = old_indexes[1]
        local new_primary_index = new_indexes[1]

        if not indexes_equal(old_primary_index, new_primary_index) then
            return false, model_ddl_error:new("Attempt to redefine primary index for type '%s'. " ..
                "Old: '%s', New: '%s'", type_name, json.encode(old_primary_index), json.encode(new_primary_index))
        end
    end

    local old_indexes_by_name = fields_by_name(old_indexes)
    local new_indexes_by_name = fields_by_name(new_indexes)

    for _, index in ipairs(new_indexes) do
        local index_name = index.name
        if old_indexes_by_name[index_name] ~= nil then
            local old_index = old_indexes_by_name[index_name]
            local new_index = new_indexes_by_name[index_name]

            if old_index.unique ~= new_index.unique then
                return nil, model_ddl_error:new("Attempt to change index unique %q of %q from %q to %q",
                    index_name, type_name, old_index.unique, new_index.unique)
            end
        end
    end

    return true
end

local function validate_affinity(type_name, old_ddl, new_ddl)
    -- previous ddl is exists
    if old_ddl.format ~= nil and #old_ddl.format > 0 then
        if not utils.cmpdeeply(old_ddl.affinity, new_ddl.affinity) then
            return nil, model_ddl_error:new("Attempt to change affinity of %q from %q to %q", type_name,
                table.concat(old_ddl.affinity or {},','),
                table.concat(new_ddl.affinity or {}, ','))
        end
    end
end

local function migrate_format(type_name, old_format, new_format)
    old_format = old_format or {}
    new_format = new_format or {}

    local old_fields_by_name = fields_by_name(old_format)
    local new_fields_by_name = fields_by_name(new_format)

    local added_fields = {}

    for field_name, field in pairs(new_fields_by_name) do
        if old_fields_by_name[field_name] == nil then
            table.insert(added_fields, field)
        else
            local old_field = old_fields_by_name[field_name]
            local new_field = new_fields_by_name[field_name]

            if old_field.type ~= new_field.type then
                return nil, model_ddl_error:new("Attempt to change type of '%s.%s' from '%s' to '%s'",
                                                type_name, field_name, old_field.type, new_field.type)
            end
        end
    end

    added_fields = table.copy(added_fields)
    table.sort(added_fields, function(lhs, rhs) return lhs.name < rhs.name end)
    local merged_format = table.copy(old_format or {})

    for _, entry in ipairs(added_fields) do
        table.insert(merged_format, entry)
    end

    return merged_format
end

local function merge_type_ddl(type_name, old_ddl, new_ddl)
    if old_ddl == nil then
        return new_ddl
    end

    local old_is_versioning_enabled = old_ddl.history_indexes ~= nil
    local new_is_versioning_enabled = new_ddl.history_indexes ~= nil
    if old_is_versioning_enabled ~= new_is_versioning_enabled then
        return nil, model_ddl_error:new('Attempt to switch versioning for %q', type_name)
    end

    local old_indexes = old_ddl.indexes
    local new_indexes = new_ddl.indexes
    local _, err = validate_indexes(type_name, old_indexes, new_indexes)
    if err ~= nil then
        return nil, err
    end

    local old_history_indexes = old_ddl.history_indexes
    local new_history_indexes = new_ddl.history_indexes
    if new_history_indexes ~= nil then
        local _, err = validate_indexes(type_name, old_history_indexes, new_history_indexes)
        if err ~= nil then
            return nil, err
        end
    end

    local _, err = validate_affinity(type_name, old_ddl, new_ddl)
    if err ~= nil then
        return nil, err
    end

    local merged_format, err = migrate_format(type_name, old_ddl.format, new_ddl.format)
    if err ~= nil then
        return nil, err
    end

    -- Can't be change since include only primary key and bucket_id
    local expiration_indexes = new_ddl.expiration_indexes

    return {
        type_name = new_ddl.type_name,
        format = merged_format,
        indexes = new_indexes,
        history_indexes = new_history_indexes,
        expiration_indexes = expiration_indexes,
        space_name = new_ddl.space_name,
        history_space_name = new_ddl.history_space_name,
        expiration_space_name = new_ddl.expiration_space_name,
        vinyl_space_name = new_ddl.vinyl_space_name,
        affinity = new_ddl.affinity,
    }
end

local function merge_ddls(old_ddl, new_ddl)
    local result_ddl = table.copy(old_ddl)

    if new_ddl == nil then
        new_ddl = {}
    end

    for type_name, new_type_ddl in pairs(new_ddl) do
        local old_type_ddl = old_ddl[type_name]

        local res, err = merge_type_ddl(type_name, old_type_ddl, new_type_ddl)
        if res == nil then
            return nil, err
        end

        result_ddl[type_name] = res
    end

    return result_ddl
end

local function drop_removed_indexes(space, indexes)
    local indexes_by_name = {}
    for _, index in ipairs(indexes) do
        indexes_by_name[index.name] = index
    end

    for name, index in pairs(space.index) do
        if type(name) == 'string' and indexes_by_name[name] == nil then
            index:drop()
        end
    end
end

local function create_space(space_name, format, indexes, opts)
    opts = opts or {}
    local space = box.schema.space.create(space_name, {
        if_not_exists = true,
        temporary = opts.temporary,
        engine = opts.engine,
    })
    space:format(format)

    for _, index in ipairs(indexes) do
        if space.index[index.name] ~= nil then
            space.index[index.name]:alter({
                unique = index.unique,
                parts = index.parts,
                hint = index.hint,
            })
        else
            space:create_index(index.name, {
                unique = index.unique,
                parts = index.parts,
                hint = index.hint,
                if_not_exists = true,
            })
        end
    end
    drop_removed_indexes(space, indexes)
    return space
end

local vinyl_format = {
    {name = 'id', type = 'string'},
    {name = 'version', type = 'unsigned'},
    {name = 'bucket_id', type = 'unsigned'},
    {name = 'data', type = 'array'}, -- tuple
}
local vinyl_indexes = {
    {name = 'id', parts = {
        {field = 'id', is_nullable = false, type = 'string'},
        {field = 'version', is_nullable = false, type = 'unsigned'},
    }, unique = true},
    {name = 'version', parts = {{field = 'version', is_nullable = false, type = 'unsigned'}}, unique = false},
    {name = 'bucket_id', parts = {{field = 'bucket_id', is_nullable = false, type = 'unsigned'}}, unique = false},
}

local function apply_type_ddl(ddl, opts)
    opts = opts or {}

    local space_name = ddl.space_name
    local history_space_name = ddl.history_space_name
    local expiration_space_name = ddl.expiration_space_name
    local vinyl_space_name = ddl.vinyl_space_name

    if opts.prefix ~= nil then
        space_name = opts.prefix .. space_name
        if history_space_name ~= nil then
            history_space_name = opts.prefix .. history_space_name
        end
        if expiration_space_name ~= nil then
            expiration_space_name = opts.prefix .. expiration_space_name
        end
    end

    if opts.postfix ~= nil then
        space_name = space_name .. opts.postfix
        if history_space_name ~= nil then
            history_space_name = history_space_name .. opts.postfix
        end
        if expiration_space_name ~= nil then
            expiration_space_name = expiration_space_name .. opts.postfix
        end
    end

    -- Transactional DDL works only for single-yield statements
    -- This mean that we can only create empty space and indexes
    -- But could not modify spaces that contain data
    -- See tarantool gh-4083
    local space = box.space[space_name]
    if space == nil then
        box.begin()
    end

    local memtx_opts = {
        engine = 'memtx',
        temporary = opts.temporary,
    }

    local spaces = {}
    if history_space_name ~= nil then
        local s, err = model_ddl_error:pcall(create_space, history_space_name,
            ddl.format, ddl.history_indexes, memtx_opts)
        if err ~= nil then
            box.rollback()
            return nil, err
        end
        spaces.history_space = s
    end


    -- We need only history space for cold storage view
    if opts.only_history_space ~= true then
        local s, err = model_ddl_error:pcall(create_space, space_name, ddl.format, ddl.indexes, memtx_opts)
        if err ~= nil then
            box.rollback()
            return nil, err
        end
        spaces.space = s

        if expiration_space_name ~= nil then
            local s, err = model_ddl_error:pcall(create_space, expiration_space_name,
                ddl.format, ddl.expiration_indexes, memtx_opts)
            if err ~= nil then
                box.rollback()
                return nil, err
            end
            spaces.expiration_space = s
        end

        -- Vinyl doesn't allow to create temporary space.
        -- But they are even not actually needed for cold storage view
        if opts.temporary ~= true and vinyl_space_name ~= nil then
            local s, err = model_ddl_error:pcall(create_space, vinyl_space_name, vinyl_format, vinyl_indexes,
                {engine = 'vinyl'})
            if err ~= nil then
                box.rollback()
                return nil, err
            end
            spaces.vinyl_space = s
        end
    end

    box.commit()
    return spaces
end

local function validate_space_format(space_name, ddl_format, space_format)
    -- space_format checks space:format() not format from ddl since
    -- ddl format could have more fields at the end that will be applied
    -- at apply config stage.
    for i, field in ipairs(space_format) do
        local ddl_field = ddl_format[i]
        model_ddl_error:assert(field.name == ddl_field.name,
            "Fields %d in space %q name mismatch. Expected %q, got %q", i, space_name, ddl_field.name, field.name)
        model_ddl_error:assert(field.type == ddl_field.type,
            "Field %d (%s) type in space %q name mismatch. Expected %q, got %q",
            i, field.name, space_name, ddl_field.type, field.type)
        local field_is_nullable = (field.is_nullable == true)
        local ddl_field_is_nullable = (ddl_field.is_nullable == true)
        model_ddl_error:assert(field_is_nullable == ddl_field_is_nullable,
            "Field %d (%s) nullability in space %q name mismatch. Expected %q, got %q",
            i, field.name, space_name, ddl_field_is_nullable, field_is_nullable)
    end
end

local function validate_space_indexes(space_name, ddl_indexes, space_indexes, format)
    -- It's enough to validate only primary key.
    -- Other indexes will be altered automatically.

    local space_pk = space_indexes[0]
    local ddl_pk = ddl_indexes[1]

    local format_fields = {}
    for i, field in ipairs(format) do
        format_fields[field.name] = i
    end

    model_ddl_error:assert(#space_pk.parts == #ddl_pk.parts,
        "Primary key length in space %q mismatch. Expected %s, got: %s", space_name,
        json.encode(ddl_pk.parts), json.encode(space_pk.parts))

    for i, space_pk_part in ipairs(space_pk.parts) do
        local ddl_pk_part = ddl_pk.parts[i]
        local ddl_fieldno = format_fields[ddl_pk_part.field]

        model_ddl_error:assert(ddl_fieldno == space_pk_part.fieldno,
            "Primary field %d (%s) part number mismatch in space %q. Expected %s, got %s",
            i, ddl_pk_part.field, space_name, ddl_fieldno, space_pk_part.fieldno)

        model_ddl_error:assert(space_pk_part.type == ddl_pk_part.type,
            "Primary field %d (%s) part type mismatch in space %q. Expected %s, got %s",
            i, ddl_pk_part.field, space_name, ddl_pk_part.type, space_pk_part.type)
    end
end

local function validate_ddl(ddl, migrations)
    if type(box.cfg) == 'function' then
        return
    end

    if migrations == nil then
        migrations = {}
    end

    local migrated_types = {}
    for _, migration_section in pairs(migrations) do
        migrated_types[migration_section.type_name] = true
    end

    for type_name, type_ddl in pairs(ddl) do
        if migrated_types[type_name] == nil then
            local space_name = type_ddl.space_name
            local history_space_name = type_ddl.history_space_name
            local expiration_space_name = type_ddl.expiration_space_name
            local vinyl_space_name = type_ddl.vinyl_space_name

            local space = box.space[space_name]
            local ddl_format = type_ddl.format
            if space ~= nil then
                validate_space_format(space_name, ddl_format, space:format())
                validate_space_indexes(space_name, type_ddl.indexes, space.index, ddl_format)
            end

            local history_space = box.space[history_space_name]
            if history_space ~= nil then
                validate_space_format(history_space_name, ddl_format, history_space:format())
                validate_space_indexes(history_space_name, type_ddl.history_indexes, history_space.index, ddl_format)
            end

            local expiration_space = box.space[expiration_space_name]
            if expiration_space ~= nil then
                validate_space_format(expiration_space_name, ddl_format, expiration_space:format())
                validate_space_indexes(expiration_space_name, type_ddl.expiration_indexes,
                    expiration_space.index, ddl_format)
            end

            local vinyl_space = box.space[vinyl_space_name]
            if vinyl_space ~= nil then
                validate_space_format(vinyl_space_name, vinyl_format, vinyl_space:format())
                validate_space_indexes(vinyl_space_name, vinyl_indexes, vinyl_space.index, vinyl_format)
            end
        end
    end
end

local function apply_ddl(ddl, opts)
    for _, type_ddl in pairs(ddl) do
        local res, err = apply_type_ddl(type_ddl, opts)

        if res == nil then
            return nil, err
        end
    end

    return true
end

local function migrate_ddl(types, old_ddl, versioning)
    checks('?string', '?table', '?table')

    if versioning == nil then
        versioning = {}
    end
    local versioning_map = {}
    for _, section in ipairs(versioning) do
        versioning_map[section.type] = section
    end

    local new_mdl, err = model.load_string(types)
    if new_mdl == nil then
        return nil, err
    end

    local new_ddl, err = generate_ddl(new_mdl, versioning_map)
    if new_ddl == nil then
        return nil, err
    end

    if old_ddl == nil then
        return new_ddl
    end

    new_ddl, err = merge_ddls(old_ddl, new_ddl, new_mdl)
    if new_ddl == nil then
        return nil, err
    end

    return new_ddl
end

return {
    merge = merge_ddls,
    apply_ddl = apply_ddl,
    validate_ddl = validate_ddl,
    apply_type_ddl = apply_type_ddl,
    migrate_ddl = migrate_ddl,
    get_space_prefix = get_space_prefix,
    get_history_space_prefix = get_history_space_prefix,
    get_vinyl_space_prefix = get_vinyl_space_prefix,
    get_space_name = get_space_name,
    get_history_space_name = get_history_space_name,
    get_expiration_space_name = get_expiration_space_name,
    get_vinyl_space_name = get_vinyl_space_name,
    generate_ddl = generate_ddl,
    generate_record_ddl = generate_record_ddl,
}
