local module_name = 'common.model_accessor'

local log = require('log.log').new(module_name)

local checks = require('checks')
local json = require('json')
local fiber = require('fiber')
local msgpack = require('msgpack')

local errors = require('errors')
local cartridge = require('cartridge')

local query_metrics = require('common.document.metrics')
local utils = require('common.utils')
local defaults = require('common.defaults')
local document = require('common.document')
local document_utils = require('common.document.utils')
local document_query_plan = require('common.document.query_plan')
local request_context = require('common.request_context')
local tracing = require('common.tracing')
local version_lib = require('common.version')
local sandbox_registry = require('common.sandbox.registry')
local tenant = require('common.tenant')

local vars = require('common.vars').new(module_name)

local model_ddl = require('common.model_ddl')
local model_flatten = require('common.model_flatten')
local model_updater = require('common.model_updater')
local model_key_def = require('storage.model_key_def')
local model_expiration = require('storage.expiration.model')

vars:new('accessor')
vars:new('cleaners')
vars:new('primary_space_triggers')
vars:new('history_space_triggers')

local last_tuple

local accessor_error = errors.new_class("accessor_error")

local function box_atomic(func, ...)
    box.begin()
    local ok, err = accessor_error:pcall(func, ...)
    if err ~= nil then
        box.rollback()
        return nil, err
    end

    box.commit()

    return ok, err
end

local function get_document_options(options)
    local opts = {
        hard_limits = cartridge.config_get_deepcopy('hard-limits')
    }

    if options ~= nil then
        opts.first = options.first
        opts.after = options.after
        opts.version = options.version
        opts.ignore_hard_limits = options.ignore_hard_limits
        opts.all_versions = options.all_versions
    end

    return opts
end

local function field_id_by_name(format)
    local res = {}

    for id, entry in ipairs(format) do
        res[entry.name] = id
    end

    return res
end

local function get_type_accessor(type_name)
    local type_accessor = vars.accessor[type_name]
    if type_accessor == nil then
        return nil, accessor_error:new("Accessor spec for %q doesn't have a type %q", tenant.name(), type_name)
    end

    return type_accessor
end

local function delete_mode()
    return fiber.self().storage.delete_mode
end

local function set_delete_mode(mode)
    fiber.self().storage.delete_mode = mode
end

local function get_type_spaces(type_name)
    local spec, err = get_type_accessor(type_name)
    if err ~= nil then
        return nil, err
    end

    local spaces = spec.type_spaces
    if spaces ~= nil then
        return spaces
    end

    spaces = {
        spec = spec,
        type_name = type_name,
        space = box.space[spec.space_name],
        history_space = box.space[spec.history_space_name],
        expiration_space = box.space[spec.expiration_space_name],
        vinyl_space = box.space[spec.vinyl_space_name],
    }

    -- Since DDL is replicated asynchronously don't cache any values until all space are available
    if spaces.space ~= nil and
        spaces.history_space ~= nil and
        spaces.expiration_space ~= nil and
        spaces.vinyl_space ~= nil then
        spec.type_spaces = spaces
    end
    return spaces
end

local function cut_version_field(pkey)
    local version_fieldno = #pkey
    local pkey_without_version
    if box.tuple.is(pkey) then
        pkey_without_version = pkey:transform(version_fieldno, version_fieldno)
    else
        pkey_without_version = table.copy(pkey)
        pkey_without_version[version_fieldno] = nil
    end
    return pkey_without_version
end

local function get_tuple_by_pk(type_name, pkey)
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end
    local space = type_spaces.space
    local history_space = type_spaces.history_space

    local pkey_without_version
    if history_space == nil then
        pkey_without_version = pkey
    else
        pkey_without_version = cut_version_field(pkey)
    end

    local tuple = space:get(pkey_without_version)

    if history_space ~= nil then
        local version_fieldno = #pkey
        if tuple ~= nil and tuple.version ~= pkey[version_fieldno] then
            tuple = history_space:get(pkey)
        end
    end

    if tuple == nil then
        return nil, accessor_error:new(
            "Object of type '%s' with pk '%s' doesn't exist",
            type_name, json.encode(pkey))
    end

    return tuple
end

--@tparam string type_name
--@tparam table filter
--@tparam ?table options
--@tparam number options.version
--@tparam string options.first
--@tparam string options.after
local function find_impl(type_name, plan, options)
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local accumulator = {}
    for _, tuple in document.tuple_select(plan, type_spaces, options) do
        table.insert(accumulator, tuple)
    end

    return accumulator
end

local function get_key_def_space_name(type_name)
    local type_spaces = get_type_spaces(type_name)
    local key_def_space_name
    if type_spaces.history_space ~= nil then
        key_def_space_name = type_spaces.history_space.name
    else
        key_def_space_name = type_spaces.space.name
    end
    return key_def_space_name
end

local function add_cursor(tuples, type_name, plan)
    local space_ddl = tenant.get_ddl(type_name)

    local format_len = #space_ddl.format
    if plan.scan_multi_index_field_no == nil then
        local key_def_space_name = get_key_def_space_name(type_name)
        local key_def = model_key_def.get_space_key_def(key_def_space_name, plan.scan_index.name)
        for i, tuple in ipairs(tuples) do
            tuples[i] = tuple:update({{'=', format_len + 1, key_def:extract_key(tuple)}})
        end
    else
        local multikey_index_field = space_ddl.format[plan.scan_multi_index_field_no].name
        local function extract_key(tuple)
            local result = {}
            local multikey_part_entry = 1
            for _, part  in ipairs(plan.scan_index.parts) do
                if part.field ~= multikey_index_field then
                    local field = tuple[part.field]
                    if field == nil then
                        field = box.NULL
                    end
                    table.insert(result, field)
                else
                    local mpos = tuple[format_len + 1].multi_position
                    local field = tuple[part.field][mpos]
                    if field == nil then
                        field = box.NULL
                    end
                    table.insert(result, field)
                    multikey_part_entry = multikey_part_entry + 1
                end
            end
            return result
        end

        for i, tuple in ipairs(tuples) do
            tuples[i] = tuple:update({{'=', format_len + 1, extract_key(tuple)}})
        end
    end
end

local function find(type_name, filter, options)
    options = options or {}
    options.first = options.first or defaults.FIND_LIMIT

    local space_ddl = tenant.get_ddl(type_name)
    local document_opts = get_document_options(options)
    local plan = document_query_plan.new(space_ddl, filter, document_opts)
    local tuples = find_impl(type_name, plan, document_opts)

    add_cursor(tuples, type_name, plan)
    local result = {tuples = tuples}

    if request_context.is_explain_enabled() then
        result.metrics = query_metrics.get()
    end
    return result
end

-- Get next chuck of tuples for repository.pairs
local function find_pairs(type_name, filter, options)
    local chunk_size = defaults.FIND_LIMIT
    options.first = chunk_size
    local result, err = find(type_name, filter, options)
    if err ~= nil then
        return nil, err
    end

    local tuples = result.tuples
    local cursor = nil
    -- Cursor is nil in case these are no tuples to return
    local last_tuple = tuples[chunk_size]
    if last_tuple ~= nil then
        cursor = {scan = last_tuple[#last_tuple]}
    end

    return {cursor = cursor, tuples = tuples}
end

local function count(type_name, filter, options)
    options = options or {}

    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local space_ddl = tenant.get_ddl(type_name)
    local document_opts = get_document_options(options)
    local plan = document_query_plan.new(space_ddl, filter, document_opts)

    local index_name = plan.scan_index.name
    local scan_value = plan.scan_value
    local iterator = plan.op
    local version = plan.version
    local all_versions = plan.all_versions
    -- Only one index is specified and choose only max version.
    -- It's possible to just call :count(scan_value, iterator)
    local is_query_by_index = #plan.filter_conditions == 0 and version == nil

    local result = 0
    if is_query_by_index and all_versions ~= true then
        -- Non-versioning scan, scan by index -> call :count()
        result = type_spaces.space.index[index_name]:count(scan_value, {iterator = iterator})
    elseif is_query_by_index and all_versions == true then
        -- Count "all_versions" by index -> sum :count() for primary and history space
        result = type_spaces.space.index[index_name]:count(scan_value, {iterator = iterator})
        if type_spaces.history_space ~= nil then
            result = result + type_spaces.history_space.index[index_name]:count(scan_value, {iterator = iterator})
        end
    else
        -- In case of scan we should not hang storage
        options.ignore_hard_limits = true
        -- Additional checks, range scans, query with version -> can't perform simply call :count()
        for _ in document.tuple_select(plan, type_spaces, options) do
            result = result + 1
        end
    end

    return result
end

local function get(type_name, pkey, options)
    options = options or {}

    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    -- if versioning is turned off we can just get tuple from space
    if type_spaces.history_space == nil then
        return type_spaces.space:get(pkey)
    end

    local key_def = model_key_def.get_space_key_def(type_spaces.space.name, 0)
    local indexes = {
        primary = type_spaces.space.index[0],
        history = type_spaces.history_space.index[0],
    }
    return document_utils.get_last_tuple_for_version(indexes, key_def, pkey, options.version)
end

local function delete_tuple(type_name, tuple)
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local key_def_space_name = get_key_def_space_name(type_name)
    local key_def = model_key_def.get_space_key_def(key_def_space_name, 0)
    local pkey = key_def:extract_key(tuple)

    local res, err
    local history_space = type_spaces.history_space
    if history_space ~= nil then
        res, err = accessor_error:pcall(history_space.delete, history_space, pkey)
        if err ~= nil then
            return nil, err
        end

        if res == nil then
            local space = type_spaces.space
            local pkey_without_version = cut_version_field(pkey)
            res, err = accessor_error:pcall(space.delete, space, pkey_without_version)
        end
    else
        local space = type_spaces.space
        res, err = accessor_error:pcall(space.delete, space, pkey)
    end

    return res, err
end

local function delete_records(type_name, record_list)
    for _, record in ipairs(record_list) do
        local _, err = delete_tuple(type_name, record)
        if err ~= nil then
            return nil, err
        end
    end
end

local function remove_old_versions_by_record(type_name, record)
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local version_count = model_expiration.get_keep_version_count(type_name)
    if version_count == nil then
        return
    end

    local space = type_spaces.space
    local history_space = type_spaces.history_space

    local key_def = model_key_def.get_space_key_def(space.name, 0)
    local pkey = key_def:extract_key(record)

    local to_delete = {}
    -- Omit one version from primary space
    local limit = version_count - 1
    for _, tuple in history_space:pairs(pkey, {iterator = box.index.REQ}) do
        if limit > 0 then
            limit = limit - 1
        else
            table.insert(to_delete, tuple)
        end
    end

    set_delete_mode(model_expiration.get_delete_mode(type_name))

    for _, tuple in ipairs(to_delete) do
        _, err = delete_tuple(type_name, tuple)
        if err ~= nil then
            break
        end
    end

    set_delete_mode(nil)

    if err ~= nil then
        return nil, err
    end

    return true
end

local function remove_old_versions_by_type_impl(type_name)
    checks('string')

    local version_count = model_expiration.get_keep_version_count(type_name)
    if version_count == nil then
        return true
    end

    -- In case of version count = 1 we simply truncate history space
    if version_count == 1 and model_expiration.get_strategy(type_name) == 'permanent' then
        return true
    end

    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local space_ddl = tenant.get_ddl(type_name)
    local document_opts = {
        version = version_lib.MAX_POSSIBLE_VERSION,
        ignore_hard_limits = true,
    }

    local plan = document_query_plan.new(space_ddl, {}, document_opts)

    local count = 0
    box.begin()
    for _, tuple in document.tuple_select(plan, type_spaces, document_opts) do
        count = count + 1
        remove_old_versions_by_record(type_name, tuple)

        if count % defaults.FORCE_YIELD_LIMIT == 0 then
            box.commit()
            fiber.sleep(0.1)
            box.begin()
        end
    end
    box.commit()

    return true
end

local function remove_old_versions_by_type(type_name)
    if vars.cleaners[type_name] ~= nil and
        vars.cleaners[type_name]:status() ~= 'dead' then
        return true
    end

    vars.cleaners[type_name] = tenant.fiber_new(remove_old_versions_by_type_impl, type_name)
    local tenant_name = tenant.name()
    local task_name = tenant_name .. '_clean_' .. type_name
    vars.cleaners[type_name]:name(task_name, {truncate = true})
    return true
end

local function get_last_tuple_version(type_name, tuple, version)
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local primary_space_name = type_spaces.space.name
    local key_def = model_key_def.get_space_key_def(primary_space_name, 0)
    local pkey = key_def:extract_key(tuple)
    local indexes = {
        primary = type_spaces.space.index[0],
        history = type_spaces.history_space ~= nil and type_spaces.history_space.index[0] or nil,
    }
    return document_utils.get_last_tuple_for_version(indexes, key_def, pkey, version)
end

local function check_version_for_atomic_operation(type_name, record_list, options)
    if options == nil then
        return true
    end
    if options.only_if_version == nil and options.if_not_exists ~= true then
        return true
    end
    if options.only_if_version ~= nil and options.if_not_exists == true then
        return nil, accessor_error:new('Atomic operation failed: Choose either only_if_version or if_not_exists')
    end

    for _, record in ipairs(record_list) do
        local version
        if options.only_if_version ~= nil and options.version ~= nil and options.only_if_version <= options.version then
            version = options.version
        end

        local tuple = get_last_tuple_version(type_name, record, version)
        if tuple == nil and options.if_not_exists ~= true then
            return nil, accessor_error:new('Atomic operation failed: %s version %q is not found', type_name,
                                           options.only_if_version)
        end
        if tuple ~= nil and options.if_not_exists == true then
            return nil, accessor_error:new('Atomic operation failed: %s has version %q but if_not_exists is %q',
                                           type_name, tuple.version, options.if_not_exists)
        end
        if tuple ~= nil and tuple.version ~= options.only_if_version then
            return nil, accessor_error:new('Atomic operation failed: %s has version %q and lock is %q', type_name,
                                           tuple.version, options.only_if_version)
        end
    end
    return true
end

local function get_replication_info(type_name, record_list)
    local info = {}
    if record_list[1] ~= nil then
        local key_def_space_name = get_key_def_space_name(type_name)
        local key_def = model_key_def.get_space_key_def(key_def_space_name, 0)
        for _, record in ipairs(record_list) do
            local key = key_def:extract_key(record)
            table.insert(info, key)
        end
    end
    return info
end

-- TODO: should be redesigned in future:
--  * Without keep_version_count - do nothing
--  * keep_version_count == 1 - just replace record
--  * keep_version_count > 1 - drop last version in history (should work as force version cleanup is implemented)
local function run_keep_version_count_clean(type_name, record_list)
    local version_count = model_expiration.get_keep_version_count(type_name)
    local strategy = model_expiration.get_strategy(type_name)

    -- Skip keep_version_count == 1 + permanent strategy
    -- because record simply replaced
    if version_count == 1 and strategy == 'permanent' then
        return
    end

    -- Check keep_version_count after all records are inserted
    if version_count ~= nil and version_count ~= 0 then
        for _, tuple in ipairs(record_list) do
            local rc, err = remove_old_versions_by_record(type_name, tuple)
            if err ~= nil then
                return rc, err
            end
        end
    end
end

local function extract_version(options)
    if options ~= nil and options.version ~= nil then
        return options.version
    end
    return version_lib.get_new()
end

local function put(type_name, record_list, options)
    local _, err = check_version_for_atomic_operation(type_name, record_list, options)
    if err ~= nil then
        return nil, err
    end

    local result = {}
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local space = type_spaces.space
    local version_field_id = type_spaces.spec.version_field_id

    local version
    if version_field_id ~= nil then
        version = extract_version(options)
    end

    for _, record in ipairs(record_list) do
        if version_field_id ~= nil then
            record[version_field_id] = version
        end
        local res = space:replace(record)
        if res == nil then
            res = last_tuple
        end
        table.insert(result, res)
    end
    last_tuple = nil

    if version_field_id ~= nil then
        local _, err = run_keep_version_count_clean(type_name, result)
        if err ~= nil then
            return nil, err
        end
    end

    return result
end

--@tparam string type_name
--@tparam table record_list
--@tparam table options
--@tparam number options.version
--@tparam number options.only_if_version
--@tparam number options.if_not_exists
local function put_atomic(type_name, record_list, options)
    options = options or {}
    return box_atomic(put, type_name, record_list, options)
end

local function gen_accessor_data(type_entry, ddl)
    local type_name = type_entry.name
    local space_name = ddl[type_name].space_name
    local history_space_name = ddl[type_name].history_space_name
    local expiration_space_name = ddl[type_name].expiration_space_name
    local vinyl_space_name = ddl[type_name].vinyl_space_name
    local format = field_id_by_name(ddl[type_name].format)
    return {
        type_name = type_name,
        space_name = space_name,
        history_space_name = history_space_name,
        expiration_space_name = expiration_space_name,
        vinyl_space_name = vinyl_space_name,
        version_field_id = format['version'],
    }
end

local function new_accessor(mdl, ddl)
    checks("table", "table")

    local accessors = {}
    for _, type_entry in ipairs(mdl) do
        if type_entry.indexes ~= nil then
            local accessor, err = gen_accessor_data(type_entry, ddl)
            if err ~= nil then
                return nil, err
            end

            accessors[type_entry.name] = accessor
        end
    end

    return accessors
end

local function get_primary_space_trigger(type_name, history_space, expiration_space, version_field_id)
    return function(old, new, space_name, op)
        local key_def = model_key_def.get_space_key_def(space_name, 0)
        --[[
            It's quite important to preserve following three layers structure:
              1. Primary space - stores only the latest version;
              2. History space - stores only history records that weren't deleted;
              3. Expiration space - stores history versions.

              We should respect that primary_space:get(id).version > history_space:max(id).version

              The replace schema:
                - Compare previous version of tuple with current tuple version
                - If there is no "previous" tuple then put it in primary space
                - If current version is greater than previous then displace previous version to history
                  space.
                - In case if previous tuple versions are equal to specified - it's just replace
                - If current tuple version is less than previous we put an object to history space
                  and it will be responsibility of history_space_trigger to support consistency between
                  versions.

              The delete schema:
                - permanent delete - promote the latest version from history space and drop current.
                - expiration - should run only for system expiration task,
                  just moves tuples to expiration space as persistent buffer between memtx and vinyl or memtx and file.
        --]]
        if op == 'REPLACE' or op == 'UPDATE' then
            last_tuple = new
            if old == nil then
                return new
            elseif old[version_field_id] == new[version_field_id] then
                -- Replace the latest version of object.
                -- It's possible in case of historical insert one of tuples.
                return new
            elseif old[version_field_id] < new[version_field_id] then
                -- We should update the latest version.
                -- Previous object should be dropped from primary space and
                -- moved to historical space.

                -- Insert old tuple into history space
                history_space:replace(old)

                -- Insert new tuple
                return new
            else -- old[version_field_id] > new[version_field_id] then
                -- In case of historical insert we should simply
                -- put object into historical space.
                history_space:replace(new)
                return old
            end
        elseif op == 'DELETE' then
            local mode = delete_mode()
            if mode == 'expiration' then
                history_space:run_triggers(false)

                local key = key_def:extract_key(old)
                for _, tuple in history_space:pairs(key, {iterator = box.index.REQ}) do
                    local key_with_version = key:update({{'!', -1, tuple.version}})
                    history_space:delete(key_with_version)
                    expiration_space:replace(tuple)
                end
                expiration_space:replace(old)

                history_space:run_triggers(true)

                return
            elseif mode == 'permanent' then
                local key = key_def:extract_key(old)
                history_space:run_triggers(false)
                for _, tuple in history_space:pairs(key, {iterator = box.index.REQ}) do
                    local key_with_version = key:update({{'!', -1, tuple.version}})
                    history_space:delete(key_with_version)
                end
                history_space:run_triggers(true)
            elseif mode == 'default' then
                return
            else
                error(('Unknown delete mode for %q'):format(type_name))
            end
        end
    end
end

local function get_history_space_trigger(type_name, primary_space, expiration_space)
    local key_def = model_key_def.get_space_key_def(primary_space.name, 0)
    return function(old, _, space_name, op)
        local history_space = box.space[space_name]
        --[[
                In order to understand it you must read previous part that's
                placed in get_primary_space_trigger.

                Important (and obvious) notes:
                  - For delete operation "new" argument is always nil
                  - For replace operation "old" non-nil argument it's quite rare case
                    because primary key includes version.
        --]]
        if op == 'DELETE' then
            local mode = delete_mode()
            if mode == 'expiration' then
                history_space:run_triggers(false)

                local key = key_def:extract_key(old)
                local scan_key = key:update({{'!', -1, old.version}})

                expiration_space:replace(old)
                for _, tuple in history_space:pairs(scan_key, {iterator = box.index.LT}) do
                    if key_def:compare_with_key(tuple, key) ~= 0 then
                        break
                    end
                    local key_with_version = key:update({{'!', -1, tuple.version}})
                    history_space:delete(key_with_version)
                    expiration_space:replace(tuple)
                end

                history_space:run_triggers(true)
                return
            elseif mode == 'permanent' then
                local key = key_def:extract_key(old)
                local scan_key = key:update({{'!', -1, old.version}})
                history_space:run_triggers(false)
                for _, tuple in history_space:pairs(scan_key, {iterator = box.index.LT}) do
                    if key_def:compare_with_key(tuple, key) ~= 0 then
                        break
                    end
                    local key_with_version = key:update({{'!', -1, tuple.version}})
                    history_space:delete(key_with_version)
                end
                history_space:run_triggers(true)
            elseif mode == 'default' then
                return
            else
                error(('Unknown delete mode for history %q'):format(type_name))
            end
        end
    end
end

local function get_choose_max_version_trigger(version_field_id)
    return function(old, new, _, op)
        if op == 'REPLACE' or op == 'UPDATE' then
            last_tuple = new
            if old == nil then
                return
            end
            if old[version_field_id] > new[version_field_id] then
                return old
            end
        end
    end
end

local function validate_config(mdl, ddl)
    local accessor, err = new_accessor(mdl, ddl)
    if accessor == nil then
        return nil, err
    end
    return true
end

local function apply_accessor(accessor, primary_space_triggers, history_space_triggers)
    for type_name, type_data in pairs(accessor) do
        local space_name = type_data.space_name
        local history_space_name = type_data.history_space_name
        local expiration_space_name = type_data.expiration_space_name

        local primary_space = box.space[space_name]
        local history_space = box.space[history_space_name]
        local expiration_space = box.space[expiration_space_name]

        if primary_space ~= nil then
            local old_trigger = primary_space_triggers[type_name]
            pcall(primary_space.before_replace, primary_space, nil, old_trigger)
        end

        if history_space ~= nil then
            local old_trigger = history_space_triggers[type_name]
            pcall(history_space.before_replace, history_space, nil, old_trigger)
        end

        if history_space ~= nil and box.info.ro == false then
            local version_field_id = type_data.version_field_id

            local keep_version_count = model_expiration.get_keep_version_count(type_name)
            local expiration_strategy = model_expiration.get_strategy(type_name)
            local keep_only_one_version = keep_version_count == 1 and expiration_strategy == 'permanent'

            if not keep_only_one_version then
                local trigger = get_primary_space_trigger(type_name, history_space, expiration_space, version_field_id)
                primary_space:before_replace(trigger)
                vars.primary_space_triggers[type_name] = trigger
            else
                local trigger = get_choose_max_version_trigger(version_field_id)
                primary_space:before_replace(trigger)
                vars.primary_space_triggers[type_name] = trigger
            end

            if not keep_only_one_version then
                local trigger = get_history_space_trigger(type_name, primary_space, expiration_space)
                history_space:before_replace(trigger)
                vars.history_space_triggers[type_name] = trigger
            end

            if keep_only_one_version and history_space:len() > 0 then
                history_space:truncate()
            end
        end
    end
end

local function apply_config(mdl, ddl)
    local accessor, err = new_accessor(mdl, ddl)
    if accessor == nil then
        return nil, err
    end

    vars.ddl = ddl
    vars.accessor = accessor
    vars.cleaners = vars.cleaners or {}
    vars.sandbox = sandbox_registry.get('active')

    local primary_space_triggers = vars.primary_space_triggers or {}
    local history_space_triggers = vars.history_space_triggers or {}
    vars.primary_space_triggers = {}
    vars.history_space_triggers = {}

    apply_accessor(accessor, primary_space_triggers, history_space_triggers)
    return true
end

local function call_for_type_spaces(type_name, fun, opts)
    opts = opts or {}

    local tenant_prefix = tenant.prefix()
    local space_name = model_ddl.get_space_name(type_name, tenant_prefix)
    local space = box.space[space_name]
    if space == nil then
        log.warn("No such space: '%s'", space_name)
    else
        space[fun](space)
    end

    local history_space_name = model_ddl.get_history_space_name(type_name, tenant_prefix)
    local history_space = box.space[history_space_name]
    if history_space == nil then
        log.warn("No such space: '%s'", history_space_name)
    else
        history_space[fun](history_space)
    end

    local expiration_space_name = model_ddl.get_expiration_space_name(type_name, tenant_prefix)
    local expiration_space = box.space[expiration_space_name]
    if expiration_space == nil then
        log.warn("No such space: '%s'", expiration_space_name)
    else
        expiration_space[fun](expiration_space)
    end

    if opts.apply_for_cold_storage == true then
        local vinyl_space_name = model_ddl.get_vinyl_space_name(type_name, tenant_prefix)
        local vinyl_space = box.space[vinyl_space_name]
        if vinyl_space == nil then
            log.warn("No such space: '%s'", vinyl_space_name)
        else
            vinyl_space[fun](vinyl_space)
        end
    end
end

--@tparam table object_names
local function delete_spaces(object_names)
    for type_name in pairs(object_names) do
        log.info("Drop spaces for %q", type_name)
        call_for_type_spaces(type_name, 'drop')
    end

    return true
end

--@tparam table object_names
local function truncate_spaces(object_names, opts)
    for type_name, _ in pairs(object_names) do
        log.info("Truncate spaces for %q", type_name)
        call_for_type_spaces(type_name, 'truncate', opts)
    end

    return true
end

local function clear_data()
    local ddl = tenant.get_cfg_deepcopy('ddl')
    return truncate_spaces(ddl, {apply_for_cold_storage = true})
end

local function validate_updater(type_name, updater)
    local valid_mutators = {'set', 'add', 'sub'}
    if not utils.has_value(valid_mutators, updater[1]) then
        return nil, accessor_error:new('Invalid setter: %s, allowed: %s',
            json.encode(updater), json.encode(valid_mutators))
    end
    local serializer = tenant.get_serializer()
    local res, err = model_updater.is_path_exists(serializer,
                                                  type_name,
                                                  updater[2])
    if not res then
        return nil, err
    end

    return true
end

--[[
    type_name - name of aggregate
    filter - AND-ed predicates
    setters - operations, which modifies records
      - operation
      - path
      - newval
]]
local function update(type_name, filter, updaters, options)
    options = options or {}
    if options.all_versions then
        return nil, accessor_error:new("Impossible to update all versions of object")
    end

    if not utils.is_array(updaters) then
        return nil, accessor_error:new("Incorrect updaters list, array expected")
    end

    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    for _, updater in ipairs(updaters) do
        local rc, err = validate_updater(type_name, updater)
        if rc == nil then
            return nil, err
        end
    end

    local version_field_id = type_spaces.spec.version_field_id
    local version
    if version_field_id ~= nil then
        version = extract_version(options)
    end

    local space = type_spaces.space

    local space_ddl = tenant.get_ddl(type_name)
    local document_opts = get_document_options(options)
    local plan = document_query_plan.new(space_ddl, filter, document_opts)

    local record_list = find_impl(type_name, plan, document_opts)
    local _, err = check_version_for_atomic_operation(type_name, record_list, options)
    if err ~= nil then
        return nil, err
    end

    local key_def = model_key_def.get_space_key_def(space.name, 0)
    local serializer = tenant.get_serializer()

    local update_lists, err = model_updater.format_update_lists(type_name, serializer, record_list, updaters)
    if err ~= nil then
        return nil, err
    end

    for i, tuple_update_list in ipairs(update_lists) do
        if version_field_id ~= nil then
            table.insert(tuple_update_list, {'=', version_field_id, version})
        end

        local pkey = key_def:extract_key(record_list[i])
        local res = space:update(pkey, tuple_update_list)
        if res == nil then
            res = last_tuple
        end
        record_list[i] = res
    end

    last_tuple = nil

    if version_field_id ~= nil then
        local _, err = run_keep_version_count_clean(type_name, record_list)
        if err ~= nil then
            return nil, err
        end
    end

    add_cursor(record_list, type_name, plan)
    return record_list
end

--@tparam string type_name
--@tparam table filter
--@tparam ?table options
--@tparam number options.version
--@tparam string options.first
--@tparam string options.after
--@tparam number options.only_if_version
local function update_atomic(type_name, filter, updaters, options)
    checks('string', 'table', 'table', '?table')
    local tuples, err = box_atomic(update, type_name, filter, updaters, options)
    if err ~= nil then
        return nil, err
    end
    return {tuples = tuples}
end

local function delete_impl(type_name, filter, options)
    local space_ddl = tenant.get_ddl(type_name)
    local document_opts = get_document_options(options)
    local plan = document_query_plan.new(space_ddl, filter, document_opts)
    local tuples = find_impl(type_name, plan, document_opts)

    local _, err = check_version_for_atomic_operation(type_name, tuples, options)
    if err ~= nil then
        return nil, err
    end

    local _, err = delete_records(type_name, tuples)
    if err ~= nil then
        return nil, err
    end

    if delete_mode() ~= 'permanent' then
        local _, err = run_keep_version_count_clean(type_name, tuples)
        if err ~= nil then
            return nil, err
        end
    end

    add_cursor(tuples, type_name, plan)
    return tuples
end

local function delete(type_name, filter, options)
    options = options ~= nil and options or {}

    set_delete_mode('permanent')
    local res, err = delete_impl(type_name, filter, options)
    set_delete_mode(nil)
    if err ~= nil then
        return nil, err
    end

    return res
end

--@tparam string type_name
--@tparam table filter
--@tparam ?table options
--@tparam number options.version
--@tparam string options.first
--@tparam string options.after
--@tparam number options.only_if_version
local function delete_atomic(type_name, filter, options)
    checks('string', 'table', '?table')

    local res, err = box_atomic(delete, type_name, filter, options)
    if err ~= nil then
        return nil, err
    end

    return {tuples = res}
end

local function protected_transaction_tail(fn_name, ok, ...)
    local is_in_txn = box.is_in_txn()
    box.rollback()

    if ok == true and is_in_txn == true then
        return nil, accessor_error:new('Function %q did not close transaction - rollback', fn_name)
    end

    if not ok then
        return nil, ...
    end

    return ...
end

-- To finalize "call_on_storage"/"map"/"combine" when transaction wasn't closed.
-- It should protect from cases when fiber was killed (e.g. by timeout),
-- raises on the first yield but transaction is still open.
-- TODO: enable MVCC - currently we don't protected from yields inside open transaction
local function protect_transaction(func_name, fn, ...)
    return protected_transaction_tail(func_name, pcall(fn, ...))
end

-- map-reduce hyper parameter
local DEFAULT_BATCH_COUNT = 500

local function process_batch(batch, map_fn_name, combine_fn_name, state, map_args, combine_args)
    local sandbox = vars.sandbox
    local map_fn, err = sandbox:dispatch_function(map_fn_name, {protected = true})
    if not map_fn then
        return nil, err
    end
    local combine_fn, err = sandbox:dispatch_function(combine_fn_name, {protected = true})
    if not combine_fn then
        return nil, err
    end
    map_args = map_args or {}
    local map_result, err = protect_transaction(map_fn_name,
        sandbox.batch_call, map_fn, batch, unpack(map_args, 1, table.maxn(map_args)))
    if err ~= nil then
        return nil, err
    end
    combine_args = combine_args or {}
    local state, err = protect_transaction(combine_fn_name,
        sandbox.batch_accumulate, combine_fn, state, map_result, unpack(combine_args, 1, table.maxn(combine_args)))
    return state, err
end

local function check_deadline_or_interrupt(deadline)
    if deadline == nil then
        return true
    end

    local now = fiber.clock()
    if now > deadline then
        return nil, accessor_error:new("Timeout exceeded")
    end

    -- Check session still exists
    if box.session.peer() == nil then
        return nil, accessor_error:new("Session interrupted")
    end

    return true
end

local function map_reduce_impl(type_spaces, filter, map_fn_name, combine_fn_name, opts)
    if opts == nil then
        opts = {}
    end
    local space_ddl = tenant.get_ddl(type_spaces.type_name)
    local plan = document_query_plan.new(space_ddl, filter, {version = opts.version})

    local state = opts.combine_initial_state
    local map_args = opts.map_args
    local combine_args = opts.combine_args
    local deadline
    if opts.timeout ~= nil then
        deadline = fiber.clock() + opts.timeout
    end

    local items = document.tuple_select(plan, type_spaces, {ignore_hard_limits = true})
    local batch = table.new(DEFAULT_BATCH_COUNT, 0)
    local count = 0
    local err
    for _, tuple in items do
        count = count + 1
        batch[count] = tuple
        if count % DEFAULT_BATCH_COUNT == 0 then
            state, err = process_batch(batch, map_fn_name, combine_fn_name, state, map_args, combine_args)
            if err ~= nil then
                return nil, err
            end

            local _, err = check_deadline_or_interrupt(deadline)
            if err ~= nil then
                return nil, err
            end

            count = 0
            table.clear(batch)
        end
    end
    state, err = process_batch(batch, map_fn_name, combine_fn_name, state, map_args, combine_args)
    if err ~= nil then
        return nil, err
    end

    return state
end

--[[
    type_name - name of aggregate
    filter - filter like in find
    version -
    map_fn - name of map function (aggregate -> value)
    combine_fn - name of reduce function (value * value -> state)
    opts - options
    opts.map_args -
    opts.combine_args -
    opts.combine_initial_state -
    opts.timeout -
    returns - result, error
]]

local option_checks = {
    map_args = '?',
    combine_args = '?',
    combine_initial_state = '?',
    version = '?number|cdata',
    timeout = '?number',
}
local function map_reduce(type_name, filter, map_fn_name, combine_fn_name, opts)
    checks('string', 'table', 'string', 'string', option_checks)

    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    -- Disable tracing in loop of sandbox.call
    tracing.disable()
    local data, err = map_reduce_impl(type_spaces, filter, map_fn_name, combine_fn_name, opts)
    tracing.enable()

    return data, err
end

local function remove_batch(space, pk_list)
    box.begin()
    for _, pk in ipairs(pk_list) do
        space:delete(pk)
    end
    box.commit()
end

local function dump_space(space, dump_fn)
    local batch_count = cartridge.config_get_readonly('force-yield-limit') or defaults.FORCE_YIELD_LIMIT
    local key_def = model_key_def.get_space_key_def(space.name, 0)

    local iteration = 0
    local batch = table.new(batch_count, 0)
    for _, tuple in space:pairs() do
        local _, err = dump_fn(tuple)
        if err ~= nil then
            return nil, err
        end

        table.insert(batch, key_def:extract_key(tuple))

        iteration = iteration + 1
        if iteration % batch_count == 0 then
            remove_batch(space, batch)
            table.clear(batch)
        end
    end
    remove_batch(space, batch)
end

--@tparam string type_name
--@tparam string strategy
--@tparam function dump_fn
--@tparam number timestamp
local function run_expiration_task(type_name, strategy, dump_fn)
    checks('string', 'string', '?function')

    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    local expiration_space = type_spaces.expiration_space
    local write_callback
    if strategy == 'file' then
        local serializer = tenant.get_serializer()
        write_callback = function(tuple)
            local object, err = model_flatten.unflatten_record(tuple, serializer, type_name)
            if err ~= nil then
                return nil, err
            end

            local _, err = accessor_error:pcall(dump_fn, object)
            if err ~= nil then
                return nil, err
            end
        end
    elseif strategy == 'cold_storage' then
        write_callback = function(tuple)
            local vinyl_space = type_spaces.vinyl_space
            local primary_space_name = type_spaces.space.name
            local key_def = model_key_def.get_space_key_def(primary_space_name, 0)
            local pkey = key_def:extract_key(tuple)
            local encoded_key = msgpack.encode(pkey)
            vinyl_space:replace({encoded_key, tuple.version, tuple.bucket_id, tuple})
        end
    else
        error('Unsupported expiration strategy')
    end

    set_delete_mode('default')
    local _, err = dump_space(expiration_space, write_callback)
    set_delete_mode(nil)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function delete_tuple_by_space_id(space_id, args, tuple)
    set_delete_mode(args.mode or 'default')
    local key_def = model_key_def.get_space_key_def(space_id, 0)
    box.space[space_id]:delete(key_def:extract_key(tuple))
    set_delete_mode(nil)
end

local function call_on_storage(func_name, func_args)
    local span = tracing.start_span('sandbox.call_by_name: %s', func_name)
    if func_args == nil then
        func_args = {}
    end
    local res = { protect_transaction(func_name,
        vars.sandbox.call_by_name, vars.sandbox, func_name, unpack(func_args, 1, table.maxn(func_args))) }
    span:finish({ error = res[2] })

    return {res = res[1], err = res[2]}
end

local function get_version_fieldno(type_name)
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    return type_spaces.spec.version_field_id
end

local function get_bucket_id_fieldno(type_name)
    local type_spaces, err = get_type_spaces(type_name)
    if err ~= nil then
        return nil, err
    end

    return type_spaces.space.index['bucket_id'].parts[1].fieldno
end

local function get_spaces_len()
    local ddl = cartridge.config_get_readonly('ddl')
    if ddl == nil then
        return {}
    end

    local res = {}
    local space_name_keys = {'space_name', 'history_space_name', 'expiration_space_name', 'vinyl_space_name'}
    for _, ddl_entry in pairs(ddl) do
        for _, key in ipairs(space_name_keys) do
            local space_name_value = ddl_entry[key]
            if space_name_value ~= nil and box.space[space_name_value] ~= nil then
                res[space_name_value] = box.space[space_name_value]:len()
            end
        end
    end
    return res
end

return {
    new = new_accessor,

    -- Public interface for repository
    find = find,
    find_pairs = find_pairs,
    count = count,
    put_atomic = put_atomic,
    update_atomic = update_atomic,
    delete_atomic = delete_atomic,

    get = get,
    put = put,
    update = update,
    delete = delete,

    get_tuple_by_pk = get_tuple_by_pk,
    delete_spaces = delete_spaces,
    truncate_spaces = truncate_spaces,
    clear_data = clear_data,
    get_spaces_len = get_spaces_len,
    remove_old_versions_by_type = remove_old_versions_by_type,
    run_expiration_task = run_expiration_task,
    delete_tuple_by_space_id = delete_tuple_by_space_id,
    call_on_storage = call_on_storage,
    get_replication_info = get_replication_info,
    get_version_fieldno = get_version_fieldno,
    get_bucket_id_fieldno = get_bucket_id_fieldno,

    apply_config = apply_config,
    validate_config = validate_config,
    -- map/reduce
    map_reduce = map_reduce,
}
