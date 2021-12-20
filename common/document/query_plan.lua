local json = require('json')
local clock = require('clock')
local errors = require('errors')
local model = require('common.model')
local defaults = require('common.defaults')
local multiindex = require('common.multiindex')
local comparators = require('common.document.comparators')
local document_utils = require('common.document.utils')
local tenant = require('common.tenant')
local model_expiration = require('storage.expiration.model')

local usage_error = errors.new_class('usage_error')

local op_to_tarantool = comparators.op_to_tarantool
local check_value_type = comparators.check_value_type
local invert_tarantool_op = comparators.invert_tarantool_op

local LIKE_OPS = {
    ['LIKE'] = 'LIKE',
    ['ILIKE'] = 'ILIKE',
}

local cache = setmetatable({}, {__mode = 'k'})
local function fields(ddl)
    local cached = cache[ddl]
    if cached ~= nil then
        return cached
    end

    local field_nos = {}
    for i, field in ipairs(ddl.format) do
        field_nos[field.name] = i
    end

    cache[ddl] = field_nos
    return field_nos
end

local function indexes(index_list)
    local cached = cache[index_list]
    if cached ~= nil then
        return cached
    end

    local result = {}
    for _, that in ipairs(index_list) do
        result[that.name] = that
    end

    cache[index_list] = result
    return result
end

local function index_keys(field_nos, indexes, index_name)
    local certain_index = indexes[index_name]
    assert(certain_index)

    local cached = cache[certain_index]
    if cached ~= nil then
        return cached
    end

    local result = {}

    for _, part in ipairs(certain_index.parts) do
        assert(field_nos[part.field])
        table.insert(result, field_nos[part.field])
    end

    cache[certain_index] = result
    return result
end

local function validate_select_condition(condition)
    usage_error:assert(type(condition) == "table", "Condition type mismatch. Expected table, got %s", type(condition))
    if #condition ~= 3 then
       error(usage_error:new("Malformed condition: %s", json.encode(condition)))
    end
    usage_error:assert(op_to_tarantool(condition[2]) ~= nil, "Operation not supported: %s", condition[2])
    usage_error:assert(check_value_type(condition[2], condition[3]) == nil, "Operation not supported: %s", condition[2])
    local condition_type = type(condition[1])
    usage_error:assert(condition_type == "string", "Expected index/field name is string, got %s", condition_type)
    usage_error:assert(
        not model.is_system_field(condition[1]),
        'Impossible to filter by system field: %s', condition[1]
    )
end

local function condition_get_index(index_list, condition)
    local field = condition[1]
    for _, idx in ipairs(index_list) do
        if not multiindex.is_multi_index(idx) then
            if idx.parts[1].field == field then
                return idx
            end
        end
    end

    for _, idx in ipairs(index_list) do
        if idx.name == field then
            return idx
        end
    end
end

local function is_select_by_full_primary_key_eq(plan, is_versioning_enabled)
    if plan.scan_value == nil then
        return false
    end
    if plan.scan_index.name == plan.pkey_index.name then
        if plan.op == 'EQ' or plan.op == 'REQ' then
            local pk_len = #plan.pkey_index.parts
            local scan_value_len = #plan.scan_value

            local offset = 0
            -- exclude `version` column
            if is_versioning_enabled then
                offset = 1
            end

            if (pk_len - offset) == scan_value_len then
                return true -- fully specified primary key
            end
        end
    end
    return false
end

--[[
    Function returns true if main query iteration can be stopped by fired opposite condition.

    For e.g.
    - iteration goes using `'id' > 10`
    - opposite condition `'id' < 100` becomes false
    - in such case we can exit from iteration
]]
local function is_early_exit_possible(scan_index, scan_op, check_name, check_op)
    if scan_index.name == check_name then
        local condition_op = op_to_tarantool(check_op)
        if scan_op == 'REQ' or scan_op == 'LT' or scan_op == 'LE' then
            if condition_op == 'GT' or condition_op == 'GE' then
                return true
            end
        elseif scan_op == 'EQ' or scan_op == 'GT' or scan_op == 'GE' then
            if condition_op == 'LT' or condition_op == 'LE' then
                return true
            end
        end
    end
    return false
end

local function like_cmp(_, r)
    return LIKE_OPS[r[2]] ~= nil
end

local function new(ddl, query, options)
    if query == nil then -- also cdata<NULL>
        query = {}
    end

    for _, condition in ipairs(query) do
        validate_select_condition(condition)
    end

    options = options or {}
    local first = options.first
    local after = options.after
    local scan_index_here = nil
    local scan_value = nil
    local scan_index = nil

    local version
    local all_versions
    local index_list

    local is_versioning_enabled = ddl.history_indexes ~= nil
    if is_versioning_enabled then
        index_list = ddl.history_indexes
        version = options.version
        all_versions = options.all_versions
        if version == nil then -- reset box.NULL to correctly pass to index:pairs
            version = nil
        end

        if version ~= nil then
            usage_error:assert(version >= 0, 'Version must be >= 0, got %s', version)
        end
    else
        index_list = ddl.indexes
    end
    local pkey_index = index_list[1]

    local ddl_indexes = indexes(index_list)
    local op = nil

    local field_nos = fields(ddl)

    --[[
        order query conditions so that LIKE checks will be applied
        after all other types of conditions
    ]]
    table.sort(query, like_cmp)
    --[[
        Searching for index for iteration over space.
        Smart query planning to use most appropriate index have to be here.
    ]]
    for i, condition in ipairs(query) do
        scan_index = condition_get_index(index_list, condition)
        -- skip index of LIKE condition as scan index
        if scan_index ~= nil then
            op = op_to_tarantool(condition[2])
            if not LIKE_OPS[condition[2]] then
                scan_value = condition[3]
                scan_index_here = i
                break
            end
        end
    end

    --[[
        Default iteration index is primary index
        Also this scan is used for like queries (without other conditions)
    ]]
    local scan_field_nos
    if scan_index == nil or (scan_index ~= nil and scan_index_here == nil) then
        op = 'GE' -- Default iteration is `next greater than previous`
        scan_index = pkey_index
        scan_field_nos = {}
        for _, part in ipairs(pkey_index.parts) do
            table.insert(scan_field_nos, field_nos[part.field])
        end
    else
        scan_field_nos = index_keys(field_nos, ddl_indexes, scan_index.name)
    end

    -- convert scan value to table if was not
    -- to use in custom filter_conditions (e.g. early exit)
    if type(scan_value) ~= 'table' then
        scan_value = {scan_value}
    end

    -- Backward iteration
    if first ~= nil and first < 0 then
        first = math.abs(first)
        op = invert_tarantool_op(op)
        scan_index_here = nil -- reset border condition
    end

    local filter_conditions = {}

    --[[
        Pagination:
        Optimization to use tarantool iteration
        to overcome `after` `scan value`.
    ]]
    if after ~= nil then
        if op == 'EQ' or op == 'REQ' then
            -- We have to exit iteration when key changed
            table.insert(filter_conditions, {
                name = scan_index.name,
                field_nos = scan_field_nos,
                comparator = '==', -- EQ
                values = scan_value,
                opts = {is_early_exit = true},
                index_parts = scan_index.parts
            })
        end

        scan_value = after.scan
        if op == 'LE' then
            op = 'LT'
        elseif op == 'GE' then
            op = 'GT'
        elseif op == 'EQ' then
            op = 'GT'
        elseif op == 'REQ' then
            op = 'LT'
        end
    end

    local has_multikey_filter_conditions = false
    local multikey_filter_conditions_count = 0
    -- Fill additional checks
    for i, condition in ipairs(query) do
        if i ~= scan_index_here then
            local field = condition[1]
            local is_like_op = LIKE_OPS[condition[2]]

            -- Index check (including one and multicolumn)
            local thatindex = ddl_indexes[field]
            if thatindex ~= nil and not is_like_op then
                local keys = index_keys(field_nos, ddl_indexes, field)
                local values = condition[3]
                local is_early_exit = is_early_exit_possible(scan_index,
                                                             op,
                                                             thatindex.name,
                                                             condition[2])
                if type(values) ~= 'table' then
                    values = {values}
                end

                if multiindex.is_multi_index(thatindex) then
                    has_multikey_filter_conditions = scan_index_here ~= nil
                    multikey_filter_conditions_count = multikey_filter_conditions_count + 1
                end

                table.insert(filter_conditions, {
                    name = thatindex.name,
                    field_nos = keys,
                    comparator = condition[2],
                    values = values,
                    opts = {is_early_exit = is_early_exit},
                    index_parts = thatindex.parts,
                })
                -- One column check
            elseif field_nos[field] ~= nil then
                local is_early_exit = is_early_exit_possible(scan_index,
                                                             op,
                                                             field,
                                                             condition[2])
                local values = condition[3]
                local fieldno = field_nos[field]
                local field_def = ddl.format[fieldno]
                if type(values) ~= 'table' then
                    values = {values}
                end

                if is_like_op and field_def.type ~= 'string' then
                    usage_error:assert(false,
                                  'Using like queries with indexes and non string fields is prohibited, condition %s',
                                  json.encode(condition))
                end

                table.insert(filter_conditions, {
                    name = field_def.name,
                    field_nos = {fieldno},
                    comparator = condition[2],
                    values = values,
                    opts = {is_early_exit = is_early_exit},
                    index_parts = {
                        {field = fieldno, type = field_def.type, is_nullable = field_def.is_nullable},
                    },
                })
            elseif thatindex ~= nil and is_like_op then
                usage_error:assert(false,
                                  'Using like queries with indexes is prohibited, condition %s',
                                  json.encode(condition))
            else
                usage_error:assert(false,
                                   'No field or index is found for condition %s',
                                   json.encode(condition))
            end
        end
    end

    if multikey_filter_conditions_count > 1 then
        usage_error:assert(false,
            'Multikey search conditions count cannot exceed 1. Current number is %s',
            multikey_filter_conditions_count)
    end

    local scan_multi_index_field_no = nil

    local part_no = multiindex.multi_index_part(scan_index)
    if part_no ~= nil then
        scan_multi_index_field_no = scan_field_nos[part_no]
    end

    -- Version as additional check
    if version ~= nil then
        table.insert(filter_conditions, {
            name = 'version',
            field_nos = {field_nos['version']},
            comparator = '<=',
            values = {version},
            opts = {is_early_exit = false},
            index_parts = {
                {field = field_nos['version'], type = 'unsigned', is_nullable = false},
            },
        })
    end

    if is_versioning_enabled then
        local lifetime_nsec = model_expiration.get_lifetime_nsec(ddl.type_name)
        if lifetime_nsec ~= nil then
            local lifetime_threshold = clock.time64() - lifetime_nsec
            table.insert(filter_conditions, {
                name = 'version',
                field_nos = {field_nos['version']},
                comparator = '>',
                values = {lifetime_threshold},
                opts = {is_early_exit = false},
                index_parts = {
                    {field = field_nos['version'], type = 'unsigned', is_nullable = false},
                },
            })
        end
    end

    -- Query plan structure
    local plan = {
        -- Main iteration fields
        op = op, -- Iteration comparator
        scan_index = scan_index,
        scan_field_nos = scan_field_nos,
        scan_value = scan_value,

        -- Where `clause`
        -- one- or multi-column indexes
        filter_conditions = filter_conditions,
        has_multikey_filter_conditions = has_multikey_filter_conditions,

        -- Pagination
        first = first, -- always positive

        -- Row version check
        pkey_index = pkey_index,
        version = version,
        all_versions = all_versions,

        scan_multi_index_field_no = scan_multi_index_field_no,
    }

    -- Suppress some checks for full primary key scan
    local only_one_tuple_needed = not all_versions and is_select_by_full_primary_key_eq(plan, is_versioning_enabled)
    if only_one_tuple_needed then
        plan.op = 'REQ'
        plan.first = 1
    end

    return plan
end

local scan_info_template = '%s SCAN USING %q %s%s INDEX %q'
local range_scan_template = 'IN RANGE FROM %s TO %s'
local storage_stats_versioned_str = '\t\tPRIMARY: SCANNED %s, SELECTED %s;\n' ..
    '\t\tHISTORY: SCANNED %s, SELECTED %s;\n' ..
    '\t\tRETURNED: %d;'
local storage_stats_non_versioned_str = '\t\tPRIMARY: SCANNED %s, SELECTED %s;'

local function sum_returned(rs)
    return rs.primary_space_returned + rs.history_space_returned
end

local function serialize_explain(plan, summary)
    -- QUERY
    local text = {
        'QUERY',
    }

    -- EXECUTED ON REPLICASET 06170fc7-e61c-4ddf-bd4d-0590d94a5f3a
    -- or
    -- IS MAP-REDUCE ON REPLICASETS 06170fc7-e61c-4ddf-bd4d-0590d94a5f3a, 06170fc7-e61c-4ddf-bd4d-0590d94a5f32...
    if #summary.replicasets == 1 then
        table.insert(text, ('EXECUTED ON REPLICASET %s'):format(summary.replicasets[1].uuid))
    else
        table.insert(text, 'IS MAP-REDUCE')
    end

    local returned_all = 0
    for _, rs in ipairs(summary.replicasets) do
        local rs_returned = sum_returned(rs)
        returned_all = returned_all + rs_returned
    end

    -- NON-VERSIONED SCAN USING "id" PRIMARY INDEX
    local versioned_str
    if summary.versioned == true then
        versioned_str = 'VERSIONED'
    else
        versioned_str = 'NON-VERSIONED'
    end

    local index_str
    if summary.scan_index == plan.pkey_index.name then
        index_str = 'PRIMARY'
    else
        index_str = 'SECONDARY'
    end

    local multikey_str = ''
    if summary.multikey_index_scan == true then
        multikey_str = 'MULTIKEY '
    end

    local iterator = summary.iterator
    local scan_info = scan_info_template:format(
        versioned_str, summary.scan_index, multikey_str, index_str, iterator)
    table.insert(text, scan_info)

    if summary.range_scan == true then
        local border_key
        for _, filter_cond in ipairs(summary.filters) do
            if filter_cond.opts ~= nil and filter_cond.opts.is_early_exit == true then
                border_key = filter_cond.values
                break
            end
        end

        local lower_bound = json.encode(summary.scan_key)
        local upper_bound = json.encode(border_key)
        if iterator == 'REQ' or iterator == 'LE' or iterator == 'LT' then
            lower_bound, upper_bound = upper_bound, lower_bound
        end
        table.insert(text, range_scan_template:format(lower_bound, upper_bound))
    end

    if #summary.filters > 0 then
        local with_checks_str = 'WITH ADDITIONAL CHECK'
        if #summary.filters > 1 then
            with_checks_str = with_checks_str .. 'S'
        end
        table.insert(text, with_checks_str .. ':')

        for _, filter in ipairs(summary.filters) do
            table.insert(text, ('\t%q %s %s'):format(filter.name, filter.comparator, json.encode(filter.values)))
        end
    end

    if #summary.replicasets > 1 then
        table.insert(text, 'ON REPLICASETS:')
    else
        table.insert(text, 'ON REPLICASET:')
    end

    for _, rs in ipairs(summary.replicasets) do
        table.insert(text, ('\t%q SCANNED SPACES:'):format(rs.uuid))
        if summary.versioned == true then
            table.insert(text, storage_stats_versioned_str:format(
                    rs.primary_space_scanned,
                    rs.primary_space_returned,
                    rs.history_space_scanned,
                    rs.history_space_returned,
                    sum_returned(rs)))
        else
            table.insert(text, storage_stats_non_versioned_str:format(
                rs.primary_space_scanned,
                rs.primary_space_returned))
        end
    end

    if #summary.replicasets > 1 then
        table.insert(text, 'WITH SORT ON ROUTER')
    end

    if returned_all > summary.first then
        local first_str = 'TRUNCATE RESULTS TO %d ROW'
        if summary.first > 1 then
            first_str = first_str .. 'S'
        end
        table.insert(text, first_str:format(summary.first))
    end

    return table.concat(text, '\n')
end

local function explain(type_name, query, options, storage_stats)
    local ddl = tenant.get_ddl(type_name)

    options = options or {}
    if options.after ~= nil then
        options.after = document_utils.decode_cursor(options.after)
    end

    local plan = new(ddl, query, options)

    local repository = tenant.get_repository()
    local replicasets
    local bucket_id = repository.get_bucket_id_for_query(plan, repository.serializer[type_name])
    if bucket_id == nil then
        replicasets = vshard.router.routeall()
    else
        local replicaset, err = vshard.router.route(bucket_id)
        if err ~= nil then
            return nil, err
        end
        replicasets = {replicaset}
    end

    local replicasets_list = {}

    for _, replicaset in pairs(replicasets) do
        table.insert(replicasets_list, {
            uuid = replicaset.uuid,
            primary_space_scanned = storage_stats[replicaset.uuid].primary_space_scanned,
            history_space_scanned = storage_stats[replicaset.uuid].history_space_scanned,
            primary_space_returned = storage_stats[replicaset.uuid].primary_space_returned,
            history_space_returned = storage_stats[replicaset.uuid].history_space_returned,
        })
    end

    local range_scan = false
    for _, filter_cond in ipairs(plan.filter_conditions) do
        if filter_cond.opts ~= nil and filter_cond.opts.is_early_exit == true then
            range_scan = true
            break
        end
    end

    local versioned = false
    if plan.all_versions == true or plan.version ~= nil then
        versioned = true
    end

    local multikey_index_scan = plan.scan_multi_index_field_no ~= nil
    local first = plan.first or defaults.FIND_LIMIT
    local summary = {
        -- Replicaset(s) where query will be executed
        replicasets = replicasets_list,
        scan_index = plan.scan_index.name,
        scan_key = plan.scan_value,
        filters = plan.filter_conditions,
        range_scan = range_scan,
        version = plan.version,
        multikey_index_scan = multikey_index_scan,
        first = first,
        iterator = plan.op,
        versioned = versioned,
    }

    summary.text = serialize_explain(plan, summary)
    return summary
end

return {
    new = new,
    explain = explain,

    -- for tests
    serialize_explain = serialize_explain,
}
