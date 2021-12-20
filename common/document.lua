local model_key_def = require('storage.model_key_def')
local errors = require('errors')
local document_key_def = require('common.document.key_def')
local version_lib = require('common.version')
local tuple_merger = require('common.document.tuple_merger')
local query_metrics = require('common.document.metrics')

local affinity_error = errors.new_class("affinity_error")

local utils = require('common.document.utils')
local comparators = require('common.document.comparators')
local filters_generator = require('common.document.filters_generator')

local tarantool_to_sort_op = comparators.tarantool_to_sort_op
local op_to_array_fun_def = comparators.op_to_array_fun_def

local SKIP_VERSION_LIMIT = 10

--[[
    Determine whether certain row is the latest before `version`
    or equal `version`.

    If `version` is `nil` determine whether certain row is the latest.
]]
local function tuple_fits_version(pk_indexes, tuple, plan, key_def, nexttuple)
    -- Apply special checks for primary key
    -- We exactly know about that tuples are sorted by versions
    if plan.scan_index.name == plan.pkey_index.name then
        local cmp  = nexttuple ~= nil and key_def:compare(tuple, nexttuple) or nil
        -- if there is not any tuple and current tuple fits
        if cmp ~= nil and cmp < 0 then
            return true
        end

        -- nexttuple is greater version of tuple
        if cmp ~= nil and cmp == 0 then
            local nexttuple_version = nexttuple.version
            -- nexttuple.version is greater then tuple.version
            -- and satisfies plan then skip current tuple
            if nexttuple_version < plan.version then
                if nexttuple_version > tuple.version then
                    return false
                end
            -- if nexttuple does not satisfy plan.version then
            -- current tuple has max version
            -- additional checks are not needed, because
            -- early_tuple_fits_plan_version has been already checked
            elseif nexttuple_version > plan.version then
                return true
            end

            if plan.op == 'GE' or plan.op == 'GT' then
                -- Next tuple version is greater and satisfies conditions
                if nexttuple_version <= plan.version then
                    return false
                else -- nexttuple.version > plan.version
                    return true
                end
            end
        end
    end

    -- get the latest tuple before or equal version (or maximum version)
    local pkey = key_def:extract_key(tuple)
    local candidate = utils.get_last_tuple_for_version(
        pk_indexes, key_def, pkey, plan.version)

    -- there is no tuple for version
    if candidate == nil then
        return false
    end

    return tuple.version == candidate.version
end

local function tuple_merger_comparator_factory(plan, scan_index)
    local key_def = document_key_def.get_key_def(scan_index)
    local op = plan.op

    local comparator
    if scan_index.hint == true or plan.scan_multi_index_field_no ~= nil then
        comparator = document_key_def.compare_hinted
    else
        comparator = document_key_def.compare
    end

    if op == 'REQ' or op == 'LT' or op == 'LE' then
        return function(t1, t2, state_left, state_right)
            return comparator(key_def, t1, t2, state_left, state_right) >= 0
        end
    else
        return function(t1, t2, state_left, state_right)
            return comparator(key_def, t1, t2, state_left, state_right) < 0
        end
    end
end

local function get_iterator_for_key(plan, scan_indexes, cursor, opts)
    local iterator = {
        fun = nil,
        param = nil,
        state = nil,
    }

    local cursor_without_version = cursor

    local primary = scan_indexes.primary
    local len = #cursor_without_version
    if len > #primary.parts then
        if box.tuple.is(cursor_without_version) then
            cursor_without_version = cursor_without_version:transform(len, len)
        else
            cursor_without_version = table.copy(cursor_without_version)
            cursor_without_version[#cursor_without_version] = nil
        end
    end

    local state = primary:pairs(cursor_without_version, opts)
    state = query_metrics.wrap_iterator(state, 'primary')
    if plan.all_versions == true or plan.version ~= nil then
        -- Use comparator based on history scan index as it considers version field
        local history = scan_indexes.history
        local lessthan = tuple_merger_comparator_factory(plan, history)
        local history_state = history:pairs(cursor, opts)
        history_state = query_metrics.wrap_iterator(history_state, 'history')
        state = tuple_merger.merge2(state, history_state, lessthan)
    end

    iterator.fun = state.gen
    iterator.param = state.param
    iterator.state = state.state

    return iterator
end

local function single_space_iterator(plan, spaces)
    local cursor = plan.scan_value
    local opts = {iterator = plan.op}

    local primary = spaces.space.index[plan.scan_index.name]
    return function(iterator)
        if iterator == nil then
            local state = primary:pairs(cursor, opts)
            state = query_metrics.wrap_iterator(state, 'primary')
            iterator = {
                fun = state.gen,
                param = state.param,
                state = state.state,
            }
        end

        local tuple
        iterator.state, tuple = iterator.fun(iterator.param, iterator.state)
        return iterator, tuple
    end
end

local function versioned_space_iterator(plan, spaces)
    local scan_indexes = {
        primary = spaces.space.index[plan.scan_index.name],
        history = spaces.history_space.index[plan.scan_index.name],
    }

    local cursor = plan.scan_value
    local opts = {iterator = plan.op}

    return function(iterator)
        if iterator == nil then
            iterator = get_iterator_for_key(plan, scan_indexes, cursor, opts)
        end

        local tuple
        iterator.state, tuple = iterator.fun(iterator.param, iterator.state)
        return iterator, tuple
    end
end

local function iterate_over_multiindex(base_iterator, plan, space)
    local format = space:format()
    local format_len = #format
    return function(iterator)
        local tuple
        iterator, tuple = base_iterator(iterator)
        if iterator.state ~= nil and plan.scan_multi_index_field_no ~= nil then
            local multi_position = document_key_def.get_tree_comparison_hint(iterator.state)
            multi_position = multi_position + 1 -- convert to lua array indexing from 1
            -- append meta info to end of tuple
            tuple = tuple:update({{'=', format_len + 1, {multi_position = multi_position}}})
        end
        return iterator, tuple
    end
end

-- skip iteration if we definitely know that version does not fit
local function early_version_skip(plan, tuple, next_tuple, state, scan_key_def)
    -- `version` is specified but tuple version is greater
    if tuple.version > plan.version then
        return true
    end

    -- `version` is specified and tuple version exactly fits
    if tuple.version == plan.version then
        return false
    end

    -- there is no next tuple
    if next_tuple == nil then
        return false
    end

    if state.turned_scan_tuple ~= nil and scan_key_def:compare(tuple, state.turned_scan_tuple) == 0 then
        return true
    end

    -- tuple scan key is equal (excluding `version`)
    if scan_key_def:compare(tuple, next_tuple) == 0 then
        local next_version = next_tuple.version

        -- `version` is specified and next tuple newer and fits
        if next_version > tuple.version and next_version <= plan.version then
            return true
        end

        -- `version` is specified and next tuple still not fit
        if next_version < tuple.version and next_version > plan.version then
            return true
        end
    end

    return false
end

--[[
    Function iterates over tuples and try to optimize version scan

    It wait SKIP_VERSION_LIMIT and after it fired skip rest versions
    using new box.index iterator

    If certain version returned before SKIP_VERSION_LIMIT it's do nothing.
    If certain version did not return before SKIP_VERSION_LIMIT it tries
    to found it by box.index LE iteration over primary key with plan.version
]]
local function iterate_over_versions(base_iterator, plan, spaces)
    local space = spaces.space
    local history_space = spaces.history_space

    local scan_index_name = plan.scan_index.name
    local scan_indexes = {
        primary = space.index[scan_index_name],
        history = history_space.index[scan_index_name],
    }

    local primary_indexes = {
        primary = space.index[0],
        history = history_space.index[0],
    }

    local scan_key_def = model_key_def.get_space_key_def(space.name, scan_index_name)
    local key_def = model_key_def.get_space_key_def(space.name, 0)

    return function(iterator)
        -- it is first run, so make iterators using plan
        if iterator ~= nil and iterator.nexttuple == nil then
            return iterator
        end
        -- TODO think of op for LIKE conditions (f.e. GE works, but like range search)

        local tuple, nexttuple
        if iterator == nil or iterator.nexttuple == nil then
            iterator, tuple = base_iterator(iterator)
            if tuple == nil then
                return iterator
            end
        else
            tuple = iterator.nexttuple
        end
        iterator, nexttuple = base_iterator(iterator)
        iterator.nexttuple = nexttuple

        -- check if tuple can be skipped
        local count = 0
        while tuple ~= nil and early_version_skip(plan, tuple, iterator.nexttuple, iterator, scan_key_def) do
            -- if skip too much, try to goto certain tuple by index
            -- only when iterator not EQ and not REQ
            count = count + 1
            if count == SKIP_VERSION_LIMIT and plan.op ~= 'EQ' and plan.op ~= 'REQ' then
                -- step over whole tuple set
                local tuple_already_found = false
                local scan_key = scan_key_def:extract_key(tuple)
                if iterator.turned_scan_tuple ~= nil and
                    key_def:compare(iterator.turned_scan_tuple, tuple) == 0 then
                    tuple_already_found = true
                end

                local op
                -- Here we jump through {scan key + primary key}
                -- For GT/GE iterator jump is just moving to {full scan key, max possible version}
                local iterator_scan_key = scan_key
                if plan.op == 'LE' or plan.op == 'LT' then
                    op = 'LT'
                elseif plan.op == 'GE' or plan.op == 'GT' then
                    iterator_scan_key = scan_key:update({{'!', -1, version_lib.MAX_POSSIBLE_VERSION}})
                    op = 'GT'
                end

                iterator = get_iterator_for_key(plan, scan_indexes, iterator_scan_key, {iterator = op})
                iterator.state, iterator.nexttuple = iterator.fun(iterator.param, iterator.state)
                count = 0

                -- if version not found before skipping
                -- try to found it by LE primary key
                if not tuple_already_found then
                    local pkey = key_def:extract_key(tuple)
                    local last_tuple = utils.get_last_tuple_for_version(primary_indexes, key_def, pkey, plan.version)
                    -- Make sure a tuple wasn't scanned yet
                    -- E.g. for non-unique secondary indexes
                    -- we can meet that has been already returned
                    if last_tuple ~= nil and scan_key_def:compare_with_key(last_tuple, scan_key) == 0 then
                        -- there is certain tuple for versioning
                        tuple = last_tuple
                        break
                    end
                end
            end

            tuple = iterator.nexttuple
            if tuple ~= nil then
                iterator, nexttuple = base_iterator(iterator)
                iterator.nexttuple = nexttuple
            end
        end

        iterator.turned_scan_tuple = tuple
        return iterator, tuple
    end
end

local function match_conditions(plan)
    local filter = filters_generator.gen_index_filter(plan.filter_conditions)
    if filter == nil then
        return nil
    end

    local filter_func = filters_generator.compile(filter)

    -- Another conditions also can be multikey indexes
    -- So, we can't use hint because it could point to wrong position in array
    local is_multi_index = plan.scan_multi_index_field_no ~= nil
    local need_multiposition = is_multi_index and not plan.has_multikey_filter_conditions
    return function(tuple)
        return utils.is_condition_match(tuple, filter_func, need_multiposition)
    end
end

local function fits_version(plan, spaces, pk_key_def_without_version)
    local primary_indexes = {
        primary = spaces.space.index[0],
        history = spaces.history_space.index[0],
    }

    return function(tuple, iterator)
        local matches = tuple_fits_version(primary_indexes, tuple, plan,
            pk_key_def_without_version, iterator.nexttuple)
        return matches, false
    end
end

--[[
    Query executor:

    - get query plan
    - choose appropriate iterator according the plan
    - move to first scan value by tarantool index iterator
    - main loop
    - check `where` clauses
    - check row version is appropriate (last before requested or maximum)
    - exit when
      - early exit condition fired
      - accumulator reaches limit
      - tarantool index iterator reaches `end of space`

    Warning! Custom iterator inside: http://lua-users.org/wiki/IteratorsTutorial
]]

local function tuple_select(plan, spaces, options)
    options = options or {}

    local space = spaces.space

    local pk_key_def_wo_version = model_key_def.get_space_key_def(space.name, 0)

    query_metrics.init({
        scanned_limit = options.hard_limits ~= nil and options.hard_limits.scanned,
        returned_limit = options.hard_limits ~= nil and options.hard_limits.returned,
        ignore_hard_limits = options.ignore_hard_limits,
        space_name = space.name,
        plan = plan,
        pk_key_def = pk_key_def_wo_version
    })

    -- Customize an iterator for scan
    -- Following iterators are available:
    --   * raw_iterator - simple box iterator, it's a base for other iterators
    --   * iterate_over_multiindex - is used for iteration that considers multi_positions
    --   * iterate_over_versions - scan that respects versioning

    local ignore_versioning = plan.version == nil or plan.version == version_lib.MAX_POSSIBLE_VERSION

    local iterate_over
    if spaces.history_space == nil then
        iterate_over = single_space_iterator(plan, spaces)
    else
        iterate_over = versioned_space_iterator(plan, spaces)
    end

    if plan.scan_multi_index_field_no ~= nil then
        iterate_over = iterate_over_multiindex(iterate_over, plan, space)
    elseif plan.all_versions ~= true and ignore_versioning ~= true then
        iterate_over = iterate_over_versions(iterate_over, plan, spaces)
    end

    -- Additional scan checks:
    --   * match_conditions - check user defined filter
    --   * fits_version - check that current tuple version is the last suitable
    local match_conditions_check = match_conditions(plan)

    -- return not only last version if all_versions.
    -- early_tuple_fits_plan_version has been already
    -- checked that version right but not necessary last
    local fits_version_check
    if plan.all_versions ~= true and ignore_versioning ~= true then
        fits_version_check = fits_version(plan, spaces, pk_key_def_wo_version)
    end

    -- Main query scan loop
    local iterator, tuple
    local count = 0
    local function gen()
        -- rowset is already returned, so exit
        if plan.first ~= nil and count >= plan.first then
            return nil
        end

        iterator, tuple = iterate_over(iterator)
        -- main loop to find next matched row
        while tuple ~= nil do
            query_metrics.inc_scanned(tuple)

            local matches = true
            local early_exit

            if match_conditions_check ~= nil then
                matches, early_exit = match_conditions_check(tuple)
                if early_exit == true then
                    return nil
                end
            end

            if matches == true and fits_version_check ~= nil then
                matches, early_exit = fits_version_check(tuple, iterator)
                if early_exit == true then
                    return nil
                end
            end

            if matches then
                -- Certain row is found
                count = count + 1
                query_metrics.inc_returned(iterator, tuple)
                return iterator, tuple
            end

            iterator, tuple = iterate_over(iterator)
        end
        return nil
    end

    return gen
end

local function create_row_lessthan(op, index)
    local func = op_to_array_fun_def(op, index.parts)

    return function(left, right)
        local lhs = left[#left]
        local rhs = right[#right]

        return func(rhs, lhs)
    end
end

local function sort_tuples(plan, tuples)
    local op = tarantool_to_sort_op(plan.op)
    local lessthan = plan.scan_index.cmp[op]
    table.sort(tuples, lessthan)
end

--[[
    Function returns `true` if

    - query plan uses equal (or reverse equal) comparator for iteration over space
    - query value for iteration is fully specified

    and `false` in other cases

    Use case:
    map/reduce process checks if sharding value is fully specified in query, so
    query can be sent to only one shard.

    For e.g.
    - sharding key is `City`
    - query contains scan condition `{'City', '==', 'Moscow'}`
    - this way query can be sent to only shard with sharding key `Moscow`
]]
local function is_select_specified_by_fields_eq(plan, field_ids)
    if plan.scan_value == nil then
        return false
    end

    if plan.op ~= 'EQ' and plan.op ~= 'REQ' then
        return false
    end

    -- check that whole field_ids set is included in scan_field set
    -- and that scan_value contains all of field_ids
    for _, field_no in ipairs(field_ids) do
        local found = false
        for pos, scan_no in ipairs(plan.scan_field_nos) do
            if field_no == scan_no and plan.scan_value[pos] ~= nil then
                found = true
                break
            end
        end
        if found == false then
            return false
        end
    end

    return true
end

local function select_affinity_from_scan_value(plan, field_ids)
    local result = {}
    for _, field_no in ipairs(field_ids) do
        local found = false
        for pos, scan_no in ipairs(plan.scan_field_nos) do
            if field_no == scan_no then
                if plan.scan_value[pos] ~= nil then
                    table.insert(result, plan.scan_value[pos])
                    found = true
                    break
                else
                    affinity_error:assert(false, "Could not get affinity value from scan value")
                end
            end
        end
        affinity_error:assert(found, "Could not find affinity value in scan plan")
    end
    return result
end

return {
    create_row_lessthan = create_row_lessthan,
    tuple_select = tuple_select,
    sort_tuples = sort_tuples,
    select_affinity_from_scan_value = select_affinity_from_scan_value,
    is_select_specified_by_fields_eq = is_select_specified_by_fields_eq,
}
