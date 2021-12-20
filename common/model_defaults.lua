local module_name = 'common.model_defaults'

local errors = require('errors')
local cartridge = require('cartridge')
local model = require('common.model')
local vars = require('common.vars').new(module_name)
local executor = require('common.sandbox.executor')
local sandbox_registry = require('common.sandbox.registry')
local lock_with_timeout = require('common.lock_with_timeout')

local type_validation_error = errors.new_class('type_validation_error')

vars:new_global('sequence_ranges', {})
vars:new_global('sequence_locks', {})

local SEQUENCE_LOCK_TIMEOUT = 0.1

local function get_next_sequence_value(sequence_name)
    local range = vars.sequence_ranges[sequence_name]
    while range == nil or range.next_id > range.last_id do
        local lock = vars.sequence_locks[sequence_name]
        if lock == nil or lock:released() then
            lock = lock_with_timeout.new(SEQUENCE_LOCK_TIMEOUT)
            vars.sequence_locks[sequence_name] = lock
            local new_range, err = cartridge.rpc_call(
                'core',
                'get_range',
                { sequence_name },
                { leader_only = true, timeout = SEQUENCE_LOCK_TIMEOUT }
            )
            lock:broadcast_and_release()

            if err ~= nil then
                return nil, err
            end

            vars.sequence_ranges[sequence_name] = {
                next_id = new_range[1],
                last_id = new_range[2],
            }
        else
            lock:wait()
        end
        range = vars.sequence_ranges[sequence_name]
    end

    local value = range.next_id
    range.next_id = range.next_id + 1
    return value
end

local function is_array_type(field)
    if type(field) ~= 'table' then
        return false
    end

    if field.type ~= 'array' then
        return false
    end

    return true
end

local function is_enum(field)
    if type(field) ~= 'table' then
        return false
    end

    if field.type ~= 'enum' then
        return false
    end

    return true
end

local PRIMITIVE_VALUE_DEFAULT = 1
local PRIMITIVE_VALUE_LAMBDA = 2
local PRIMITIVE_VALUE_AUTO_INCREMENT = 3
local ARRAY = 4
local DICTIONARY = 5
local UNION = 6

--
-- We need root_type and parent type to avoid sharing sequences
-- for different nested field.
-- Also here we use the fact that records with the same names is prohibited.
--
-- Sequence name: <root_record_name>_<parent_field_name>_<field_name>
--
local function get_sequence_name(root_type_name, parent_name, field_name)
    return table.concat({root_type_name, parent_name, field_name}, '_')
end

local function fill_field(root_type_name, parent_name, field, field_resolver)
    local field_type = field.type
    if model.is_primitive_or_derived_type(field_type) then
        local type = field_type.type ~= nil and field_type.type or field_type
        if field.default ~= nil then
            return {PRIMITIVE_VALUE_DEFAULT, {default=field.default, type=type}}
        elseif field.default_function ~= nil then
            local lambda, err = field_resolver(field)
            if err ~= nil then
                return nil, err
            end
            return {PRIMITIVE_VALUE_LAMBDA, {default_lambda=lambda, type=type}}
        elseif field.auto_increment == true then
            local sequence_name = get_sequence_name(root_type_name, parent_name, field.name)
            return {PRIMITIVE_VALUE_AUTO_INCREMENT, {sequence_name=sequence_name, type=type}}
        end
    elseif is_array_type(field_type) then
        local field_items = field_type.items
        if model.is_union_type(field_items) then
            local rc, err = fill_field(root_type_name, parent_name, {type = field_items}, field_resolver)
            if err ~= nil then
                return nil, err
            end
            if next(rc) then
                return {ARRAY, rc}
            end
        elseif not model.is_primitive_type(field_items) then
            local rc, err = fill_field(root_type_name, parent_name, field_items, field_resolver)
            if err ~= nil then
                return nil, err
            end
            if next(rc) then
                return {ARRAY, rc}
            end
        end
    elseif model.is_union_type(field_type) then
        local children = {}

        for _, subfield in ipairs(field_type) do
            if type(subfield) == 'string' then
                if subfield ~= 'null' then
                    return nil, type_validation_error:error('Unsupported type in union: %s', subfield)
                end
            else
                local rc, err = fill_field(root_type_name, field_type.name, subfield, field_resolver)
                if err ~= nil then
                    return nil, err
                end
                if next(rc) then
                    children[subfield.name] = rc
                end
            end
        end

        if next(children) then
            return { UNION, children }
        end
    elseif model.is_record(field_type) then
        local children = {}

        for _, subfield in ipairs(field_type.fields) do
            local rc, err = fill_field(root_type_name, field_type.name, subfield, field_resolver)
            if err ~= nil then
                return nil, err
            end
            if next(rc) then
                children[subfield.name] = rc
            end
        end
        if next(children) then
            return { DICTIONARY, children }
        end
    elseif is_enum(field_type) then
        if field.default ~= nil then
            return {PRIMITIVE_VALUE_DEFAULT, {default=field.default}}
        end
    else
        local children = {}
        for _, subfield in ipairs(field.fields or {}) do
            local rc, err = fill_field(root_type_name, parent_name, subfield, field_resolver)
            if err ~= nil then
                return nil, err
            end
            if next(rc) then
                children[subfield.name] = rc
            end
        end
        return children
    end

    return {}
end

local function field_resolver(field)
    local sandbox = sandbox_registry.get('active')
    local fn, err = sandbox:dispatch_function(field.default_function, {protected = true})
    if not fn then
        return nil, err
    end
    local lambda = function(...) return executor.call(fn, ...) end
    return lambda
end

local function fill_mdl_defaults(mdl)
    local res = {}
    for _, type_entry in ipairs(mdl) do
        if type_entry.indexes ~= nil then
            local type_res = {}
            local type_name = type_entry.name
            for _, subfield in ipairs(type_entry.fields) do
                local rc, err = fill_field(type_name, type_name, subfield, field_resolver)
                if err ~= nil then
                    return nil, err
                end
                if next(rc) then
                    type_res[subfield.name] = rc
                end
            end
            if next(type_res) then
                res[type_entry.name] = type_res
            end
        end
    end
    return res
end

local mock_field_resolver = function() end

local function validate_mdl_defaults(mdl)
    for _, type_entry in ipairs(mdl) do
        local type_name = type_entry.name
        for _, subfield in ipairs(type_entry.fields or {}) do
            local _, err = fill_field(type_name, type_name, subfield, mock_field_resolver)
            if err ~= nil then
                return nil, err
            end
        end
    end
    return true
end

-- avro schema doesn't support complex tarantool types
-- such as decimal and uuid.
-- So we should explicitly cast them to primitive types such as string
-- https://github.com/tarantool/avro-schema/issues/131
local function cast_to_primitive(value, type)
    if type == 'string' then
        return tostring(value)
    end
    return value
end

local function fill_default_entity(defaults, input, root_object, current_path)
    for field_name, opts in pairs(defaults) do
        table.insert(current_path, field_name)
        if opts[1] == PRIMITIVE_VALUE_DEFAULT then
            if input[field_name] == nil then
                input[field_name] = opts[2].default
            end
        elseif opts[1] == PRIMITIVE_VALUE_LAMBDA then
            if input[field_name] == nil then
                local rc, err = opts[2].default_lambda(root_object, current_path)
                if err ~= nil then
                    return nil, err
                end
                input[field_name] = cast_to_primitive(rc, opts[2].type)
            end
        elseif opts[1] == PRIMITIVE_VALUE_AUTO_INCREMENT then
            if input[field_name] == nil then
                local err
                input[field_name], err = get_next_sequence_value(opts[2].sequence_name)
                if err ~= nil then
                    return nil, err
                end
            end
        elseif opts[1] == ARRAY then
            if input[field_name] ~= nil then
                for _, item in ipairs(input[field_name]) do
                    local defaults = opts[2]
                    if defaults[1] == UNION then
                        local type_name
                        type_name, item = next(item)
                        defaults = defaults[2][type_name]
                    end

                    local _, err = fill_default_entity(defaults, item, root_object, current_path)
                    if err ~= nil then
                        return nil, err
                    end
                end
            end
        elseif opts[1] == DICTIONARY then
            if input[field_name] ~= nil then
                local child = input[field_name]
                local _, err = fill_default_entity(opts[2],
                    child, root_object, current_path)
                if err ~= nil then
                    return nil, err
                end
                if next(child) then
                    input[field_name] = child
                end
            end
        elseif opts[1] == UNION then
            if input[field_name] ~= nil then
                local child = input[field_name]
                local entry_type = next(child)
                child = child[entry_type]
                local child_defaults = opts[2][entry_type]
                local _, err = fill_default_entity(child_defaults,
                    child, root_object, current_path)
                if err ~= nil then
                    return nil, err
                end
                if next(child) then
                    input[field_name][entry_type] = child
                end
            end
        end
        table.remove(current_path)
    end
end

local function fill_defaults(defaults, input, type_name)
    if defaults[type_name] == nil then
        return true
    end

    local _, err = fill_default_entity(defaults[type_name], input, input, {})
    if err ~= nil then
        return nil, err
    end

    return true
end

return {
    validate_mdl_defaults = validate_mdl_defaults,
    fill_defaults = fill_defaults,
    fill_mdl_defaults = fill_mdl_defaults,
}
