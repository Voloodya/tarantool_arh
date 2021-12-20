local module_name = 'common.model_updater' -- luacheck: ignore

local decimal = require('decimal')
local uuid = require('uuid')
local utils = require('common.utils')
local errors = require('errors')
local model_flatten = require('common.model_flatten')

local update_error = errors.new_class("update_error")

local function box_null_or_value(value)
    if value == nil then
        return box.NULL
    end
    return value
end

local function update_table_value(value, op, operand)
    if op == '=' then
        return box_null_or_value(operand)
    elseif op == '+' then
        return value + operand
    elseif op == '-' then
        return value - operand
    end
end

local function is_path_exists(schema, type_name, path)
    local parsedpath = path:split('.')

    schema = schema[type_name][2].tree

    local head = {} -- for err message
    local ends = nil
    local pos = 1
    while pos <= #parsedpath do
        local item = parsedpath[pos]

        schema = schema[item]
        if schema == nil then
            return nil,
              update_error:new('Path key %q not found after %q',
                               item, table.concat(head, '.'))
        end

        if model_flatten.is_scalar_node(schema) then
            ends = pos
            break
        elseif model_flatten.is_array_node(schema) then
            if pos == #parsedpath then
                ends = pos
                break
            end

            if parsedpath[pos+1] == '*' or tonumber(parsedpath[pos+1]) ~= nil then
                -- skip next array index if it is valid
                pos = pos + 1
                schema = schema[2].tree
                if model_flatten.is_single_container_node(schema) then
                    schema = schema[2].tree
                end
            else
                return nil,
                  update_error:new('Invalid index %q for entity array after %q',
                      parsedpath[pos+1], table.concat(head, '.'))
            end

            if pos == #parsedpath then
                ends = pos
                break
            end
        elseif model_flatten.is_single_container_node(schema) then
            schema = schema[2].tree
        end

        pos = pos + 1
        table.insert(head, item)
    end

    if ends == nil then
        return nil, update_error:new('Path %q targets non-scalar node',
                                     path)
    end

    if ends < #parsedpath then
        return nil, update_error:new('Path part %q reach scalar node',
                                     table.concat(head, '.'))
    end
    return true
end

local update_operation_to_tarantool = {
    ['set'] = '=',
    ['add'] = '+',
    ['sub'] = '-',
}

local VALUE = 1
local ARRAY = 2
local NESTED_RECORD = 3

---
--- Build internal representation of update.
--- Since we support updates of nested entries in some cases
--- we should perform merge operations on user updates
--- and transform them to our storage schema.
--- IR (internal representation) is an table with following format
--- {
---    node_type = <VALUE/RECORD/UNION>
---    field_no = target field number
---    index = <not nil in case when array element is updated>
---    path = [field1, field2] -- number of field_id of record roots
---    tail = [field_N, field_N_p1, ...] -- we can't flatten all possible
---       structures e.g. array of array of record will be stored without flattening.
---       "path" part shows we should perform some kind of json-path update
---   op = <=/+/->
---   value = <value for update>
---   value_type = <value type>
---   union_root_fieldno = union type identifier field number
---   union_type = union type name
--- }
---
--- Note: IR is is not dependent on tuple values!
---

local function serialize_scalar_value(node_type, value)
    if value == nil then
        return nil
    end

    local err
    if node_type == model_flatten.DATE_TIME then
        value, err = utils.iso8601_str_to_nsec(value)
        if value == nil then
            return nil, err
        end
    elseif node_type == model_flatten.DATE then
        value, err = utils.date_str_to_nsec(value)
        if value == nil then
            return nil, err
        end
    elseif node_type == model_flatten.TIME then
        value, err = utils.time_str_to_nsec(value)
        if value == nil then
            return nil, err
        end
    elseif node_type == model_flatten.DECIMAL then
        local ok
        ok, value = pcall(decimal.new, value)
        if not ok then
            return nil, value
        end
    elseif node_type == model_flatten.UUID then
        if uuid.is_uuid(value) == false then
            local ok, res = pcall(uuid.fromstr, value)
            if ok == false or res == nil then
                return nil, string.format('Value %q is not UUID', value)
            end
            value = res
        end
    end
    return value
end

local function build_ir(ir, schema_node, op, value, ...)
    local head, meta = ...

    if head == nil then
        -- Array of scalars
        if model_flatten.is_scalar_node(schema_node) then
            ir.node_type = VALUE
            ir.field_no = schema_node[2].id

            local err
            value, err = serialize_scalar_value(schema_node[1], value)
            if err ~= nil then
                return nil, err
            end

            if schema_node[2].enum ~= nil then
                local values_array
                if type(value) == 'table' then
                    values_array = value
                else
                    values_array = {value}
                end
                local symbols = schema_node[2].enum.symbols
                for _, element in pairs(values_array) do
                    if symbols[element] == nil then
                        return nil, model_flatten.build_unknown_enum_error(element, schema_node[2].enum.name)
                    end
                end
            end

            ir.value = box_null_or_value(value)
            table.remove(ir.path)
        end
        return
    end

    ir.op = update_operation_to_tarantool[op]

    schema_node = schema_node[head]
    local node_type = schema_node[1]
    local opts = schema_node[2]
    if model_flatten.is_scalar_node(schema_node) then
        local err
        value, err = serialize_scalar_value(node_type, value)
        if err ~= nil then
            return nil, err
        end

        ir.node_type = VALUE
        ir.field_no = opts.id
        ir.value = box_null_or_value(value)

        if schema_node[2].enum ~= nil then
            if schema_node[2].enum.symbols[value] == nil then
                return nil, model_flatten.build_unknown_enum_error(value, opts.enum.name)
            end
        end

        return
    elseif node_type == model_flatten.NESTED_RECORD then
        ir.path = ir.path or {}
        ir.node_type = NESTED_RECORD
        ir.field_no = opts.id
        ir.value = box_null_or_value(value)
        ir.schema_node = schema_node

        table.insert(ir.path, opts.id)
        return build_ir(ir, opts.tree, op, value, select(2, ...))
    elseif node_type == model_flatten.ARRAY then
        ir.path = ir.path or {}
        ir.node_type = ARRAY
        ir.value = box_null_or_value(value)
        ir.index = meta
        ir.schema_node = schema_node

        local element_node = opts.tree
        table.insert(ir.path, opts.id)
        return build_ir(ir, element_node, op, value, select(3, ...))
    elseif node_type == model_flatten.UNION then
        local union_type = meta
        local subtype = opts.tree[union_type]
        if subtype == nil then
            return nil, update_error:new('Union type %q is not found', union_type)
        end
        ir.union_root_fieldno = opts.id
        ir.union_type = union_type
        return build_ir(ir, subtype, op, value, select(3, ...))
    else
        error('Unreachable')
    end
end

-- By default Tarantool consider empty array as a map
local function create_array()
    return setmetatable({}, {__serialize = 'array'})
end

local function merge_array(ir, update_list, field_updates, tuple, array_nodes)
    local path = ir.path or {}
    local index = ir.index

    local root_field_id = path[1] ~= nil and path[1] or {}

    local presence_array
    local len = 0
    if root_field_id ~= nil then
        presence_array = tuple[root_field_id]
        if presence_array ~= nil then
            len = #presence_array
        end
    end
    if presence_array == nil then
        presence_array = tuple[ir.field_no]
        if presence_array ~= nil then
            len = #presence_array
        end
    end
    if presence_array == nil then
        presence_array = {}
    end

    for i, path_part in ipairs(path) do
        local array, num
        if array_nodes[path_part] == nil then
            array = tuple[path_part]
            if array == nil then
                array = create_array()
            end
            num = #update_list + 1
        else
            num = array_nodes[path_part].num
            array = update_list[num][3]
        end

        local lower_bound, upper_bound

        -- This flag prohibits inserts via update
        local is_wildcard
        if index == '*' then
            is_wildcard = true
            lower_bound = 1
            upper_bound = len
        else
            is_wildcard = false
            index = tonumber(index)
            lower_bound = index
            upper_bound = index
        end

        local is_modified = false
        for array_index = lower_bound, upper_bound, 1 do
            if presence_array[array_index] ~= nil then
                -- TODO: handle nulls
                array[array_index] = true
            elseif not is_wildcard and array[array_index] ~= true then
                array[array_index] = true
                is_modified = true
            end
        end

        if i ~= 1 or is_modified then
            update_list[num] = {'=', path_part, array}
            array_nodes[path_part] = {num = num}
        end
    end

    for field_no, value in pairs(field_updates) do
        local array, num
        if array_nodes[field_no] == nil then
            array = tuple[field_no]
            if array == nil then
                array = create_array()
            end
            num = #update_list + 1
        else
            num = array_nodes[field_no].num
            array = update_list[num][3]
        end

        local is_wildcard
        local lower_bound, upper_bound
        if index == '*' then
            is_wildcard = true
            lower_bound = 1
            upper_bound = len
        else
            is_wildcard = false
            lower_bound = index
            upper_bound = index
        end

        for array_index = lower_bound, upper_bound, 1 do
            if presence_array[array_index] ~= nil then
                -- TODO: handle nulls
                array[array_index] = update_table_value(array[array_index], ir.op, value)
            elseif not is_wildcard then
                array[array_index] = update_table_value(array[array_index], ir.op, value)
            end
        end

        update_list[num] = {'=', field_no, array}
        array_nodes[field_no] = {num = #update_list}
    end
end

local function update_values(ir, update_list, field_updates)
    if ir.path ~= nil then
        for _, path_part in ipairs(ir.path) do
            table.insert(update_list, {'=', path_part, true})
        end
    end

    if ir.union_root_fieldno ~= nil then
        table.insert(update_list, {'=', ir.union_root_fieldno, ir.union_type})
    end

    for field_no, value in pairs(field_updates) do
        table.insert(update_list, {ir.op, field_no, value})
    end
end

local function assign_array(ir, update_list, field_updates, _, array_nodes)
    local fieldnos = ir.schema_node[2].fieldnos
    local len = #ir.value

    local stub = table.new(len, 0)
    for i = 1, len do
        stub[i] = box.NULL
    end

    for _, fieldno in ipairs(fieldnos) do
        local value
        if field_updates[fieldno] == nil then
            value = {'=', fieldno, table.copy(stub)}
        else
            value = {'=', fieldno, field_updates[fieldno]}
        end
        table.insert(update_list, value)
        array_nodes[fieldno] = {num = #update_list}
    end
end

local function process_ir_array(ir, tuple)
    local update_list = {}
    local array_nodes = {
        --[[  [field_id] = {num = <update_list index>} ]]
    }

    for _, action in ipairs(ir) do
        local node_type = action.node_type
        if node_type == VALUE then
            if action.index == nil then
                update_values(action, update_list, {[action.field_no] =  action.value})
            else
                merge_array(action, update_list, {[action.field_no] =  action.value}, tuple, array_nodes)
            end
        elseif node_type == ARRAY then
            local update_map = {}
            local _, err
            if action.index == nil then
                -- assign array
                _, err = model_flatten.dispatch_node(update_map, action.value, action.schema_node)
                if err ~= nil then
                    return nil, err
                end
                assign_array(action, update_list, update_map, tuple, array_nodes)
            else
                -- put element to array
                _, err = model_flatten.dispatch_node(update_map, action.value, action.schema_node[2].tree)
                if err ~= nil then
                    return nil, err
                end
                merge_array(action, update_list, update_map, tuple, array_nodes)
            end
        elseif node_type == NESTED_RECORD then
            local update_map = {}
            local _, err = model_flatten.dispatch_node(update_map, action.value, action.schema_node)
            if err ~= nil then
                return nil, err
            end
            table.remove(action.path)

            if action.index == nil then
                update_values(action, update_list, update_map)
            else
                merge_array(action, update_list, update_map, tuple, array_nodes)
            end
        end
    end

    return update_list
end

local function build_invalid_enum_error_msg(err, base_field_path)
    local full_path = base_field_path
    if err.node_path then
        full_path = full_path .. '.' .. err.node_path
    end
    return string.format("Invalid enum value %q for field %q of type %q",
            err.enum_value, full_path, err.enum_type)
end

local function is_unknown_enum_error(err)
    return errors.is_error_object(err) and err.err == "Unknown enum value"
end

local function format_update_lists(type_name, schema, tuples, updaters)
    local ir = {}
    for _, updater in ipairs(updaters) do
        local op, path, value = updater[1], updater[2], updater[3]
        local parts = path:split('.')
        local update_ir = {}
        local _, err = build_ir(update_ir, schema[type_name][2].tree, op, value, unpack(parts))
        if err ~= nil then
            if is_unknown_enum_error(err) then
                err = build_invalid_enum_error_msg(err, path)
            end
            return nil, err
        end
        table.insert(ir, update_ir)
    end

    local update_lists = {}
    for i, tuple in ipairs(tuples) do
        local tuple_update_list, err = process_ir_array(ir, tuple)
         if err ~= nil then
            if is_unknown_enum_error(err) then
                err = build_invalid_enum_error_msg(err, updaters[i][2])
            end
            return nil, err
         end
        table.insert(update_lists, tuple_update_list)
    end

    return update_lists
end

return {
    is_path_exists = is_path_exists,
    format_update_lists = format_update_lists,
}
