local model_walker = require('common.model.walker')
local json = require('json')
local format = string.format

local function get_path(ctx)
    return table.concat(ctx.dst_path, '.')
end

local function put(ctx, s, ...)
    if s == 'end' or s == 'else' then
        ctx.nesting = ctx.nesting - 1
    end

    local indent = string.rep('    ', ctx.nesting)
    table.insert(ctx.lines, indent .. string.format(s,...))

    if s:startswith('if ') or s:startswith('for ') or s == 'else' then
        ctx.nesting = ctx.nesting + 1
    end
end

local function push_state(ctx)
    table.insert(ctx.states, {
        src_path = ctx.src_path,
        dst_path = ctx.dst_path,
        variable = ctx.variable,
        dst_index = ctx.dst_index,
    })
end

local function pop_state(ctx)
    local state = table.remove(ctx.states)
    if state == nil then
        print(debug.traceback())
        print(table.concat(ctx.lines, '\n'))
        error('Unexpected state: ' .. debug.traceback())
    end
    ctx.variable = state.variable
    ctx.dst_path = state.dst_path
    ctx.src_path = state.src_path
    ctx.dst_index = state.dst_index
end

local function is_validate_only(ctx)
    return #ctx.array_nesting > 1
end

-- format path
local function fp(ctx)
    if #ctx.src_path == 0 then
        return ''
    end
    local parts = '["' .. table.concat(ctx.src_path, '"]["') .. '"]'
    return parts
end

local function obj(ctx)
    return ctx.variable
end

local function idx(ctx)
    return ctx.dst_index
end

local function push_validation_path(ctx, subpath)
    table.insert(ctx.validation_path, subpath)
end

local function pop_validation_path(ctx)
    table.remove(ctx.validation_path)
end

local function is_path_const(ctx)
    return #ctx.validation_indexes == 0
end

local function push_validation_index(ctx, index)
    push_validation_path(ctx, '%d')
    table.insert(ctx.validation_indexes, index)
end

local function pop_validation_index(ctx)
    pop_validation_path(ctx, '%d')
    table.remove(ctx.validation_indexes)
end

local function path(ctx)
    local basepath = '"' .. table.concat(ctx.validation_path, '/') .. '"'

    if #ctx.validation_indexes > 0 then
        basepath = basepath .. ', ' .. table.concat(ctx.validation_indexes, ', ')
    end

    return basepath
end

-- push path
local function push_path(ctx, node)
    table.insert(ctx.src_path, node.name)
    table.insert(ctx.dst_path, node.name)
    push_validation_path(ctx, node.name)
end

-- pop path
local function pop_path(ctx)
    table.remove(ctx.src_path)
    table.remove(ctx.dst_path)
    pop_validation_path(ctx)
end

local function get_fieldno(ctx)
    local ddl = ctx.ddl
    local path = get_path(ctx)

    for i, field in ipairs(ddl.format) do
        if field.name == path then
            return i
        end
    end
end

local function before_record(node, ctx)
    if ctx.env.records[node.name] == nil then
        local fieldmap = {}
        for fi, f in ipairs(node.fields) do
            fieldmap[f.name] = fi
        end

        if is_path_const(ctx) then
            ctx.env.records[node.name] = function(data, path)
                if type(data) ~= 'table' then
                    error(format('%s is not a record %s: %s', path, node.name, json.encode(data)), 0)
                end

                for k, _ in pairs(data) do
                    if fieldmap[k] == nil then
                        error(format('%s/%s: Unknown field', path, k), 0)
                    end
                end
            end
        else
            ctx.env.records[node.name] = function(data, path, ...)
                if type(data) ~= 'table' then
                    local fullpath = format(path, ...)
                    error(format('%s is not a record %s: %s', fullpath, node.name, json.encode(data)), 0)
                end

                for k, _ in pairs(data) do
                    if fieldmap[k] == nil then
                        local fullpath = format(path, ...)
                        error(format('%s/%s: Unknown field', fullpath, k), 0)
                    end
                end
            end
        end
    end

    if ctx.root == nil then
        ctx.root = node
        push_validation_path(ctx, node.name)
        put(ctx, 'is_record_%s(%s%s, %s)', node.name, obj(ctx), fp(ctx), path(ctx))
        return
    end

    put(ctx, 'is_record_%s(%s%s, %s)', node.name, obj(ctx), fp(ctx), path(ctx))

    if not is_validate_only(ctx) then
        local fieldno = get_fieldno(ctx)
        if fieldno == nil then -- union
            return
        end

        put(ctx, 'result[%d]%s = true', fieldno, idx(ctx))
        local num = #ctx.states + 1
        put(ctx, 'local tmp_%d = %s%s', num, obj(ctx), fp(ctx))

        push_state(ctx)

        ctx.variable = 'tmp_' .. num
        ctx.src_path = {}
    end
end

local function after_record(node, ctx)
    if ctx.root == node then
        return
    end

    if not is_validate_only(ctx) then
        local fieldno = get_fieldno(ctx)
        if fieldno == nil then -- union
            return
        end

        pop_state(ctx)
    end
end

local function before_array(xtype, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'is_array'
    else
        fn_name = 'is_array_var'
    end
    put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))

    local fieldno = get_fieldno(ctx)
    table.insert(ctx.array_nesting, true)

    push_state(ctx)

    -- Important to preserve trailing dot here
    local path_prefix = table.concat(ctx.dst_path, '.') .. '.'

    if not is_validate_only(ctx) then
        put(ctx, 'result[%d]%s = {}', fieldno, idx(ctx))
        for i, field in ipairs(ctx.ddl.format) do
            if field.name:startswith(path_prefix) then
                put(ctx, 'result[%d]%s = {}', i, idx(ctx))
            end
        end
    else
        if fieldno ~= nil then
            put(ctx, 'result[%d]%s = %s%s', fieldno, idx(ctx), obj(ctx), fp(ctx))
        end
    end

    local dst_index_no = #ctx.array_nesting
    local dst_index = string.format('i%d', dst_index_no)
    push_validation_index(ctx, dst_index)
    ctx.dst_index =  '[' .. dst_index .. ']'

    put(ctx, 'for %s, value in ipairs(%s%s) do', dst_index, obj(ctx), fp(ctx))

    ctx.variable = 'value'
    ctx.src_path = {}

    if xtype.items.nullable == true then
        put(ctx, 'if value ~= nil then', fieldno)
    end
end

local function after_array(xtype, ctx)
    table.remove(ctx.array_nesting)

    local dst_index = ctx.dst_index

    pop_validation_index(ctx)
    pop_state(ctx)

    if not is_validate_only(ctx) then
        local fieldno = get_fieldno(ctx)
        if xtype.items.nullable == true then
            put(ctx, 'else', fieldno)
            put(ctx, 'result[%d]%s = NULL', fieldno, dst_index)
            put(ctx, 'end', fieldno)
        end
    end

    put(ctx, 'end')
end

local function before_int(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_int'
    else
        fn_name = 'to_int_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_int(_, _)
end

local function before_boolean(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_boolean'
    else
        fn_name = 'to_boolean_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_boolean(_, _)
end

local function before_long(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_long'
    else
        fn_name = 'to_long_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_long(_, _)
end

local function before_double(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_double'
    else
        fn_name = 'to_double_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_double(_, _)
end

local function before_string(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_string'
    else
        fn_name = 'to_string_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)',
            fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_string(_, _)
end

local function before_any(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_any'
    else
        fn_name = 'to_any_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)',
            fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_any(_, _)
end

local function before_enum(node, ctx)
    if ctx.env.enums[node.name] == nil then
        local symbols_map = {}
        for _, symbol in ipairs(node.symbols) do
            symbols_map[symbol] = true
        end

        if is_path_const(ctx) then
            ctx.env.enums[node.name] = function(data, path)
                if symbols_map[data] == nil then
                    error(format('%s is not an enum %s: %s', path, node.name, json.encode(data)), 0)
                end
                return data
            end
        else
            ctx.env.enums[node.name] = function(data, path, ...)
                if symbols_map[data] == nil then
                    local fullpath = format(path, ...)
                    error(format('%s is not an enum %s: %s', fullpath, node.name, json.encode(data)), 0)
                end
                return data
            end
        end
    end

    if is_validate_only(ctx) then
        put(ctx, 'is_enum_%s(%s%s, %s)',
            node.name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = is_enum_%s(%s%s, %s)',
            fieldno, idx(ctx), node.name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_enum(_, _)
end

local function before_datetime(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_datetime'
    else
        fn_name = 'to_datetime_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_datetime(_, _)

end

local function before_date(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_date'
    else
        fn_name = 'to_date_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_date(_, _)

end

local function before_time(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_time'
    else
        fn_name = 'to_time_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_time(_, _)
end

local function before_decimal(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_decimal'
    else
        fn_name = 'to_decimal_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_decimal(_, _)
end

local function before_uuid(_, ctx)
    local fn_name
    if is_path_const(ctx) then
        fn_name = 'to_uuid'
    else
        fn_name = 'to_uuid_var'
    end

    if is_validate_only(ctx) then
        put(ctx, '%s(%s%s, %s)', fn_name, obj(ctx), fp(ctx), path(ctx))
    else
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = %s(%s%s, %s)',
            fieldno, idx(ctx), fn_name, obj(ctx), fp(ctx), path(ctx))
    end
end

local function after_uuid(_, _)
end

local function is_nullable_union(xtype)
    for _, union_type in ipairs(xtype) do
        if union_type == 'null' then
            return true
        end
    end
    return false
end

local function before_union(xtype, ctx)
    if is_nullable_union(xtype) then
        put(ctx, 'if %s%s ~= nil then', obj(ctx), fp(ctx))
    end

    local union_num = #ctx.env.unions + 1
    local union_tag_map = {}
    for _, union_type in ipairs(xtype) do
        if type(union_type) == 'table' then
            union_tag_map[union_type.name] = true
        end
    end

    if is_path_const(ctx) then
        table.insert(ctx.env.unions, function(data, path)
            if type(data) ~= 'table' then
                error(format('%s is not a union: %s', path, json.encode(data)), 0)
            end

            local union_type, union_value = next(data)
            if union_tag_map[union_type] == nil then
                error(format('%s: Unexpected union type %s', path, json.encode(union_type)), 0)
            end

            if next(data, union_type) ~= nil then
                error(format('%s: union should have only one branch', path), 0)
            end
            return union_type, union_value
        end)
    else
        table.insert(ctx.env.unions, function(data, path, ...)
            if type(data) ~= 'table' then
                local fullpath = format(path, ...)
                error(format('%s is not a union: %s', fullpath, json.encode(data)), 0)
            end

            local union_type, union_value = next(data)
            if union_tag_map[union_type] == nil then
                local fullpath = format(path, ...)
                error(format('%s: Unexpected union type %s', fullpath, json.encode(union_type)), 0)
            end

            if next(data, union_type) ~= nil then
                local fullpath = format(path, ...)
                error(format('%s: union should have only one branch', fullpath), 0)
            end
            return union_type, union_value
        end)
    end

    put(ctx, 'local union_type, union_value = to_union_%d(%s%s)', union_num, obj(ctx), fp(ctx))
    if not is_validate_only(ctx) then
        local fieldno = get_fieldno(ctx)
        put(ctx, 'result[%d]%s = union_type', fieldno, idx(ctx))
    end

    push_state(ctx)

    ctx.variable = 'union_value'
    ctx.src_path = {}
end

local function after_union(xtype, ctx)
    local fieldno = get_fieldno(ctx)
    if is_nullable_union(xtype) and fieldno ~= nil then
        put(ctx, 'else')
        put(ctx, 'result[%d]%s = NULL', fieldno, idx(ctx))
        put(ctx, 'end')
    end

    pop_state(ctx)
end

local function before_union_type(node, ctx)
    if node == 'null' then
        return
    end

    push_validation_path(ctx, node.name)

    table.insert(ctx.dst_path, node.name)
    put(ctx, 'if union_type == %q then', node.name)
end

local function after_union_type(node, ctx)
    if node == 'null' then
        return
    end

    pop_validation_path(ctx)

    table.remove(ctx.dst_path)
    put(ctx, 'end', node.name)
end

local function before_field(node, ctx)
    push_path(ctx, node)

    if type(node) == 'table' and node.type.nullable == true then
        put(ctx, 'if %s%s ~= nil then', obj(ctx), fp(ctx))
    end
end

local function after_field(node, ctx)
    if type(node) == 'table' and node.type.nullable == true then
        if not is_validate_only(ctx) then
            local fieldno = get_fieldno(ctx)
            put(ctx, 'else', fieldno, idx(ctx))
            put(ctx, 'result[%d]%s = NULL', fieldno, idx(ctx))
        end
        put(ctx, 'end')
    end

    pop_path(ctx, node)
end

local function before_nullable() end
local function after_nullable() end

local function before_null() end
local function after_null() end

local callbacks = {
    before_nullable = before_nullable,
    after_nullable = after_nullable,

    before_field = before_field,
    after_field = after_field,

    before_record = before_record,
    after_record = after_record,
    before_array = before_array,
    after_array = after_array,
    before_enum = before_enum,
    after_enum = after_enum,
    before_datetime = before_datetime,
    after_datetime = after_datetime,
    before_date = before_date,
    after_date = after_date,
    before_time = before_time,
    after_time = after_time,
    before_decimal = before_decimal,
    after_decimal = after_decimal,
    before_uuid = before_uuid,
    after_uuid = after_uuid,
    before_union = before_union,
    after_union = after_union,
    before_union_type = before_union_type,
    after_union_type = after_union_type,

    before_null = before_null,
    after_null = after_null,
    before_int = before_int,
    after_int = after_int,
    before_boolean = before_boolean,
    after_boolean = after_boolean,
    before_string = before_string,
    after_string = after_string,
    before_long = before_long,
    after_long = after_long,
    before_double = before_double,
    after_double = after_double,
    before_any = before_any,
    after_any = after_any,
}

local new = model_walker.new(callbacks)

return {
    new = new,
}
