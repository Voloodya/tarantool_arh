local model_walker = require('common.model.walker')
local model = require('common.model')

local function get_path(ctx)
    return table.concat(ctx.src_path, '.')
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

-- format path
local function fp(ctx)
    if #ctx.dst_path == 0 then
        return ''
    end
    local parts = '["' .. table.concat(ctx.dst_path, '"]["') .. '"]'
    return parts
end

local function src(ctx)
    return ctx.src_variable
end

local function dst(ctx)
    return ctx.dst_variable
end

local function dst_idx(ctx)
    return ctx.dst_index
end

local function src_idx(ctx)
    return ctx.src_index
end

-- push path
local function push_path(ctx, node)
    table.insert(ctx.src_path, node.name)
    table.insert(ctx.dst_path, node.name)
end

-- pop path
local function pop_path(ctx)
    table.remove(ctx.src_path)
    table.remove(ctx.dst_path)
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

local function before_record(xtype, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    if #ctx.array_nesting > 1 then
        return
    end

    if xtype.nullable then
        put(ctx, 'if %s[%d]%s == true then', src(ctx), fieldno, src_idx(ctx))
    end
    put(ctx, '%s%s%s = table_new(0, %d)', dst(ctx), fp(ctx), dst_idx(ctx), #xtype.fields)
end

local function after_record(xtype, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    if #ctx.array_nesting > 1 then
        put(ctx, '%s%s%s = %s[%d]%s', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
        return
    end

    if xtype.nullable then
        put(ctx, 'end')
    end
end

local function before_array(xtype, ctx)
    local fieldno = get_fieldno(ctx)

    table.insert(ctx.array_nesting, true)

    if fieldno == nil then
        return
    end

    if #ctx.array_nesting > 1 then
        return
    end

    if xtype.nullable == true then
        put(ctx, 'if %s[%d] ~= nil then', src(ctx), fieldno)
    end

    table.insert(ctx.states, {
        src_path = ctx.src_path,
        dst_path = ctx.dst_path,
        src_variable = ctx.src_variable,
        dst_variable = ctx.dst_variable,
        src_index = ctx.src_index,
        dst_index = ctx.dst_index,
    })

    local process_items
    if model.is_primitive_or_derived_type(xtype.items) then
        put(ctx, 'local array = %s[%d]', src(ctx), fieldno)
        process_items = false
    elseif xtype.items.type == 'enum' then
        put(ctx, 'local array = %s[%d]', src(ctx), fieldno)
        process_items = false
    elseif model.is_array(xtype.items) then
        put(ctx, 'local array = %s[%d]', src(ctx), fieldno)
        process_items = false
    elseif model.is_union_type(xtype.items) then
        put(ctx, 'local array = {}')

        put(ctx, 'for i, value in ipairs(%s[%d]) do', src(ctx), fieldno)

        if xtype.items.nullable == true then
            put(ctx, 'if value == nil then')
            put(ctx, 'array[i] = NULL')
            put(ctx, 'else')
        end
        process_items = true
    else
        put(ctx, 'local array = {}')

        put(ctx, 'for i, value in ipairs(%s[%d]) do', src(ctx), fieldno)

        if xtype.items.nullable == true then
            put(ctx, 'if value == nil then')
            put(ctx, 'array[i] = NULL')
            put(ctx, 'else')
        end

        process_items = true
    end

    ctx.dst_path = {}
    ctx.dst_variable = 'array[i]'
    ctx.src_index = '[i]'
    return process_items
end

local function after_array(xtype, ctx)
    table.remove(ctx.array_nesting)
    if #ctx.array_nesting > 0 then
        return
    end

    local state = table.remove(ctx.states)
    ctx.src_variable = state.src_variable
    ctx.dst_variable = state.dst_variable
    ctx.dst_path = state.dst_path
    ctx.src_path = state.src_path
    ctx.src_index = state.src_index
    ctx.dst_index = state.dst_index

    if model.is_primitive_or_derived_type(xtype.items) == false and
        xtype.items.type ~= 'enum' and
        model.is_array(xtype.items) == false then

        if xtype.items.nullable == true then
            put(ctx, 'end')
        end

        put(ctx, 'end')
    end
    put(ctx, '%s%s%s = array', dst(ctx), fp(ctx), dst_idx(ctx))

    if xtype.nullable == true then
        put(ctx, 'end')
    end
end

local function before_primitive(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = %s[%d]%s', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_primitive(_, _)
end

local function before_enum(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = %s[%d]%s', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_enum(_, _)
end

local function before_datetime(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = from_datetime(%s[%d]%s)', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_datetime(_, _)
end

local function before_date(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = from_date(%s[%d]%s)', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_date(_, _)
end

local function before_time(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = from_time(%s[%d]%s)', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_time(_, _)
end

local function before_decimal(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = from_decimal(%s[%d]%s)', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_decimal(_, _)
end

local function before_uuid(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = from_uuid(%s[%d]%s)', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_uuid(_, _)
end

local function before_union(_, ctx)
    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, 'local union_type = %s[%d]%s', src(ctx), fieldno, src_idx(ctx))
    put(ctx, 'if union_type ~= nil then')
    put(ctx, '%s%s%s = { [union_type] = {} }', dst(ctx), fp(ctx), dst_idx(ctx))
    put(ctx, 'end')
end

local function after_union(_, _)
end

local function before_union_type(node, ctx)
    if node == 'null' then
        return
    end

    push_path(ctx, node)
    put(ctx, 'if union_type == %q then', node.name)

    local fieldno = get_fieldno(ctx)
    if fieldno == nil then
        return
    end

    put(ctx, '%s%s%s = %s[%d]%s', dst(ctx), fp(ctx), dst_idx(ctx), src(ctx), fieldno, src_idx(ctx))
end

local function after_union_type(node, ctx)
    if node == 'null' then
        return
    end

    pop_path(ctx)
    put(ctx, 'end')
end

local function before_field(node, ctx)
    push_path(ctx, node)
end

local function after_field(node, ctx)
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
    before_int = before_primitive,
    after_int = after_primitive,
    before_boolean = before_primitive,
    after_boolean = after_primitive,
    before_string = before_primitive,
    after_string = after_primitive,
    before_long = before_primitive,
    after_long = after_primitive,
    before_double = before_primitive,
    after_double = after_primitive,
    before_any = before_primitive,
    after_any = after_primitive,
}

local new = model_walker.new(callbacks)

return {
    new = new,
}
