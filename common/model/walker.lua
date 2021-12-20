local function no_op() end

local no_op_callbacks = {
    before_nullable = no_op,
    after_nullable = no_op,

    before_field = no_op,
    after_field = no_op,

    before_record = no_op,
    after_record = no_op,
    before_array = no_op,
    after_array = no_op,
    before_enum = no_op,
    after_enum = no_op,
    before_datetime = no_op,
    after_datetime = no_op,
    before_date = no_op,
    after_date = no_op,
    before_time = no_op,
    after_time = no_op,
    before_decimal = no_op,
    after_decimal = no_op,
    before_uuid = no_op,
    after_uuid = no_op,
    before_union = no_op,
    after_union = no_op,
    before_union_type = no_op,
    after_union_type = no_op,

    before_null = no_op,
    after_null = no_op,
    before_int = no_op,
    after_int = no_op,
    before_boolean = no_op,
    after_boolean = no_op,
    before_string = no_op,
    after_string = no_op,
    before_long = no_op,
    after_long = no_op,
    before_double = no_op,
    after_double = no_op,
    before_any = no_op,
    after_any = no_op,

    process_ctx = no_op
}

local function push_path(ctx, node)
    if node ~= nil then
        table.insert(ctx.path, node.name)
    end
end

local function pop_path(ctx)
    table.remove(ctx.path)
end

local decorate_with_push_path_in_ctx = function(f)
    return function (node, ctx)
        push_path(ctx, node)
        f(node, ctx)
    end
end

local decorate_with_pop_path_from_ctx = function(f)
    return function (node, ctx)
        f(node, ctx)
        pop_path(ctx)
    end
end

local path_decorators = {
    before_union_type = decorate_with_push_path_in_ctx,
    before_field = decorate_with_push_path_in_ctx,

    after_union_type = decorate_with_pop_path_from_ctx,
    after_field = decorate_with_pop_path_from_ctx
}

local function build_callbacks(overrides, opts)
    opts = opts or {}

    local callbacks = table.copy(no_op_callbacks)
    for callback_name, f in pairs(overrides) do
        callbacks[callback_name] = f
    end

    if opts.with_path_decorators then
        for callback_name, decorate in pairs(path_decorators) do
            callbacks[callback_name] = decorate(callbacks[callback_name])
        end
    end

    return callbacks
end

local function new(callbacks)
    local function walk(schema, ctx)
        local schematype = type(schema) == 'string' and schema or schema.type

        if schema.nullable then
            callbacks.before_nullable(schema, ctx)
        end

        if schematype == 'null' then
            callbacks.before_null(schema, ctx)
            callbacks.after_null(schema, ctx)
        elseif type(schema) == 'table' and schema.logicalType == 'DateTime' then
            callbacks.before_datetime(schema, ctx)
            callbacks.after_datetime(schema, ctx)
        elseif type(schema) == 'table' and schema.logicalType == 'Date' then
            callbacks.before_date(schema, ctx)
            callbacks.after_date(schema, ctx)
        elseif type(schema) == 'table' and schema.logicalType == 'Time' then
            callbacks.before_time(schema, ctx)
            callbacks.after_time(schema, ctx)
        elseif type(schema) == 'table' and schema.logicalType == 'Decimal' then
            callbacks.before_decimal(schema, ctx)
            callbacks.after_decimal(schema, ctx)
        elseif type(schema) == 'table' and schema.logicalType == 'UUID' then
            callbacks.before_uuid(schema, ctx)
            callbacks.after_uuid(schema, ctx)
        elseif schematype == 'boolean' then
            callbacks.before_boolean(schema, ctx)
            callbacks.after_boolean(schema, ctx)
        elseif schematype == 'int' then
            callbacks.before_int(schema, ctx)
            callbacks.after_int(schema, ctx)
        elseif schematype == 'long' then
            callbacks.before_long(schema, ctx)
            callbacks.after_long(schema, ctx)
        elseif schematype == 'double' or schematype == 'float' then
            callbacks.before_double(schema, ctx)
            callbacks.after_double(schema, ctx)
        elseif schematype == 'bytes' or schematype == 'string' then
            callbacks.before_string(schema, ctx)
            callbacks.after_string(schema, ctx)
        elseif schematype == 'enum' then
            callbacks.before_enum(schema, ctx)
            callbacks.after_enum(schema, ctx)
        else
            -- record, enum, array, map
            if schematype == 'record' then
                callbacks.before_record(schema, ctx)
                for _, field in ipairs(schema.fields) do
                    callbacks.before_field(field, ctx)
                    walk(field.type, ctx)
                    callbacks.after_field(field, ctx)
                end
                callbacks.after_record(schema, ctx)
            elseif schematype == 'array'  then
                local process_items = callbacks.before_array(schema, ctx)
                if process_items ~= false then
                    walk(schema.items, ctx)
                end
                callbacks.after_array(schema, ctx)
            elseif not schematype then -- union
                callbacks.before_union(schema, ctx)

                for _, union_type in ipairs(schema) do
                    callbacks.before_union_type(union_type, ctx)
                    walk(union_type, ctx)
                    callbacks.after_union_type(union_type, ctx)
                end

                callbacks.after_union(schema, ctx)
            elseif schematype == 'any' then
                callbacks.before_any(schema, ctx)
                callbacks.after_any(schema, ctx)
            else
                assert(false)
            end
        end

        if schema.nullable then
            callbacks.after_nullable(schema, ctx)
        end
    end
    return walk
end

return {
    new = new,

    build_callbacks = build_callbacks
}
