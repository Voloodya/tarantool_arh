local function get_type(field)
    if type(field.type) ~= 'table' then
        return {
            name = field.type,
            is_nullable = (field.nullable == true),
            is_array = false,
            is_object = false,
        }
    end

    local val = field.type

    if val.type == 'record' then
        return {
            name = val.name,
            is_nullable = (val.nullable == true),
            is_array = false,
            is_object = true,
        }
    end

    if val.type == 'array' then
        if type(val.items) == 'table' then
            return {
                name = val.items.name,
                is_nullable = (val.nullable == true),
                is_array = true,
                is_object = true,
            }
        else
            return {
                name = val.items,
                is_nullable = (val.nullable == true),
                is_array = true,
                is_object = false,
            }
        end
    end

    return {
        name = val,
        is_nullable = true,
        is_array = false,
        is_object = false,
    }
end

local function make_object_map(mdl)
    local objects = {}

    for _, type_info in ipairs(mdl or {}) do
        local obj = { doc = type_info.doc }

        if type_info.type == 'enum' then
            obj.type = 'enum'
        else
            local fields = {}

            for _, field in ipairs(type_info.fields or {}) do
                table.insert(fields, {
                    name = field.name,
                    doc = field.doc,
                    type = get_type(field),
                })
            end

            obj.fields = fields
            obj.indexes = type_info.indexes
        end

        objects[type_info.name] = obj
    end

    return objects
end

return {
    make_object_map = make_object_map,
}
