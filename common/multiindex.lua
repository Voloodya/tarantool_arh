local function multi_json_path_postfix(path)
    local _, prefix_len = path:find('%[%*%]%.*')
    if prefix_len == nil then
        return nil
    end

    local postfix = path:sub(prefix_len + 1)

    if postfix == '' then
        return nil
    end

    return string.split(postfix, '.')
end

local function is_multi_json_path(path)
    return path:find('[*]', 0, true) ~= nil
end

local function multi_index_part(index)
    for pos, part in ipairs(index.parts) do
        if part.path ~= nil then
            if is_multi_json_path(part.path) then
                return pos
            end
        end
    end
    return nil
end

local function is_multi_index(index)
    return multi_index_part(index) ~= nil
end

return {
    multi_json_path_postfix = multi_json_path_postfix,
    multi_index_part = multi_index_part,

    is_multi_json_path = is_multi_json_path,
    is_multi_index = is_multi_index,
}
