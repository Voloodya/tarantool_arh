local digest = require('digest')
local msgpack = require('msgpack')

local function get_by_path(obj, path)
    local obj = obj
    for _, part in ipairs(path) do
        if obj == nil then
            return nil
        end
        obj = obj[part]
    end

    return obj
end

local function fields_slice(tuple, indexes)
    local res = {}
    for _, idx in ipairs(indexes) do
        table.insert(res, tuple[idx])
    end
    return res
end

--[[
    Function checks additional conditions such as
    `age >= 31` or `country == Russia`
    Returns matches, early_exit boolean values
]]
local function is_condition_match(tuple, filter_func, need_multiposition)
    -- Check for index conditions (including one or multicolumn)
    local multi_position
    if need_multiposition == true then
        multi_position = tuple[#tuple].multi_position
    end
    local matches, early_exit = filter_func(tuple, multi_position)
    return matches, early_exit
end

local function get_last_tuple_for_version(indexes, key_def, key, version)
    local primary = indexes.primary
    local history = indexes.history

    local tuple, _ = primary:get(key)
    if history == nil or version == nil or (tuple ~= nil and tuple.version <= version) then
        return tuple
    end

    local key_with_version
    local key_len = #key
    if type(key) == 'table' then
        key[key_len + 1] = version
        key_with_version = box.tuple.new(key)
        key[key_len + 1] = nil
    else
        key_with_version = key:update({{'=', #key + 1, version}})
    end

    if tuple ~= nil then
        local fun, param, state = history:pairs(key_with_version, 'LE')
        _, tuple = fun(param, state)
        if tuple ~= nil and key_def:compare_with_key(tuple, key) == 0 then
            return tuple
        end
    end

    return nil
end

local function is_cursor_valid(decoded)
    if type(decoded) ~= 'table' then
        return false
    end

    if decoded.scan == nil then
        return false
    end

    return true
end

local function decode_cursor(cursor)
    if cursor == nil then
        return nil
    end

    local ok, raw = pcall(digest.base64_decode, cursor)

    if not ok then
        return nil, string.format("Failed to decode cursor: %q", cursor)
    end

    local ok, decoded = pcall(msgpack.decode, raw)

    if not ok then
        return nil, string.format("Failed to decode cursor: %q", cursor)
    end

    if not is_cursor_valid(decoded) then
        return nil, string.format("Failed to decode cursor: %q", cursor)
    end

    return decoded
end

local function encode_cursor(cursor)
    if cursor == nil then
        return nil
    end

    local raw = msgpack.encode(cursor)
    local encoded = digest.base64_encode(raw, {nopad=true, nowrap=true,
                                               urlsafe=true})

    return encoded
end

return {
    get_by_path = get_by_path,
    fields_slice = fields_slice,
    is_condition_match = is_condition_match,
    get_last_tuple_for_version = get_last_tuple_for_version,
    decode_cursor = decode_cursor,
    encode_cursor = encode_cursor,
}
