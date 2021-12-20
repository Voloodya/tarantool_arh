--[[
Function get_by_path allows you to take
value from object:table by path:string with delimiter:string (default: .)
--]]
local function get_by_path(object, path, delimiter)
    delimiter = delimiter or "."
    path = string.split(path, delimiter)

    for _, path_item in ipairs(path) do
      if type(object) ~= "table" then
        return nil
      end

      object = object[path_item]
    end

    return object
end


--[[
Function set_by_path allows you to assign
value to object:table by path:string with delimiter:string (default: .)
--]]
local function set_by_path(object, path, value, delimiter)
    delimiter = delimiter or "."
    path = string.split(path, delimiter)

    for i, path_item in ipairs(path) do
      if type(object) ~= "table" then
        return nil, "Some item on the way isn't table"
      end

      if i == #path then
        object[path_item] = value
        return true
      end

      if object[path_item] == nil then
        object[path_item] = {}
      end

      object = object[path_item]
    end

    return nil
end


--[[
Function just_get_and_set allows you to take
value from source_object:table and assign
value to target_object:table by path:string with dot delimiter
--]]
local function just_get_and_set(source_object, source_path, target_object, target_path)
    set_by_path(target_object, target_path, get_by_path(source_object, source_path))
end

return {
  get_by_path = get_by_path,
  set_by_path = set_by_path,
  just_get_and_set = just_get_and_set,
}
