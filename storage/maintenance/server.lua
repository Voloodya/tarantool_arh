local tenant = require('common.tenant')
local model_explorer = require('common.model_explorer')

local function get_aggregates(_, _)
    local mdl, err = tenant.get_mdl()
    if err then
        return nil, err
    end

    local objects = model_explorer.make_object_map(mdl)
    if objects == nil then
        return {}
    end

    local res = {}
    for name, obj in pairs(objects) do
        if obj.indexes ~= nil then
            table.insert(res, { name = name })
        end
    end

    return res
end

return {
    get_aggregates = get_aggregates,
}
