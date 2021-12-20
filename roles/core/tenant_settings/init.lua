local module_name = 'tenant_settings'
local tenant = require('common.tenant')
local tenant_settings_error = require('errors').new_class(module_name)

local BASE_SPACE_NAME = 'tdg_tenant_settings'

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME, tenant.uid())
end

local function get_space()
    local space = box.space[get_space_name()]
    assert(space, 'tenant settings storage is not initialized')
    return space
end

local function apply_config()
    if box.info.ro then
        return
    end

    if box.space[get_space_name()] ~= nil then
        return
    end

    box.begin()

    local space = box.schema.space.create(get_space_name(), { if_not_exists = true })
    space:format({
        { name = 'key', type = 'string' },
        { name = 'value', type = 'any' },
    })
    space:create_index('id', {
        parts = {
            { field = 'key', type = 'string' },
        },
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })

    box.commit()
end

local function get(key)
    local tuple = get_space():get(key)

    if tuple == nil then
        return nil, tenant_settings_error:new('Attempt to get unexisting key %q', key)
    end
    return tuple.value
end

local function put(key, value)
    local res = get_space():replace({ key, value })
    return res.value
end

return {
    apply_config = apply_config,

    tenant_settings_get = get,
    tenant_settings_put = put,
}
