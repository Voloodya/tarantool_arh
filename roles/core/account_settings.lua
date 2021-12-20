local account_settings = require('roles.core.account_settings.init')

local function apply_config()
    local _, err = account_settings.apply_config()
    if err ~= nil then
        return nil, err
    end

    return true
end

local function get(key)
    return account_settings.get(key)
end

local function put(key, value)
    return account_settings.put(key, value)
end

local function delete(key)
    return account_settings.delete(key)
end

return {
    apply_config = apply_config,

    -- rpc registry
    account_settings_get = get,
    account_settings_put = put,
    account_settings_delete = delete,
}
