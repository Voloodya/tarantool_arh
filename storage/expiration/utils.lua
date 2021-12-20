local defaults = require('common.defaults')
local cartridge = require('cartridge')

local function get_default_keep_version_count()
    return cartridge.config_get_readonly('default_keep_version_count') or defaults.DEFAULT_KEEP_VERSION_COUNT
end

local function get_version_count(expiration)
    if expiration == nil or expiration.enabled ~= true  then
        return nil
    end

    if expiration.keep_version_count ~= nil then
        return expiration.keep_version_count
    end

    return get_default_keep_version_count()
end

local function get_lifetime_nsec(expiration)
    if expiration == nil or expiration.enabled ~= true or expiration.lifetime_hours == nil  then
        return nil
    end

    return expiration.lifetime_hours * 3600ULL * 1e9
end

return {
    get_version_count = get_version_count,
    get_default_keep_version_count = get_default_keep_version_count,
    get_lifetime_nsec = get_lifetime_nsec,
}
