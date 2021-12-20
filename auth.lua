local auth = require('common.admin.auth')
local account_provider = require('account_provider.account_provider')

-- Add a function to check the credentials
local function check_password(username, password)
    -- Check the credentials any way you like
    -- Return an authentication success or failure

    local user = auth.check_user_password(username, password)
    if user ~= nil then
        return true
    end

    return false
end

local function get_user(login)
    local user = account_provider.get_user_by_login(login)
    if user == nil then
        return nil
    end

    local version = 0
    if user.last_password_update_time ~= nil then
        version = user.last_password_update_time
    end

    return {
        username = user.login,
        fullname = user.username,
        email = user.email,
        version = version,
    }
end

return {
    check_password = check_password,
    get_user = get_user,
}
