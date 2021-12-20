local digest = require('digest')
local cartridge = require('cartridge')

-- Warning! After modifying make Migration for it!
local function password_digest(password, salt)
    local saltpassword = password
    if salt ~= nil then
        saltpassword = password..salt
    end
    return digest.base64_encode(digest.sha512(saltpassword),
                                {nopad = true, urlsafe = true, nowrap = true})
end

local HARDPEPPER = '2d60ec7f-e9f0-4018-b354-c54907b9423d'

local function get_salted_password(password)
    local pepper = cartridge.config_get_readonly('pepper') or HARDPEPPER
    return password_digest(password, pepper)
end

return {
    password_digest = password_digest,
    get_salted_password = get_salted_password,
}
