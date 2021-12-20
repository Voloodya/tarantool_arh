local account_provider = require('account_provider.account_provider')

local function init()
    return account_provider.init()
end

local function apply_config(cfg)
    return account_provider.apply_config(cfg)
end

local function validate_config(cfg)
    return account_provider.validate_config(cfg)
end

return {
    validate_config = validate_config,
    apply_config = apply_config,
    init = init,

    permanent = true,
    role_name = 'account_provider',
    dependencies = {},
}
