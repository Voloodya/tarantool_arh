local request_context = require('common.request_context')

local function set_account(account)
    local context = request_context.get()

    if context.account == nil then
        error('Expects the default account to be set\n' .. debug.traceback())
    end

    context.account = account
end

local function get_account()
    local context = request_context.get()

    if context.account == nil then
        error('Expects the account to be set\n' .. debug.traceback())
    end

    return context.account
end

local function init(context)
    context.account = {
        is_unauthorized = true }
end

local function is_empty()
    if request_context.is_empty() then
        return true
    end

    local context = request_context.get()
    return context.account == nil
end

local function set_user(user)
    set_account({
        is_user = true,
        id = user.uid,
        name = user.login,
        role_id = user.role_id,
        tenant = user.tenant ~= nil and user.tenant or nil,
    })
end

local function set_token(token)
    set_account({
        is_token = true,
        id = token.uid,
        name = token.name,
        role_id = token.role_id,
        tenant = token.tenant ~= nil and token.tenant or nil,
    })
end

local function set_anonymous(tenant_uid)
    set_account({
        is_anonymous = true,
        tenant = tenant_uid,
    })
end

local function is_user()
    local account = get_account()
    return account.is_user == true
end

local function is_token()
    local account = get_account()
    return account.is_token == true
end

local function is_anonymous()
    local account = get_account()
    return account.is_anonymous == true
end

local function is_unauthorized()
    local account = get_account()
    return account.is_unauthorized == true
end

local function id()
    local account = get_account()
    return account.id
end

local function role_id()
    local account = get_account()
    return account.role_id
end

local function name()
    local account = get_account()
    return account.name
end

local function tenant()
    local account = get_account()
    return account.tenant
end

local function kind()
    local account = get_account()

    if account.is_token == true then
        return 'token'
    elseif account.is_user == true then
        return 'user'
    elseif account.is_anonymous == true then
        return 'anonymous'
    elseif account.is_unauthorized == true then
        return 'unauthorized'
    end

    error('Invalid account type\n' .. debug.traceback())
end

local function tostring()
    local account = get_account()

    if account.str == nil then
        if account.is_token == true then
            account.str = ('token %q'):format(account.name)
        elseif account.is_user == true then
            account.str = ('user %q'):format(account.name)
        elseif account.is_anonymous == true then
            account.str = 'anonymous'
        elseif account.is_unauthorized == true then
            account.str = 'unauthorized'
        else
            error('Invalid account type\n' .. debug.traceback())
        end
    end

    return account.str
end

return {
    -- This method is for colling only from request_context.init()
    init = init,
    is_empty = is_empty,

    set_user = set_user,
    set_token = set_token,
    set_anonymous = set_anonymous,
    set_account = set_account,

    is_user = is_user,
    is_token = is_token,
    is_anonymous = is_anonymous,
    is_unauthorized = is_unauthorized,

    kind = kind,

    id = id,
    name = name,
    role_id = role_id,
    tenant = tenant,

    tostring = tostring
}
