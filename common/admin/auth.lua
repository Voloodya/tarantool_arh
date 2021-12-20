local module_name = 'common.admin.auth'

local log = require('log.log').new(module_name)

local audit_log = require('audit.log').new(module_name)

local errors = require('errors')
local cartridge = require('cartridge')
local checks = require('checks')
local cartridge_auth = require('cartridge.auth')
local cartridge_auth_http_authorize_request = cartridge_auth.authorize_request

local account_provider = require('account_provider.account_provider')
local activity_observer = require('account_provider.activity_observer')
local account = require('common.admin.account')
local token_acl = require('common.admin.token_acl')
local account_states = require('account_manager.states')
local access_roles = require('account_manager.access_role')
local access_actions_list = require('account_manager.access_actions_list')

local auth_error = errors.new_class('authorization_error')
local access_error = errors.new_class('not enough access rights')
local tenant_lib = require('common.tenant')

local account_types = {
    USER =  { type = 'user', credentials = 'cookie' },
    TOKEN = { type = 'token', credentials = 'token' },
    TOKEN_NAME = { type = 'token', credentials = 'token_name' },
}

local function is_anonymous_allowed()
    return not cartridge_auth.get_enabled()
end

local function has_access(account, account_type)
    local tenant, err = tenant_lib.get(account.tenant)
    if err ~= nil then
        return false, err
    end

    if tenant ~= nil and tenant.state == tenant_lib.states.BLOCKED then
        return false, string.format(
            'Attempt to authorize with %s but tenant %q is blocked',
            account_type.credentials,
            account.tenant
        )
    end

    if account.state == account_states.BLOCKED then
        return false, string.format(
            'Attempt to authorize with %s but %s is blocked',
            account_type.credentials,
            account_type.type
        )
    end

    return true
end

local function is_token_name_valid(name)
    local token, err = account_provider.token_get_by_name(name)
    if token == nil then
        local err_msg = string.format('Failed to find a token %q', name)
        if err ~= nil then
            err_msg = string.format('%s: %s', err_msg, err)
        end
        return false, err_msg
    end

    local ok, err = has_access(token, account_types.TOKEN_NAME)
    if not ok then
        return false, err
    end

    return true
end

local function authorize_with_token_name(name)
    local token, err = account_provider.token_get_by_name(name)
    if token == nil or token.uid == nil then
        if err == nil then
            audit_log.warn('Access denied. Unknown token %q', name)
        else
            audit_log.warn('Access denied. Some error has occurred with token %q', name)
            log.error(err)
        end
        return false
    end

    local _, err = has_access(token, account_types.TOKEN_NAME)
    if err ~= nil then
        audit_log.warn(err)
        return false
    end

    account.set_token(token)
    audit_log.verbose('Access granted by token')
    activity_observer.push()

    return true
end

local function authorize_with_external_plugin(request)
    if account_provider.external_auth_enabled() then
        local auth_obj, err = account_provider.check_external_auth(request)
        if auth_obj == nil then
            audit_log.warn('Error while performing external authentication: %s', err)
            return { granted = false }, {
                is_error = true,
                message = err
            }
        end

        if type(auth_obj) ~= 'table' then
            return { granted = false }, {
                is_error = true,
                message = 'Invalid auth plugin response',
            }
        end

        if auth_obj.decision == 'reject' then
            audit_log.verbose('Access denied by external auth: %s', auth_obj.reason)
            return { granted = false }, {
                is_error = false,
                message = auth_obj.reason,
            }
        end

        if auth_obj.decision == 'accept' then
            local external_account = auth_obj.account
            if external_account == nil then
                return { granted = false }, {
                    is_error = true,
                    message = 'Invalid account is returned from auth plugin',
                }
            end
            account.set_account(external_account)
            audit_log.verbose('Access granted by external token')
            return { granted = true }
        end

        if auth_obj.decision ~= 'fallback' then
            return { granted = false }, {
                is_error = true,
                message = 'Invalid auth plugin decision',
            }
        end
    end
    return { granted = false, fallback = true }
end

local function authorize_with_token_impl(token)
    if token ~= nil then
        local info, err = token_acl.get_token_info(token)
        if info == nil then
            if err == nil then
                audit_log.warn('Attempt to authorize with token, but token %q is unknown', token)
            else
                audit_log.warn('Attempt to authorize with token %q, but some error has occurred', token)
                log.error(err)
            end
            return { granted = false, fallback = true }
        end

        local _, err = has_access(info, account_types.TOKEN)
        if err ~= nil then
            audit_log.warn(err)
            return { granted = false }
        end

        if info.state == account_states.NEW then
            info.role_id = access_roles.SYSTEM_ROLES.ONE_TIME_ACCESS
        end

        account.set_token(info)
        audit_log.verbose('Access granted by token')
        activity_observer.push()
        return { granted = true }
    end
    return { granted = false, fallback = true }
end

local function authorize_with_token_from_request(request)
    local token = token_acl.get_token_from_request(request)
    return authorize_with_token_impl(token)
end

local function authorize_with_cookies(request)
    if cartridge_auth_http_authorize_request(request) then
        local login = cartridge.http_get_username()
        if login ~= nil then
            local user, err = account_provider.get_user_by_login(login)
            if user == nil then
                if err == nil then
                    audit_log.warn('Attempt to authorize with cookies, but user %q is unknown', login)
                else
                    audit_log.warn('Attempt to authorize with cookies for login %q, but some error has occurred', login)
                    log.error(err)
                end
                return { granted = false, fallback = true }
            end

            local _, err = has_access(user, account_types.USER)
            if err ~= nil then
                audit_log.warn(err)
                return { granted = false }
            end

            if user.state == account_states.NEW then
                user.role_id = access_roles.SYSTEM_ROLES.ONE_TIME_ACCESS
            end

            local user_source = user.source
            user_source = user_source or 'TDG'

            account.set_user(user)
            audit_log.info('Access granted to %s user', user_source)
            activity_observer.push()
            return { granted = true }
        end
    end
    return { granted = false, fallback = true }
end

local function authorize(request)
    local allowed, err = authorize_with_external_plugin(request)
    if allowed.granted == true or allowed.fallback ~= true then
        return allowed.granted, err
    end

    allowed = authorize_with_token_from_request(request)
    if allowed.granted == true or allowed.fallback ~= true then
        return allowed.granted
    end

    allowed = authorize_with_cookies(request)
    if allowed.granted == true or allowed.fallback ~= true then
        return allowed.granted
    end

    if is_anonymous_allowed() then
        account.set_anonymous()
        audit_log.info('Access granted to anonymous user')
        return true
    end

    audit_log.warn('Access denied')
    return false
end

-- @monkeypatch
-- We don't need cartridge auth anymore
-- It supports only users but not tokens
cartridge_auth.authorize_request = function() return true end

local function authorize_with_token(token)
    local allowed = authorize_with_token_impl(token)
    if allowed.granted == true or allowed.fallback ~= true then
        return allowed.granted
    end

    if is_anonymous_allowed() then
        account.set_anonymous()
        audit_log.info('Access granted to anonymous user')
        return true
    end

    audit_log.warn('Access denied')
    return false
end

local function check_user_password(login, password)
    checks('string', 'string')
    local ok, err = account_provider.check_password(login, password)

    if err ~= nil then
        return nil, auth_error:new(err)
    end

    if not ok then
        return nil, auth_error:new('Incorrect password')
    end

    return true
end

local function check_permission(aggregate, what)
    -- Check role based access model
    local role_id = account.role_id()
    if role_id == nil then
        if is_anonymous_allowed() then
            return true
        else
            return nil, access_error:new('Anonymous access is denied')
        end
    end

    if role_id == access_roles.SYSTEM_ROLES.ADMIN then
        return true
    elseif role_id == access_roles.SYSTEM_ROLES.SUPERVISOR then
        if what == 'read' then
            return true
        end
    elseif role_id == access_roles.SYSTEM_ROLES.ONE_TIME_ACCESS then
        return nil, access_error:new('Change your password before performing any action')
    end

    local has_access, err = account_provider.check_data_action(role_id, aggregate, what)
    if err ~= nil then
        return nil, err
    end

    if has_access == true then
        return true
    end

    local name = account.name()
    local kind = account.kind()
    return nil, access_error:new("Can't find permissions to %s aggregate %q for %s %q",
        what, aggregate, kind, name)
end

local function is_read_allowed(aggregate)
    return check_permission(aggregate, 'read')
end

local function is_write_allowed(aggregate)
    return check_permission(aggregate, 'write')
end

local function check_role_has_access(action)
    local role_id = account.role_id()
    if role_id == nil then
        if is_anonymous_allowed() then
            return true
        end

        local action_description = access_actions_list.get_description(action) or action
        return nil, access_error:new("Access to %s is prohibited for unknown role", action_description)
    end

    local has_access = account_provider.action_is_allowed_for_role(role_id, action)
    if has_access ~= true then
        if role_id == access_roles.SYSTEM_ROLES.ONE_TIME_ACCESS then
            return nil, access_error:new("Change your password before performing any action")
        end

        local role = account_provider.access_role_get(role_id) or {}
        local role_name = role.name or role_id
        local action_description = access_actions_list.get_description(action) or action
        return nil, access_error:new("Access to '%s' for role %s is prohibited", action_description, role_name)
    end
    return true
end

local function check_default_tenant()
    if tenant_lib.is_default() ~= true then
        return nil, access_error:new("This action is allowes for default tenant members only")
    end
    return true
end

return {
    is_token_name_valid = is_token_name_valid,
    authorize_with_token_name = authorize_with_token_name,
    authorize_with_token = authorize_with_token,
    authorize = authorize,
    check_user_password = check_user_password,
    is_anonymous_allowed = is_anonymous_allowed,
    check_permission = check_permission,
    is_read_allowed = is_read_allowed,
    is_write_allowed = is_write_allowed,
    check_role_has_access = check_role_has_access,
    check_default_tenant = check_default_tenant,
}
