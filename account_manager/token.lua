local module_name = 'account_manager.token'

local uuid = require('uuid')
local clock = require('clock')
local errors = require('errors')

local log = require('log.log').new(module_name)
local audit_log = require('audit.log').new(module_name)
local vars = require('common.vars').new(module_name)
local tenant = require('common.tenant')
local password_digest = require('account_manager.password_digest')
local states = require('account_manager.states')
local account_manager_server = require('account_manager.server')
local account = require('common.admin.account')
local account_manager_expiration = require('account_manager.expiration.server')
local account_manager_tenant = require('account_manager.tenant')

local token_error = errors.new_class('account_manager_token_error')

vars:new_global('users')
vars:new_global('access_roles')

local format = {
    {name = 'uid', type = 'string', is_nullable = false},
    {name = 'name', type = 'string', is_nullable = false},
    {name = 'created_at', type = 'unsigned', is_nullable = false},
    {name = 'last_login', type = 'unsigned', is_nullable = true},
    {name = 'state', type = 'unsigned', is_nullable = false},
    {name = 'state_reason', type = 'string', is_nullable = true},
    {name = 'is_deleted', type = 'boolean', is_nullable = false},
    {name = 'role_id', type = 'unsigned', is_nullable = false},
    {name = 'expires_in', type = 'unsigned', is_nullable = false},
    {name = 'unblocked_at', type = 'unsigned', is_nullable = true},
    {name = 'tenant', type = 'string', is_nullable = true},
}

local space_name = 'tdg_tokens'
local LAST_LOGIN_FIELDNO = 4
local STATE_FIELDNO = 5
local STATE_REASON_FIELDNO = 6
local IS_DELETED_FIELDNO = 7
local ROLE_ID_FIELDNO = 8
local EXPIRES_IN_FIELDNO = 9
local UNBLOCKED_AT_FIELDNO = 10

local function get_space()
    local space = box.space[space_name]
    assert(space, 'Space ' .. space_name .. ' is not found')
    return space
end

local function flatten(token)
    return {
        token.uid,
        token.name,
        token.created_at,
        token.last_login,
        token.state,
        token.state_reason,
        token.is_deleted,
        token.role_id,
        token.expires_in,
        token.unblocked_at,
        token.tenant,
    }
end

local function unflatten(token)
    if type(token) == 'cdata' then
        return token:tomap({names_only = true})
    end

    local result = {}
    for i ,field in ipairs(format) do
        result[field.name] = token[i]
    end
    return result
end

local function token_exists(token)
    if token == nil or token.is_deleted == true then
        return false
    end
    return true
end

local function get_by_name_impl(name)
    local space = get_space()
    name = name:strip()
    local token = space.index.name:get({name})
    if token ~= nil and token.name == name then
        return token
    end
    return nil
end

local function get_by_name_ci_impl(name)
    local space = get_space()
    name = name:strip()
    return space.index.name:get({name})
end

local function check_tenant(tenant)
    -- default tenant
    if tenant == nil then
        return box.NULL
    end

    local _, err = account_manager_tenant.get(tenant)
    if err ~= nil then
        return nil, err
    end

    return tenant
end

local function tenant_has_access(token)
    if not tenant.is_default() then
        if token.tenant ~= tenant.uid() then
            return false
        end
    end
    return true
end

local function import(token)
    local space = get_space()

    token.name = token.name:strip()
    if #token.name == 0 then
        return nil, token_error:new('Failed to create token: name is required')
    end

    token.uid = token.uid:strip()
    if #token.uid == 0 then
        return nil, token_error:new('Failed to create token: uid is required')
    end

    if not tenant_has_access(token) then
        return nil, token_error:new('Failed to create token: can not create token with not own tenant')
    end

    -- Token could lose assess to the system.
    -- It should use "update" instead of reimport.
    if (not account.is_empty()) and account.id() == token.uid then
        return nil, token_error:new('Unable to reimport yourself. Use update instead')
    end

    local token_by_uid = space:get({token.uid})
    if token_by_uid ~= nil then
        if utf8.casecmp(token.name, token_by_uid.name) ~= 0 then
            return nil, token_error:new("Failed to create token: token with the same uid and another name exists")
        end
    else
        local token_by_name = get_by_name_ci_impl(token.name)
        if token_by_name ~= nil then
            if not token_exists(token_by_name) then
                return nil, token_error:new(
                    "Failed to create token: a token with same name was deleted, reuse of this name is prohibited")
            end
            return nil, token_error:new("Failed to create token: token with the same name already exists")
        end
    end

    if vars.users.get_by_login_ci(token.name) ~= nil then
        return nil, token_error:new('Failed to create token: name is used by some user')
    end

    local tenant_uid, err = check_tenant(token.tenant)
    if err ~= nil then
        return nil, err
    end
    token.tenant = tenant_uid

    if vars.access_roles.get(token.role_id, token.tenant) == nil then
        return nil, token_error:new('Failed to create token: role %s does not exist', token.role_id)
    end

    if token.state == states.NEW then
        return nil, token_error:new('Failed to create token: unable to use "NEW" state for tokens')
    end

    token.is_deleted = false

    local state, state_reason
    if account_manager_expiration.is_expired(token) then
        state = states.BLOCKED
        state_reason = 'Expired'
    elseif account_manager_expiration.is_inactive(token) then
        state = states.BLOCKED
        state_reason = 'Inactive'
    end

    if state ~= nil and token.state ~= states.BLOCKED then
        log.warn("Token's %q state changed to %q - %s", token.name, states.to_string(state), state_reason)
        token.state = state
        token.state_reason = state_reason
    end

    space:replace(flatten(token))
    audit_log.info('Token %s created', token.uid)
    account_manager_server.notify_subscribers('tokens', token.uid)

    return {
        uid = token.uid,
        name = token.name,
        created_at = token.created_at,
        role_id = token.role_id,
        expires_in = token.expires_in,
        state = token.state,
        state_reason = token.state_reason,
        tenant = token.tenant,
    }
end

local function create(token)
    local role_id = token.role_id
    role_id = role_id ~= nil and role_id or vars.access_roles.SYSTEM_ROLES.ADMIN

    local expires_in = token.expires_in
    expires_in = expires_in ~= nil and expires_in or 0

    local visible_token = uuid.str()
    local uid = password_digest.password_digest(visible_token)

    local result, err = import({
        uid = uid,
        name = token.name,
        created_at = clock.time64(),
        last_login = box.NULL,
        state = states.ACTIVE,
        state_reason = box.NULL,
        role_id = role_id,
        expires_in = expires_in,
        unblocked_at = box.NULL,
        tenant = token.tenant,
    })
    if err ~= nil then
        return nil, err
    end

    result.token = visible_token
    return result
end

local function set_state_impl(token, new_state, reason)
    if token.state == new_state then
        return nil, token_error:new("Token %s is already in '%s' state", token.name, states.to_string(new_state))
    end

    if not tenant_has_access(token) then
        return nil, token_error:new('Can not update state of token with not own tenant')
    end

    local space = get_space()
    local message = ('%s state is changed to %s'):format(token.name, states.to_string(new_state))
    if reason ~= nil then
        message = message .. ': ' .. tostring(reason)
    end

    local update_list = {
        {'=', STATE_FIELDNO, new_state},
        {'=', STATE_REASON_FIELDNO, message},
    }
    if token.state == states.BLOCKED then
        table.insert(update_list, {'=', UNBLOCKED_AT_FIELDNO, clock.time64()})
    end
    token = space:update({token.uid}, update_list)
    account_manager_server.notify_subscribers('tokens', token.uid)
    audit_log.info(message)
    return token
end

local function get(uid)
    local space = get_space()
    local token = space:get({uid})
    if not token_exists(token) then
        return nil
    end

    return unflatten(token)
end

local function get_by_name(name)
    local token = get_by_name_impl(name)
    if not token_exists(token) then
        return nil
    end

    return unflatten(token)
end

local function get_by_name_ci(name)
    local token = get_by_name_ci_impl(name)
    if not token_exists(token) then
        return nil
    end

    return unflatten(token)
end

local function update(name, updates)
    local space = get_space()

    local token = get_by_name_impl(name)
    if not token_exists(token) then
        return nil, token_error:new("Unknown token '%s'", name)
    end

    if not tenant_has_access(token) then
        return nil, token_error:new('Failed to update token: can not update token with not own tenant')
    end

    local update_list = {}
    local update_list_str = {}

    local role_id = updates.role_id
    if role_id ~= nil and role_id ~= token.role_id then
        if vars.access_roles.get(role_id, token.tenant) == nil then
            return nil, token_error:new('Failed to update token: role %s does not exist', role_id)
        end
        table.insert(update_list_str, ('role: %s -> %s'):format(token.role_id, role_id))
        table.insert(update_list, { '=', ROLE_ID_FIELDNO, role_id })
    end

    local expires_in = updates.expires_in
    if expires_in ~= nil and expires_in ~= token.expires_in then
        table.insert(update_list_str, ('expires_in: %s -> %s'):format(token.expires_in, expires_in))
        table.insert(update_list, { '=', EXPIRES_IN_FIELDNO, expires_in })
    end

    if #update_list == 0 then
        return token
    end

    token = space:update({ token.uid }, update_list)
    audit_log.info('Token %s updated\n%s', token.uid, table.concat(update_list_str, '\n'))
    account_manager_server.notify_subscribers('tokens', token.uid)
    return unflatten(token)
end

local function remove(name)
    local space = get_space()

    local token = get_by_name_impl(name)
    if not token_exists(token) then
        return nil, token_error:new("Token %s is not found", name)
    end

    if (not account.is_empty()) and account.id() == token.uid then
        return nil, token_error:new("Unable to delete yourself")
    end

    if not tenant_has_access(token) then
        return nil, token_error:new('Failed to delete token: can not delete token with not own tenant')
    end

    space:update({token.uid}, {{'=', IS_DELETED_FIELDNO, true}})
    audit_log.info('Token %s removed', token.uid)
    account_manager_server.notify_subscribers('tokens', token.uid)
    return unflatten(token)
end

local function iterate()
    local space = get_space()

    local tenant_uid
    if not tenant.is_default() then
        tenant_uid = tenant.uid()
    end

    return space.index.tenant:pairs({tenant_uid})
end

local function list()
    return iterate():filter(token_exists):map(unflatten):totable()
end

local function update_activity(tokens)
    local space = get_space()

    for uid, time in pairs(tokens) do
        local token = space:get({uid})
        if token_exists(token) then
            space:update({uid}, {{'=', LAST_LOGIN_FIELDNO, time}})
        end
    end
end

local NANOSECONDS_IN_SECONDS = 1e9 * 1ULL
local function is_expired(token)
    local expires_in = token.expires_in
    if expires_in == 0 then
        return false
    end
    if (clock.time64() - token.created_at > expires_in * NANOSECONDS_IN_SECONDS) then
        return true
    end
    return false
end

local function set_state(name, new_state, reason)
    local token = get_by_name_impl(name)
    if token == nil then
        return nil, token_error:new('Token "%s" is not found', name)
    end

    if (not account.is_empty()) and account.id() == token.uid then
        return nil, token_error:new("Unable to change own state")
    end

    if is_expired(token) then
        return nil, token_error:new("Unable to change the state of expired token")
    end

    if new_state == states.NEW then
        return nil, token_error:new('Unable set system "NEW" state')
    end

    local tuple, err = set_state_impl(token, new_state, reason)
    if err ~= nil then
        return nil, err
    end
    return unflatten(tuple)
end

local function init()
    vars.users = require('account_manager.user')
    vars.access_roles = require('account_manager.access_role')
    if box.info.ro then
        return
    end

    local space = box.space[space_name]
    if space ~= nil then
        local ok, err = pcall(space.format, space, format)
        token_error:assert(ok, "Impossible to format a tokens space: %s", err)
        return
    end

    box.begin()
    space = box.schema.space.create(space_name, {
        format = format,
        if_not_exists = true,
    })

    space:create_index('uid', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'uid', type = 'string'}},
    })

    space:create_index('name', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'name', type = 'string', collation="unicode_ci"}},
    })

    space:create_index('tenant', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {{field = 'tenant', type = 'string', is_nullable = true}},
    })
    box.commit()
end

local function apply_config()
    init()
end

return {
    apply_config = apply_config,
    iterate = iterate,

    create = create,
    import = import,
    get = get,
    set_state = set_state,
    set_state_impl = set_state_impl,
    get_by_name = get_by_name,
    get_by_name_ci = get_by_name_ci,
    update = update,
    remove = remove,
    list = list,
    update_activity = update_activity,
}
