local module_name = 'account_manager.user'

local digest = require('digest')
local clock = require('clock')
local uuid = require('uuid')
local checks = require('checks')
local errors = require('errors')
local cartridge = require('cartridge')
local cartridge_utils = require('cartridge.utils')

local user_error = errors.new_class('account_manager_user_error')
local config_error = errors.new_class('account_manager_user_config_error')

local cartridge_auth = require('cartridge.auth')

local password_digest = require('account_manager.password_digest')
local states = require('account_manager.states')
local account_manager_server = require('account_manager.server')
local account_manager_expiration = require('account_manager.expiration.server')
local password_generator = require('account_manager.password_generator.server')
local account_manager_tenant = require('account_manager.tenant')

local vars = require('common.vars').new(module_name)
local utils = require('common.utils')
local tenant = require('common.tenant')
local account = require('common.admin.account')
local config_checks = require('common.config_checks').new(config_error)

local audit_log = require('audit.log').new(module_name)
local log = require('log.log').new(module_name)
local output_config = require('connector.config.output')
local config_filter = require('common.config_filter')

local NANOSECONDS_IN_SECONDS = 1e9 * 1ULL
local DEFAULT_ONLY_ONE_TIME_PASSWORDS = false
local MAX_UNSUCCESSFUL_ATTEMPTS_MSG = 'Threshold of possible unsuccessful login is exceeded'

vars:new_global('tokens')
vars:new_global('access_roles')
vars:new('output')
vars:new_global('only_one_time_passwords', DEFAULT_ONLY_ONE_TIME_PASSWORDS)
vars:new_global('password_change_timeout_seconds', nil)
vars:new_global('block_after_n_failed_attempts')
vars:new_global('prev_login_time', {})

local format = {
    {name = 'uid', type = 'string', is_nullable = false},
    {name = 'password', type = 'string', is_nullable = false},
    {name = 'email', type = 'string', is_nullable = false},
    {name = 'login', type = 'string', is_nullable = false},
    {name = 'username', type = 'string', is_nullable = false},
    {name = 'created_at', type = 'unsigned', is_nullable = false},
    {name = 'last_login', type = 'unsigned', is_nullable = true},
    {name = 'state', type = 'unsigned', is_nullable = false},
    {name = 'state_reason', type = 'string', is_nullable = true},
    {name = 'is_deleted', type = 'boolean', is_nullable = false},
    {name = 'role_id', type = 'unsigned', is_nullable = false},
    {name = 'expires_in', type = 'unsigned', is_nullable = false},
    {name = 'last_password_update_time', type = 'unsigned', is_nullable = true},
    {name = 'failed_login_attempts', type = 'unsigned', is_nullable = false},
    {name = 'unblocked_at', type = 'unsigned', is_nullable = true},
    {name = 'tenant', type = 'string', is_nullable = true},
}

local PASSWORD_FIELDNO = 2
local EMAIL_FIELDNO = 3
local USERNAME_FIELDNO = 5
local LAST_LOGIN_FIELDNO = 7
local STATE_FIELDNO = 8
local STATE_REASON_FIELDNO = 9
local IS_DELETED_FIELDNO = 10
local ROLE_ID_FIELDNO = 11
local EXPIRES_IN_FIELDNO = 12
local LAST_PASSWORD_UPDATE_TIME_FIELDNO = 13
local FAILED_LOGIN_ATTEMPTS_FIELDNO = 14
local UNBLOCKED_AT_FIELDNO = 15

local space_name = 'tdg_users'

local function get_space()
    local space = box.space[space_name]
    assert(space, 'Space ' .. space_name .. ' is not found')
    return space
end

local function get_by_login_impl(login)
    local space = get_space()
    login = login:strip()
    local user = space.index.login:get({login})
    if user ~= nil and user.login == login then
        return user
    end
    return nil
end

local function get_by_login_ci_impl(login)
    local space = get_space()
    login = login:strip()
    return space.index.login:get({login})
end

local LOWER_SYMBOLS = 'abcdefghijklmnopqrstuvwxyz'
local LOWER_SYMBOLS_LEN = #LOWER_SYMBOLS
local function generate_random_lower_string(len)
    local byte_string = digest.urandom(len)
    local result = ''
    for i = 1, len do
        local num = math.fmod(byte_string:byte(i), LOWER_SYMBOLS_LEN) + 1
        local symbol = LOWER_SYMBOLS:sub(num, num)
        result = result .. symbol
    end
    return result
end

local function generate_login()
    local generated_new
    local login
    repeat
        login = generate_random_lower_string(2) .. ("%04d"):format(math.random(0, 1e4 - 1))
        generated_new = get_by_login_ci_impl(login) == nil and vars.tokens.get_by_name_ci(login) == nil
    until generated_new
    return login
end

local function generate_uid()
    return uuid.str()
end

local function flatten(user)
    return {
        user.uid,
        user.password,
        user.email,
        user.login,
        user.username,
        user.created_at,
        user.last_login,
        user.state,
        user.state_reason,
        user.is_deleted,
        user.role_id,
        user.expires_in,
        user.last_password_update_time,
        user.failed_login_attempts,
        user.unblocked_at,
        user.tenant,
    }
end

local function unflatten(user)
    local result = {}

    if box.tuple.is(user) then
        result = user:tomap({names_only = true})
    else
        for i ,field in ipairs(format) do
            result[field.name] = user[i]
        end
    end

    result.last_login = vars.prev_login_time[result.uid] or result.last_login
    return result
end

local function user_exists(user)
    if user ~= nil and user.is_deleted ~= true then
        return true
    end
    return false
end

local function iterate()
    local space = get_space()

    local tenant_uid
    if not tenant.is_default() then
        tenant_uid = tenant.uid()
    end

    return space.index.tenant:pairs({tenant_uid})
end

local function is_last_user()
    local count = 0
    for _, tuple in iterate() do
        if user_exists(tuple) then
            count = count + 1
        end
        if count > 1 then
            return false
        end
    end
    return true
end

-- Check email correctness and cast to lowercase
local function normalize_email(email)
    if email == nil then
        return nil, user_error:new('Email should be specified')
    end
    email = email:strip()
    email = email:lower()

    local valid, err = utils.is_email_valid(email)
    if not valid then
        return nil, err
    end
    return email
end

local function send_password_to_output(email, password)
    if vars.output == nil then
        return nil, user_error:new('no output configured to send passwords')
    end

    local options = table.deepcopy(vars.output.options or {})
    cartridge_utils.table_setrw(options)
    options.to = email

    return cartridge.rpc_call('connector', 'handle_output', {
        vars.output.name,
        { obj = password, output_options = options },
    }, { leader_only = true })
end

local function create_or_validate_password(password)
    if password == nil then
        return password_generator.generate()
    end

    local res, err = password_generator.validate(password)
    if err ~= nil then
        return nil, user_error:new('Failed to use custom password: %s', err)
    end
    if not res then
        return nil, user_error:new('Failed to use custom password: password is too weak')
    end

    return password
end

local function get_by_email(email)
    local space = get_space()
    for _, user_by_email in space.index.email:pairs({email}) do
        if user_exists(user_by_email) then
            return user_by_email
        end
    end
    return nil
end

local function check_tenant(tenant_uid)
    -- default tenant
    if tenant_uid == nil then
        return box.NULL
    end

    local _, err = account_manager_tenant.get(tenant_uid)
    if err ~= nil then
        return nil, err
    end

    return tenant_uid
end

local function tenant_has_access(user)
    if not tenant.is_default() then
        if user.tenant ~= tenant.uid() then
            return false
        end
    end
    return true
end

local function import(user)
    local space = get_space()

    user.login = user.login:strip()
    if #user.login == 0 then
        return nil, user_error:new('Failed to create user: login is required')
    end

    local existing_login = false

    -- User could lose assess to the system.
    -- It should use "update" instead of reimport.
    if (not account.is_empty()) and account.id() == user.uid then
        return nil, user_error:new('Unable to reimport yourself. Use update instead')
    end

    local user_by_login = get_by_login_ci_impl(user.login)

    -- User could lose assess to the system.
    -- It should use "update" instead of reimport.
    -- Login is also unique identifier so we once again
    -- perform this check.
    if user_by_login ~= nil and (not account.is_empty()) and account.id() == user_by_login.uid then
        return nil, user_error:new('Unable to reimport yourself. Use update instead')
    end

    if user_by_login ~= nil and user.uid ~= user_by_login.uid then
        -- In case user with the same login exists but is deleted,
        -- and there is no user with the same uid
        -- we actually replace user with the same login
        local user_by_uid = space:get({user.uid})
        if user_by_login.is_deleted == false or user_by_uid ~= nil then
            return false, user_error:new("Failed to create user: user with the same login already exists")
        end
        existing_login = true
    end

    if not tenant_has_access(user) then
        return nil, user_error:new('Failed to create user: can not create user with not own tenant')
    end

    local err
    user.email, err = normalize_email(user.email)
    if err ~= nil then
        return nil, user_error:new(err)
    end

    local user_with_email = get_by_email(user.email)
    if user_with_email ~= nil and user_with_email.uid ~= user.uid then
        return nil, user_error:new('Failed to create user: user with the same email already exists')
    end

    if vars.tokens.get_by_name_ci(user.login) ~= nil then
        return nil, user_error:new('Failed to create user: login is used by some token')
    end

    local tenant_uid, err = check_tenant(user.tenant)
    if err ~= nil then
        return nil, err
    end
    user.tenant = tenant_uid

    if vars.access_roles.get(user.role_id, user.tenant) == nil then
        return nil, user_error:new('Failed to create user: role %s does not exist', user.role_id)
    end

    if user.password == nil and user.generate_password ~= true then
        return nil, user_error:new(
                'Failed to create user: password has nil value, but password generation disabled')
    end

    if user.password ~= nil then
        if user.generate_password == true then
            return nil, user_error:new(
                    'Failed to create user: password has not nil value, but password generation enabled')
        end
        if vars.only_one_time_passwords then
            return nil, user_error:new(
                    'Failed to create user: only one time passwords allowed, but custom password present')
        end
    end

    local password, err = create_or_validate_password(user.password)
    if err ~= nil then
        return nil, err
    end

    if user.use_mail then
        local _, err = send_password_to_output(user.email, password)
        if err ~= nil then
            return nil, err
        end
    end

    user.username = user.username:strip()
    user.password = password_digest.get_salted_password(password)
    user.is_deleted = false

    local state, state_reason
    if account_manager_expiration.is_expired(user) then
        state = states.BLOCKED
        state_reason = 'Expired'
    elseif account_manager_expiration.is_inactive(user) then
        state = states.BLOCKED
        state_reason = 'Inactive'
    elseif user.failed_login_attempts ~= nil and
        vars.block_after_n_failed_attempts ~= nil and
        user.failed_login_attempts >= vars.block_after_n_failed_attempts then
        state = states.BLOCKED
        state_reason = MAX_UNSUCCESSFUL_ATTEMPTS_MSG
    end

    if state ~= nil and user.state ~= states.BLOCKED then
        log.warn("User's %q state changed to %q - %s", user.login, states.to_string(state), state_reason)
        user.state = state
        user.state_reason = state_reason
    end

    if existing_login then
        space.index.login:delete({user.login})
    end
    space:replace(flatten(user))

    audit_log.info('User %s created', user.uid)
    account_manager_server.notify_subscribers('users', user.login)

    user.password = password
    return user
end

local function create(user)
    return import({
        uid = generate_uid(),
        password = user.password,
        use_mail = user.password == nil,
        generate_password = user.password == nil,
        email = user.email,
        login = user.login ~= nil and user.login or generate_login(),
        username = user.username ~= nil and user.username or '',
        created_at = clock.time64(),
        last_login = box.NULL,
        state = user.password ~= nil and states.ACTIVE or states.NEW,
        state_reason = box.NULL,
        role_id = user.role_id ~= nil and user.role_id or vars.access_roles.SYSTEM_ROLES.ADMIN,
        expires_in = user.expires_in ~= nil and user.expires_in or 0,
        last_password_update_time = box.NULL,
        failed_login_attempts = 0,
        unblocked_at = box.NULL,
        tenant = user.tenant,
    })
end

local function set_state_impl(user, new_state, reason)
    local uid = user.uid

    if not tenant_has_access(user) then
        return nil, user_error:new('Can not update state of user with not own tenant')
    end

    if user.state == new_state then
        return nil, user_error:new("User %s is already in '%s' state", uid, states.to_string(new_state))
    end

    local message = ('%s state is changed to %s'):format(user.email, states.to_string(new_state))
    if reason ~= nil then
        message = message .. ': ' .. tostring(reason)
    end

    local space = get_space()
    local update_list = {
        {'=', STATE_FIELDNO, new_state},
        {'=', STATE_REASON_FIELDNO, message},
    }
    if user.state == states.BLOCKED then
        table.insert(update_list, {'=', UNBLOCKED_AT_FIELDNO, clock.time64()})
    end
    user = space:update({uid}, update_list)

    account_manager_server.notify_subscribers('users', user.login)
    audit_log.info(message)
    return user
end

local function get(uid)
    local space = get_space()

    local user = space:get({uid})
    if not user_exists(user) then
        return nil
    end

    return unflatten(user)
end

local function get_by_login(login)
    local tuple = get_by_login_impl(login)
    if not user_exists(tuple) then
        return nil
    end

    return unflatten(tuple)
end

local function get_by_login_ci(login)
    local tuple = get_by_login_ci_impl(login)
    if not user_exists(tuple) then
        return nil
    end

    return unflatten(tuple)
end

local function update_activity(users)
    local space = get_space()

    for uid, time in pairs(users) do
        local user = space:get({uid})
        if user_exists(user) then
            space:update({uid}, {{'=', LAST_LOGIN_FIELDNO, time}})
        end
    end
end

local function list()
    return iterate():filter(user_exists):map(unflatten):totable()
end

local function delete(uid)
    local space = get_space()
    local user = space:get({uid})
    if not user_exists(user) then
        return nil, user_error:new("Unknown user '%s'", uid)
    end

    if cartridge_auth.get_enabled() and is_last_user() then
        return nil, user_error:new('Failed to delete last user, because anonymous access is denied')
    end

    if (not account.is_empty()) and account.id() == user.uid then
        return nil, user_error:new("Unable to delete yourself")
    end

    if not tenant_has_access(user) then
        return nil, user_error:new('Failed to delete user: can not delete user with not own tenant')
    end

    space:update({uid}, {{'=', IS_DELETED_FIELDNO, true}})
    audit_log.info('User %s deleted', user.uid)
    account_manager_server.notify_subscribers('users', user.login)
    return unflatten(user)
end

local function password_update_is_allowed(user)
    if vars.password_change_timeout_seconds == nil then
        return true
    end

    if user.uid ~= account.id() then
       return true
    end

    if user.last_password_update_time == nil then
        return true
    end

    local time = clock.time64()
    local time_since_last_modification_sec = (time - user.last_password_update_time) / NANOSECONDS_IN_SECONDS
    return time_since_last_modification_sec > vars.password_change_timeout_seconds
end

local function update(uid, updates)
    local space = get_space()
    local user = space:get({uid})
    if not user_exists(user) then
        return nil, user_error:new("Unknown user '%s'", uid)
    end

    if not tenant_has_access(user) then
        return nil, user_error:new('Failed to update user: can not update user with not own tenant')
    end

    local update_list = {}
    local update_list_str = {}

    local email = updates.email
    if email ~= nil and email ~= user.email then
        local user_with_email = get_by_email(email)
        if user_with_email ~= nil and user_with_email.uid ~= user.uid then
            return nil, user_error:new('Failed to update user: user with the same email already exists')
        end

        local email, err = normalize_email(email)
        if err ~= nil then
            return nil, user_error:new(err)
        end
        table.insert(update_list_str, ('email: %s -> %s'):format(user.email, email))
        table.insert(update_list, {'=', EMAIL_FIELDNO, email})
    end

    local password = updates.password
    if password ~= nil then
        if not password_update_is_allowed(user) then
            return nil, user_error:new('Unable to change password too often')
        end

        local _, err = password_generator.validate(password)
        if err ~= nil then
            return nil, err
        end

        local time = clock.time64()
        table.insert(update_list, {'=', PASSWORD_FIELDNO, password_digest.get_salted_password(password)})
        table.insert(update_list_str, ('password at %s'):format(time))

        if user.state ~= states.ACTIVE then
            table.insert(update_list, {'=', STATE_FIELDNO, states.ACTIVE})
            local reason = 'Change to active after password change'
            table.insert(update_list_str,
                ('state: %s -> %s (%s)'):format(states.to_string(user.state), states.to_string(states.ACTIVE), reason))
            table.insert(update_list, {'=', STATE_REASON_FIELDNO, reason})
        end
        table.insert(update_list, {'=', LAST_PASSWORD_UPDATE_TIME_FIELDNO, time})
    end

    local role_id = updates.role_id
    if role_id ~= nil and role_id ~= user.role_id then
        if vars.access_roles.get(role_id, user.tenant) == nil then
            return nil, user_error:new('Failed to update user: role %s does not exist', role_id)
        end
        table.insert(update_list_str, ('role: %s -> %s'):format(user.role_id, role_id))
        table.insert(update_list, {'=', ROLE_ID_FIELDNO, role_id})
    end

    local username = updates.username
    if username ~= nil and username ~= user.username then
        username = username:strip()
        table.insert(update_list_str, ('username: %s -> %s'):format(user.username, username))
        table.insert(update_list, {'=', USERNAME_FIELDNO, username})
    end

    local expires_in = updates.expires_in
    if expires_in ~= nil and expires_in ~= user.expires_in then
        table.insert(update_list_str, ('expires_in: %s -> %s'):format(user.expires_in, expires_in))
        table.insert(update_list, {'=', EXPIRES_IN_FIELDNO, expires_in})
    end

    if #update_list == 0 then
        return user
    end

    user = space:update({uid}, update_list)
    audit_log.info('User %s updated\n%s', user.uid, table.concat(update_list_str, '\n'))
    account_manager_server.notify_subscribers('users', user.login)
    return unflatten(user)
end

local function is_expired(user)
    local expires_in = user.expires_in
    if expires_in == 0 then
        return false
    end
    if (clock.time64() - user.created_at > expires_in * NANOSECONDS_IN_SECONDS) then
        return true
    end
    return false
end

local function set_state(uid, new_state, reason)
    local space = get_space()
    local user = space:get({uid})

    if not user_exists(user) then
        return nil, user_error:new("Unknown user '%s'", uid)
    end

    if (not account.is_empty()) and account.id() == user.uid then
        return nil, user_error:new("Unable to change own state")
    end

    if is_expired(user) then
        return nil, user_error:new("Unable to change the state of expired user. Modify expiration before")
    end

    if new_state == states.NEW then
        return nil, user_error:new('Unable set system "NEW" state')
    end

    local tuple, err = set_state_impl(user, new_state, reason)
    if err ~= nil then
        return nil, err
    end
    return unflatten(tuple)
end

local function reset_failed_login_attempts(uid)
    local space = get_space()
    local user = space:get({uid})
    vars.prev_login_time[uid] = user.last_login
    space:update({uid}, {{'=', FAILED_LOGIN_ATTEMPTS_FIELDNO, 0}, {'=', LAST_LOGIN_FIELDNO, clock.time64()}})
end

local function inc_failed_login_attempts(uid)
    local space = get_space()
    local tuple = space:update({uid}, {{'+', FAILED_LOGIN_ATTEMPTS_FIELDNO, 1}})
    if vars.block_after_n_failed_attempts ~= nil and
        tuple.failed_login_attempts >= vars.block_after_n_failed_attempts then
        local _, err = set_state(uid, states.BLOCKED, MAX_UNSUCCESSFUL_ATTEMPTS_MSG)
        if err ~= nil then
            log.error('Failed to automatically ban user (%s): %s', MAX_UNSUCCESSFUL_ATTEMPTS_MSG, err)
        end
    end
end

local function reset_password(uid)
    local space = get_space()
    local user = space:get({ uid })
    if not user_exists(user) then
        return nil, user_error:new("Unknown user '%s'", uid)
    end

    local password, err = password_generator.generate()
    if err ~= nil then
        return nil, err
    end

    local _, err = send_password_to_output(user.email, password)
    if err ~= nil then
        return nil, err
    end

    local salted_password = password_digest.get_salted_password(password)


    local reason = 'Password was reset'
    if not account.is_empty() and account.name() ~= nil then
        reason = reason .. ' by ' .. account.name()
    end

    local update_list = {
        {'=', PASSWORD_FIELDNO, salted_password},
        {'=', LAST_PASSWORD_UPDATE_TIME_FIELDNO, box.NULL},
        {'=', STATE_FIELDNO, states.NEW},
        {'=', STATE_REASON_FIELDNO, reason},
    }
    if user.state == states.BLOCKED then
        table.insert(update_list, {'=', UNBLOCKED_AT_FIELDNO, clock.time64()})
    end
    space:update({uid}, update_list)

    account_manager_server.notify_subscribers('users', user.login)
    return password
end

local function apply_config()
    vars.tokens = require('account_manager.token')
    vars.access_roles = require('account_manager.access_role')
    if box.info.ro then
        return
    end

    local space = box.space[space_name]
    if space ~= nil then
        local ok, err = pcall(space.format, space, format)
        user_error:assert(ok, "Impossible to format a users space: %s", err)
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

    space:create_index('email', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {{field = 'email', type = 'string'}},
    })

    space:create_index('login', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'login', type = 'string', collation='unicode_ci'}},
    })

    space:create_index('tenant', {
        type = 'TREE',
        unique = false,
        if_not_exists = true,
        parts = {{field = 'tenant', type = 'string', is_nullable = true}},
    })
    box.commit()
end

local function tenant_validate_config(cfg)
    checks('table')

    local conf = config_filter.compare_and_get(cfg, 'account_manager', module_name)
    if conf == nil then
        return
    end

    if conf.output ~= nil then
        config_checks:check_luatype('account_manager.output', conf.output, 'table')

        config_checks:check_luatype('account_manager.output.name', conf.output.name, 'string')
        config_checks:assert(output_config.output_exists(cfg, conf.output.name),
                "Output '%s' does not exist into connector.output", conf.output.name)

        config_checks:check_optional_luatype('account_manager.output.options', conf.output.options, 'table')
    end

    config_checks:check_optional_luatype('account_manager.only_one_time_passwords',
            conf.only_one_time_passwords, 'boolean')

    config_checks:assert(not conf.only_one_time_passwords or conf.output ~= nil,
            'Impossible to enable only one-time passwords without output section')

    config_checks:check_optional_luatype('account_manager.password_change_timeout_seconds',
            conf.password_change_timeout_seconds, 'number')
    if conf.password_change_timeout_seconds ~= nil then
        config_checks:assert(conf.password_change_timeout_seconds > 0,
            'password_change_timeout_seconds should be greater zero')
    end

    config_checks:check_optional_luatype('account_manager.block_after_n_failed_attempts',
            conf.block_after_n_failed_attempts, 'number')
    if conf.block_after_n_failed_attempts ~= nil then
        config_checks:assert(conf.block_after_n_failed_attempts > 0,
            'account_manager.block_after_n_failed_attempts should be greater than zero')
    end

    return true
end

local function tenant_apply_config(cfg)
    checks('table')

    local conf, err = config_filter.compare_and_set(cfg, 'account_manager', module_name)
    if err ~= nil then
        return true
    end
    conf = conf or {}

    vars.output = conf.output
    vars.only_one_time_passwords = conf.only_one_time_passwords or DEFAULT_ONLY_ONE_TIME_PASSWORDS
    vars.password_change_timeout_seconds = conf.password_change_timeout_seconds
    vars.block_after_n_failed_attempts = conf.block_after_n_failed_attempts
end

return {
    apply_config = apply_config,
    tenant_validate_config = tenant_validate_config,
    tenant_apply_config = tenant_apply_config,
    iterate = iterate,

    create = create,
    import = import,
    update = update,
    delete = delete,
    set_state = set_state,
    set_state_impl = set_state_impl,
    reset_password = reset_password,
    get_by_login = get_by_login,
    get_by_login_ci = get_by_login_ci,
    get = get,
    list = list,
    update_activity = update_activity,

    reset_failed_login_attempts = reset_failed_login_attempts,
    inc_failed_login_attempts = inc_failed_login_attempts,

    -- for tests
    generate_login = generate_login,
}
