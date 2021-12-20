local module_name = 'account_provider.ldap'

local ldap = require('ldap')
local utils = require('common.utils')
local errors = require('errors')
local cartridge = require('cartridge')
local vars = require('common.vars').new(module_name)
local SYSTEM_ROLES = require('account_manager.access_role').SYSTEM_ROLES

local audit_log = require('audit.log').new(module_name)
local config_filter = require('common.config_filter')

local ldap_error = errors.new_class('ldap_error')
local ldap_config_error = errors.new_class('ldap_config_error')
local config_checks = require('common.config_checks').new(ldap_config_error)

vars:new_global('users', {})
vars:new_global('DEFAULT_SEARCH_TIMEOUT', 2)

local function get_connection(host, username, password, use_tls)
    local ld, err = ldap.open(host, username, password, use_tls)
    if err ~= nil then
        return nil, ldap_error:new(err)
    end
    return ld
end

-- https://stackoverflow.com/questions/18756688/what-are-cn-ou-dc-in-an-ldap-search
local function parse_username(username)
    local user_domain = username:split('@', 1)

    -- Username contains one or more than two "@" - it's not ldap username
    if #user_domain ~= 2 then
        return nil
    end

    local user = user_domain[1]
    local domain = user_domain[2]
    return {
        cn = user,
        domain = domain,
    }
end

local function ldap_is_enabled()
    return cartridge.config_get_readonly('ldap') ~= nil
end

local function format_base(cn, ous, dc)
    local username_parts = {}

    if cn ~= nil then
        table.insert(username_parts, string.format('cn=%s', cn))
    end

    if ous ~= nil then
        for _, ou in ipairs(ous) do
            table.insert(username_parts, string.format('ou=%s', ou))
        end
    end

    if dc ~= nil then
        table.insert(username_parts, dc)
    end

    return table.concat(username_parts, ',')
end

local function format_username(cn, ous, dc)
    return format_base(cn, ous, dc)
end

local function authorize(username, password)
    local ldap_cfg = cartridge.config_get_readonly('ldap')
    if ldap_cfg == nil then
        return nil, ldap_error:new('ldap is not configured')
    end

    local parsed = parse_username(username)
    if parsed == nil then
        return nil
    end

    local domain = parsed.domain
    local domain_parts = domain:split('.')
    for i, domain_part in ipairs(domain_parts) do
        domain_parts[i] = 'dc=' .. domain_part
    end
    local dc = table.concat(domain_parts, ',')
    local cn = parsed.cn

    local section
    local ld, err
    for _, s in ipairs(ldap_cfg) do
        if s.domain == domain then
            section = s
            local user
            if s.use_active_directory then
                -- username == UPN == mail
                user = username
            else
                local ous = s.organizational_units
                user = format_username(cn, ous, dc)
            end
            for _, host in ipairs(s.hosts) do
                ld, err = get_connection(host, user, password, s.use_tls)
                if err ~= nil then
                    audit_log.verbose('Attempt to authorize user %q via ldap failed: %s', user, err)
                elseif ld ~= nil then
                    break
                end
            end

            if ld ~= nil then
                break
            end
        end
        section = nil
    end

    if section == nil then
        return nil, ldap_error:new('domain %q is not specified in config', domain)
    end

    if ld == nil then
        return nil, ldap_error:new('Could not connect to ldap server: %s', err)
    end

    local base = format_base(nil, section.organizational_units, dc)
    local timeout = section.search_timeout
    if timeout == nil then
        timeout = vars.DEFAULT_SEARCH_TIMEOUT
    end

    local filter
    if section.use_active_directory then
        filter = string.format('(userprincipalname=%s)', username)
    else
        filter = string.format('(cn=%s)', cn)
    end

    local iter, err = ldap.search(ld, {
        base = base,
        scope = 'subtree',
        sizelimit = 10,
        filter = filter,
        timeout = timeout,
    })

    if err ~= nil then
        ldap.close(ld)
        audit_log.warn('ldap.search error for user %q: %s',
            format_username(cn, section.organizational_units, dc), err)
        return nil, err
    end

    local user_record
    for item in iter do
        if item ~= nil and item.attrs ~= nil then
            -- for Active Directory, compare with userPrincipalName (~ email)
            if section.use_active_directory and item.attrs.userPrincipalName == username or item.attrs.cn == cn then
                user_record = item
                break
            end
        end
    end
    ldap.close(ld)

    if user_record == nil then
        return nil, ldap_error:new('User %q is not found', cn)
    end

    local roles = {}
    for _, role_options in ipairs(section.roles) do
        for _, domain_group in ipairs(role_options.domain_groups) do
            roles[domain_group] = role_options.role
        end
    end

    local memberOf = user_record.attrs.memberOf
    if type(memberOf) ~= 'table' then
        memberOf = {memberOf}
    end

    local role
    for _, member in ipairs(memberOf) do
        role = roles[member]
        if role ~= nil then
            break
        end
    end

    if role == nil then
        return nil, ldap_error:new('User %q does not match any role',
            format_username(cn, section.organizational_units, dc))
    end

    local role_id
    if role == 'admin' then
        role_id = SYSTEM_ROLES.ADMIN
    elseif role == 'supervisor' then
        role_id = SYSTEM_ROLES.SUPERVISOR
    elseif role == 'user' then
        role_id = SYSTEM_ROLES.USER
    else
        local account_provider = require('account_provider.account_provider')
        local access_role, err = account_provider.access_role_get_by_name(role)
        if err ~= nil then
            return nil, ldap_error:new(err)
        end
        role_id = access_role.id
    end

    vars.users[username] = {
        id = user_record.dn,
        login = username,
        username = username,
        fullname = user_record.attrs.givenName,
        email = user_record.attrs.mail,
        role_id = role_id,
        source = 'LDAP',
    }

    return true
end

local function get_user(username)
    if vars.users[username] ~= nil then
        return vars.users[username]
    end
    return nil
end

local sformat = string.format
local function validate_config(cfg)
    if cfg == nil or cfg.ldap == nil then
        return true
    end

    local conf = config_filter.compare_and_get(cfg, 'ldap', module_name)
    if conf == nil then
        return true
    end

    config_checks:check_luatype('ldap', cfg.ldap, 'table')
    config_checks:assert(utils.is_array(cfg.ldap) == true, 'roles expected to be an array')
    config_checks:assert(#cfg.ldap > 0, 'ldap.roles expected to be an array with at least one element')

    for num, ldap_cfg in ipairs(cfg.ldap) do
        local prefix = sformat('ldap[%d]', num)
        config_checks:check_table_keys(prefix, ldap_cfg,
            {'domain', 'organizational_units', 'hosts', 'use_tls', 'search_timeout', 'roles', 'use_active_directory'})

        prefix = prefix .. '.'
        config_checks:check_luatype(prefix .. 'domain', ldap_cfg.domain, 'string')
        if ldap_cfg.organizational_units ~= nil then
            local organizational_units_prefix = prefix .. 'organizational_units'
            config_checks:check_luatype(organizational_units_prefix, ldap_cfg.organizational_units, 'table')
            for i, ou in ipairs(ldap_cfg.organizational_units) do
                config_checks:check_luatype(sformat('%s[%d]', organizational_units_prefix, i), ou, 'string')
            end
        end
        config_checks:check_luatype(prefix .. 'hosts', ldap_cfg.hosts, 'table')
        config_checks:assert(utils.is_array(ldap_cfg.hosts) == true, prefix .. 'hosts expected to be an array')
        config_checks:assert(#ldap_cfg.hosts > 0, prefix .. 'hosts expected to be an array with at least one element')
        for i, host in ipairs(ldap_cfg.hosts) do
            config_checks:check_luatype(sformat('%shosts[%d]', prefix, i), host, 'string')
        end

        config_checks:check_optional_luatype(prefix .. 'use_tls', ldap_cfg.use_tls, 'boolean')
        config_checks:check_optional_luatype(prefix .. 'search_timeout', ldap_cfg.search_timeout, 'number')
        config_checks:check_luatype(prefix .. 'roles', ldap_cfg.roles, 'table')
        config_checks:assert(utils.is_array(ldap_cfg.roles) == true, '%sroles expected to be an array', prefix)
        config_checks:assert(#ldap_cfg.roles > 0, '%sroles expected to be an array with at least one element', prefix)

        for i, role_data in ipairs(ldap_cfg.roles) do
            local prefix = sformat('%sroles[%d].', prefix, i)
            config_checks:check_luatype(sformat('%srole', prefix),
                role_data.role, 'string')
            config_checks:check_luatype(sformat('%sdomain_groups', prefix),
                role_data.domain_groups, 'table')
            config_checks:assert(utils.is_array(role_data.domain_groups) == true,
                '%sdomain_groups expected to be an array', prefix)
            config_checks:assert(#role_data.domain_groups > 0,
                '%sdomain_groups expected to be an array with at least one element', prefix)
            for j, domain_group in ipairs(role_data.domain_groups) do
                config_checks:check_luatype(sformat('%sdomain_groups[%d]', prefix, j),
                    domain_group, 'string')
            end
        end

        config_checks:check_optional_luatype(prefix .. 'use_active_directory', ldap_cfg.use_active_directory, 'boolean')
    end
    return true
end

local function apply_config(cfg)
    local _, err = config_filter.compare_and_set(cfg, 'ldap', module_name)
    if err ~= nil then
        return true
    end
    vars.users = {}
end

return {
    ldap_is_enabled = ldap_is_enabled,
    authorize = authorize,
    get_user = get_user,

    apply_config = apply_config,
    validate_config = validate_config,
}
