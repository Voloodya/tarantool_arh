local graphql = require('common.graphql')
local types = require('graphql.types')
local cartridge = require('cartridge')

local ldap_role_config_schema = types.object({
    name = 'LdapRoleConfigSchema',
    fields = {
        role = types.string.nonNull,
        domain_groups = types.list(types.string.nonNull).nonNull,
    },
    description = 'LDAP role configuration',
    schema = 'admin',
})

local ldap_config_schema = types.object({
    name = 'LdapConfigSchema',
    fields = {
        domain = types.string.nonNull,
        organizational_units = types.list(types.string),
        hosts = types.list(types.string.nonNull).nonNull,
        use_tls = types.boolean,
        search_timeout = types.float,
        roles = types.list(ldap_role_config_schema.nonNull).nonNull,
        use_active_directory = types.boolean,
    },
    description = 'LDAP configuration',
    schema = 'admin',
})

local input_ldap_role_config_schema = types.inputObject({
    name = 'InputLdapRoleConfigSchema',
    fields = {
        role = types.string.nonNull,
        domain_groups = types.list(types.string.nonNull).nonNull,
    },
    description = 'LDAP role configuration input',
    schema = 'admin',
})

local input_ldap_config_schema = types.inputObject({
    name = 'InputLdapConfigSchema',
    fields = {
        domain = types.string.nonNull,
        organizational_units = types.list(types.string),
        hosts = types.list(types.string.nonNull).nonNull,
        use_tls = types.boolean,
        search_timeout = types.float,
        roles = types.list(input_ldap_role_config_schema).nonNull,
        use_active_directory = types.boolean,
    },
    description = 'LDAP configuration input',
    schema = 'admin',
})

local function read_ldap_config()
    local ldap = cartridge.config_get_deepcopy('ldap')
    if ldap == nil then
        ldap = {}
    end

    return ldap
end

local function write_ldap_config(_, args)
    local _, err = cartridge.config_patch_clusterwide({ldap = args.config})
    if err ~= nil then
        return nil, err
    end

    return read_ldap_config()
end

local function init()
    graphql.add_mutation_prefix('admin', 'account_provider', 'Access management')
    graphql.add_callback_prefix('admin', 'account_provider', 'Access management')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'account_provider',
        name = 'ldap',
        callback = 'account_provider.graphql.read_ldap_config',
        kind = types.list(ldap_config_schema),
        args = {},
        doc = 'Show LDAP configuration',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'account_provider',
        name = 'ldap',
        callback = 'account_provider.graphql.write_ldap_config',
        kind = types.list(ldap_config_schema),
        args = {config = types.list(input_ldap_config_schema)},
        doc = 'Show LDAP configuration',
    })
end

return {
    init = init,
    read_ldap_config = read_ldap_config,
    write_ldap_config = write_ldap_config,
}
