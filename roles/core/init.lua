local account_manager = require('roles.core.account_manager')
local configuration_archive = require('roles.core.configuration_archive')
local notifier = require('roles.core.notifier')
local scheduler = require('roles.core.scheduler')
local sequence_generator = require('roles.core.sequence_generator')
local core_config = require('roles.core.config')
local account_settings = require('roles.core.account_settings')
local tenant_settings = require('roles.core.tenant_settings.init')
local tenant = require('roles.core.tenant')
local common_tenant = require('common.tenant')

local system_methods = {
    init = true,
    apply_config = true,
    validate_config = true,
    tenant_apply_config = true,
    tenant_validate_config = true,
}
local function extend_role(role, methods)
    for name, fun in pairs(methods) do
        if system_methods[name] == nil then
            role[name] = fun
        end
    end
end

local function init()
    return true
end

local function tenant_validate_config(cfg)
    local _, err = core_config.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager.tenant_validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = scheduler.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = sequence_generator.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = configuration_archive.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function tenant_apply_config(cfg, opts)
    -- TODO: account_manager is a part of "tenant" or "cluster" configuration?
    local _, err = account_manager.tenant_apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    local _, err = scheduler.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    configuration_archive.init(cfg)
    local _, err = configuration_archive.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = tenant_settings.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function validate_config(cfg)
    local _, err = core_config.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = notifier.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function apply_config(cfg, opts)
    local _, err = notifier.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    local _, err = sequence_generator.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_manager.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = account_settings.apply_config()
    if err ~= nil then
        return nil, err
    end

    tenant.init()
    local cwcfg = common_tenant.get_cwcfg()
    local cwcfg_plaintext = cwcfg:get_plaintext()
    tenant.config_save(cwcfg_plaintext)

    return true
end

local role_methods =  {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,

    tenant_validate_config = tenant_validate_config,
    tenant_apply_config = tenant_apply_config,

    role_name = 'core',
    implies_router = true,
    dependencies = {'cartridge.roles.vshard-router'},
}

function role_methods.tenant_config_get()
    return tenant.config_get()
end

function role_methods.core_tenant_get_type_ddl(type_name, version)
    return tenant.get_type_ddl(type_name, version)
end

function role_methods.core_tenant_apply_config(cfg, opts)
    local _, err = tenant.validate_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    return tenant.apply_config(cfg, opts)
end

function role_methods.core_tenant_patch_config(patch, opts)
    return tenant.patch_config(patch, opts)
end

function role_methods.tenant_set_state(uid, state, state_reason)
    return tenant.set_tenant_state(uid, state, state_reason)
end

extend_role(role_methods, account_manager)
extend_role(role_methods, configuration_archive)
extend_role(role_methods, notifier)
extend_role(role_methods, scheduler)
extend_role(role_methods, sequence_generator)
extend_role(role_methods, account_settings)
extend_role(role_methods, tenant_settings)

return role_methods
