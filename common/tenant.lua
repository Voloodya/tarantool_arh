local module_name = 'common.tenant'

local fiber = require('fiber')
local yaml = require('yaml')
local checks = require('checks')
local system_log = require('log')

local errors = require('errors')

local model = require('common.model')
local model_flatten = require('common.model_flatten')
local request_context = require('common.request_context')
local account = require('common.admin.account')
local account_states = require('account_manager.states')
local utils = require('common.utils')

local cartridge = require('cartridge')
local cartridge_roles = require('cartridge.roles')
local cartridge_failover = require('cartridge.failover')
local cartridge_confapplier = require('cartridge.confapplier')
local cartridge_clusterwide_config = require('cartridge.clusterwide-config')
local sandbox_registry = require('common.sandbox.registry')
local cartridge_argparse = require('cartridge.argparse')

local tenant_error = errors.new_class('tenant_error')
local tenant_access_error = errors.new_class('not enough access rights')

local vars = require('common.vars').new(module_name)
vars:new_global('tenants', {
    ['default'] = nil,
})

vars:new_global('locks', {
    ['default'] = nil,
})

vars:new_global('shared_types', {})
vars:new_global('is_initialized', false)

local states = {
    INITIAL = 'initial',
    ACTIVE = 'active',
    CONFIG_APPLY = 'config_apply',
    ERROR = 'error',
    BLOCKED = 'blocked',
}

local function get_instance_alias()
    local args = cartridge_argparse.parse()
    return args.alias or args.instance_name or ''
end

local function get_tenant_uid()
    if account.is_empty() then
        return 'default'
    else
        return account.tenant() or 'default'
    end
end

local function get_tenant_name(uid)
    if uid == nil then
        uid = get_tenant_uid()
    end
    return vars.tenants[uid].name
end

local function set_state_impl(self, state)
    if self.state ~= state then
        local name = get_tenant_name(self.uid)
        system_log.info('Switch tenant %q state to %q', name, state)
        self.state = state
    end
end

local function get_state_impl(self)
    return self.state
end

local function is_role_enabled(name)
    if type(box.cfg) == 'function' then
        return false
    end

    local topology = cartridge.config_get_readonly('topology')
    if topology == nil then
        return false
    end

    return topology.replicasets[box.info.cluster.uuid].roles[name] == true
end

local function is_tenant_default()
    local uid = get_tenant_uid()
    return uid == nil or uid == 'default'
end

local roles_list = {
    'storage',
    'runner',
    'connector',
    'core',
    'tracing',
    'common',
}

local function validate_config_impl_throw(_, cfg, cfg_opts)
    checks('table', 'table', '?table')

    if cfg_opts == nil then
        cfg_opts = {}
    end

    local sb = sandbox_registry.set_cfg('tmp', cfg)
    sb:validate()

    require('common.repository').validate_config(cfg)

    for _, role_name  in ipairs(roles_list) do
        local role = cartridge_roles.get_role(role_name)

        if role.tenant_validate_config ~= nil then
            local _, err = role.tenant_validate_config(cfg, cfg_opts)
            if err ~= nil then
                error(err)
            end
        end
    end
    return true
end

local function validate_config_impl(self, cfg, cfg_opts)
    if self:get_state() == states.CONFIG_APPLY then
        local name = get_tenant_name()
        local err = tenant_error:new('Config apply for tenant %q already is in progress. Wait for finishing', name)
        system_log.error(err)
        return nil, err
    end

    return tenant_error:pcall(validate_config_impl_throw, self, cfg, cfg_opts)
end

local function apply_config_impl_throw(self, cwcfg, cfg_opts)
    checks('table', 'ClusterwideConfig', '?table')

    if cfg_opts == nil then
        cfg_opts = {}
    end

    local cfg = cwcfg:get_readonly()
    local opts = {
        is_master = cartridge_failover.is_leader(),
    }
    if cfg_opts.migration ~= nil then
        opts.migration = cfg_opts.migration
        opts.prev_mdl = self.mdl
        opts.prev_ddl = self.ddl
    end

    assert(sandbox_registry.get('active'), 'Sandbox must be registered before')
    local tenant_uid = get_tenant_uid()

    require('audit.log').init()
    require('log.log').init(get_instance_alias())

    self.ddl = cfg.ddl

    self.types = cfg.types or ''
    local mdl, err = model.apply_config(cfg)
    if err ~= nil then
        error(err)
    end
    self.mdl = mdl or {}

    -- Calculate mdl and ddl hash
    local mdl_hash = utils.calc_hash(self.mdl)
    local ddl_hash = utils.calc_hash(self.ddl)
    -- if there are any differences in ddl or mdl we need new serializer
    if mdl_hash ~= self.mdl_hash or ddl_hash ~= self.ddl_hash then
        local serializer, err = model_flatten.new(mdl, cfg.ddl or {})
        if err ~= nil then
            error(err)
        end
        self.serializer = serializer
        self.mdl_hash = mdl_hash
        self.ddl_hash = ddl_hash
    end

    -- Save the order as repository uses mdl, ddl, serializer
    -- Use pre-calculated mdl and ddl hashes due to performance reasons
    local _, err = self.repository:apply_config(self.mdl, self.ddl, self.serializer, self.mdl_hash, self.ddl_hash)
    if err ~= nil then
        error(err)
    end

    self.cfg = cwcfg

    for _, role_name  in ipairs(roles_list) do
        if is_role_enabled(role_name) then
            local role = cartridge_roles.get_role(role_name)

            if role.tenant_apply_config ~= nil then
                local _, err = role.tenant_apply_config(cfg, opts)
                if err ~= nil then
                    error(err)
                end
            end
        end
    end

    if cfg ~= nil and cfg.shared_types ~= nil then
        vars.shared_types[tenant_uid] = cfg.shared_types
    end
end

local function apply_config_impl(self, cwcfg, opts)
    if self:get_state() == states.CONFIG_APPLY then
        local name = get_tenant_name()
        system_log.warn('Config apply for tenant %q already is in progress. Skip it', name)
        return true
    end

    local cfg = cwcfg:get_deepcopy()
    sandbox_registry.set_cfg('tmp', cfg) -- needed for model.load
    sandbox_registry.set_cfg('active', cfg)

    self:set_state(states.CONFIG_APPLY)
    local _, err = tenant_error:pcall(apply_config_impl_throw, self, cwcfg, opts)
    if err ~= nil then
        self:set_state(states.ERROR)
        return nil, err
    end
    self:set_state(states.ACTIVE)
    return true
end

local function new(data)
    local uid = get_tenant_uid()
    local is_blocked = account_states.from_string('BLOCKED') == data.state
    local instance = {
        state = is_blocked and states.BLOCKED or states.INITIAL,
        set_state = set_state_impl,
        get_state = get_state_impl,

        uid = uid,
        name = data.name,

        cfg = cartridge_clusterwide_config.new(),
        types = nil,
        mdl = nil,
        mdl_hash = nil,
        ddl = nil,
        ddl_hash = nil,
        serializer = nil,
        repository = require('common.repository').new(),
    }

    vars.tenants[uid] = instance

    return instance
end

local function get_safe(uid)
    if uid == nil then
        uid = get_tenant_uid()
    end
    return vars.tenants[uid]
end

local function get(uid)
    local tenant = get_safe(uid)
    if tenant == nil then
        return nil
    end

    tenant_error:assert(tenant:get_state() ~= states.ERROR,
        'Instance is in "ERROR" state. Please fix problems and apply config before proceeding')
    return tenant
end

local RPC_CALL_TIMEOUT = 5

-- Fetch configuration only from leader instance.
-- Replica could have outdated version of configuration.
-- It seems inappropriate case because we don't have
-- any ability to sync outdated configuration on such "outdated" instance.
local function fetch_config()
    local cfg, err = cartridge.rpc_call('core',
        'tenant_config_get', {}, {timeout = RPC_CALL_TIMEOUT, prefer_local = true, leader_only = true})
    return cfg, err
end

local function get_cwcfg()
    if is_tenant_default() then
        return cartridge_confapplier.get_active_config()
    end

    local tenant = get()
    if tenant == nil then
        return nil
    end

    return tenant.cfg
end

local function get_cfg(section)
    local cfg = get_cwcfg()
    if cfg == nil then
        return nil
    end

    return cfg:get_readonly(section)
end

local function get_cfg_non_null(...)
    local cfg = get_cwcfg()
    if cfg == nil then
        return nil
    end
    for _, section in ipairs({...}) do
        local section_cfg = cfg:get_readonly(section)
        if section_cfg ~= nil then
            return section_cfg
        end
    end
    return nil
end

local function get_ddl(type_name)
    local tenant = get()
    if tenant == nil then
        return nil
    end

    local ddl = tenant.ddl
    if type_name == nil then
        return ddl
    end
    return ddl[type_name]
end

local function get_mdl()
    local tenant = get()
    if tenant == nil then
        return nil
    end
    return tenant.mdl
end

local function get_cfg_deepcopy(section)
    local cwcfg = get_cwcfg()
    if cwcfg == nil then
        return nil
    end
    return cwcfg:get_deepcopy(section)
end

local function get_cfg_non_null_deepcopy(...)
    local cwcfg = get_cwcfg()
    if cwcfg == nil then
        return nil
    end
    for _, section in ipairs({...}) do
        local section_cfg = cwcfg:get_deepcopy(section)
        if section_cfg ~= nil then
            return section_cfg
        end
    end
    return nil
end

local function get_model()
    local tenant = get()
    if tenant.types ~= nil then
        return tenant.types
    else
        return ''
    end
end

local function get_serializer()
    local tenant = get()
    return tenant.serializer
end

local function get_repository(ctx)
    local uid
    if ctx ~= nil then
        uid = ctx.tenant
    end

    local tenant = get(uid)
    if tenant == nil then
        error(string.format('Tenant %q is not found', ctx.tenant))
    end
    return tenant.repository
end

local function get_tenant_prefix(tenant_uid)
    local tenant = tenant_uid

    if tenant == nil and account.is_empty() then
        return ''
    else
        tenant = tenant or account.tenant()
        if tenant ~= nil and tenant ~= 'default' then
            return tenant .. '_'
        end
        return ''
    end
end

local function get_space_name(base_name, tenant_uid)
    return get_tenant_prefix(tenant_uid) .. base_name
end

local function get_sequence_name(base_name, tenant_uid)
    return get_tenant_prefix(tenant_uid) .. base_name
end

local function get_state()
    local tenant = get_safe()
    return tenant:get_state()
end

local function patch_config(patch, opts)
    local _, err
    if is_tenant_default() then
        request_context.put_options(opts)
        _, err = cartridge.config_patch_clusterwide(patch)
    else
        _, err = cartridge.rpc_call(
            'core',
            'core_tenant_patch_config',
            {patch, opts},
            {leader_only = true}
        )
    end

    if err ~= nil then
        return nil, err
    end
end

local function patch_config_with_ddl(patch, opts)
    opts = opts or {}
    opts.patch_ddl = true
    return patch_config(patch, opts)
end

local function apply_config(cfg)
    local _, err = cartridge.rpc_call('core', 'core_tenant_apply_config', {cfg}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end
end

local function get_or_create_tenant()
    local tenant = get_safe()
    if tenant == nil then
        local data, err
        if is_tenant_default() then
            data = {name = 'default'}
        else
            local uid = get_tenant_uid()
            data, err = cartridge.rpc_call('core', 'tenant_get', {uid},
                {prefer_local = true, timeout = RPC_CALL_TIMEOUT})
            if err ~= nil then
                return nil, err
            end
        end

        tenant, err = new(data)
        if err ~= nil then
            return nil, err
        end
    end
    return tenant
end

local function validate_config_local(cfg, opts)
    local tenant = get_or_create_tenant()
    return tenant_error:pcall(validate_config_impl, tenant, cfg, opts)
end

local function apply_config_local(cfg, opts)
    local tenant = get_or_create_tenant()
    local res, err = tenant_error:pcall(apply_config_impl, tenant, cfg, opts)
    if err ~= nil then
        return nil, err
    end

    return res
end

local function fiber_new_impl(uid, fun, ...)
    request_context.init({})
    account.set_anonymous(uid)
    return fun(...)
end

local function fiber_new(fun, ...)
    local uid = get_tenant_uid()
    return fiber.new(fiber_new_impl, uid, fun, ...)
end

local function call_with_tenant(tenant_uid, fun, ...)
    if get_tenant_uid() == tenant_uid then
        return tenant_error:pcall(fun, ...)
    end

    local ctx
    if not request_context.is_empty() then
        ctx = request_context.get()
        request_context.clear()
    end

    request_context.init({})
    account.set_account({tenant = tenant_uid})

    local res, err = tenant_error:pcall(fun, ...)

    request_context.clear()

    if ctx ~= nil then
        request_context.set(ctx)
    end

    return res, err
end

local function init_all()
    if vars.tenants['default'] == nil then
        -- To initialize some default subsystems as log
        call_with_tenant(nil, new, {name = 'default'})
    end

    if vars.is_initialized == true then
        return true
    end

    if #cartridge.rpc_get_candidates('core') == 0 then
        if type(box.cfg) == 'function' then
            return false
        end

        -- Bootstrap from local space
        local role_common = require('roles.permanent.common')
        local cfg_list = role_common.local_config_list()
        for tenant, data in pairs(cfg_list) do
            local _, err = call_with_tenant(tenant.uid, new,
                -- TODO: Find a way to use "name" (not uid) here
                {name = tenant, state = data.state})
            if err ~= nil then
                return false, err
            end
        end
    else
        local tenant_list, err = cartridge.rpc_call('core', 'tenant_list', {}, {leader_only = true})
        if err ~= nil then
            return false, err
        end

        for _, tenant in ipairs(tenant_list) do
            local _, err = call_with_tenant(tenant.uid, new, tenant)
            if err ~= nil then
                return false, err
            end
        end
    end

    vars.is_initialized = true
    return true
end

-- This is a system routines. We run them into separate fibers
-- with specified "account" value.
-- We do it because account.tenant() required for configuration management.
local function validate_current_config_for_tenant(uid)
    local tenant = vars.tenants[uid]
    if tenant == nil then
        return nil, 'Attempt to apply config for unexisting tenant'
    end

    local cfg = tenant.cfg:get_readonly()
    local _, err = call_with_tenant(tenant.uid, validate_config_impl, tenant, cfg)
    if err ~= nil then
        return nil, err
    end
end

local function validate_config_default_tenant(cfg)
    local opts = request_context.get_options()
    local _, err = validate_config_impl(vars.tenants['default'], cfg, opts)
    if err ~= nil then
        return nil, err
    end
end

local function validate_config_all(default_tenant_config)
    init_all()

    local _, err = validate_config_default_tenant(default_tenant_config)
    if err ~= nil then
        return nil, err
    end

    if vars.is_initialized ~= true then
        return true
    end

    for uid in pairs(vars.tenants) do
        if uid ~= 'default' then
            local _, err = validate_current_config_for_tenant(uid)
            if err ~= nil then
                return nil, err
            end
        end
    end
    return true
end

local function apply_current_config_for_tenant(uid)
    local tenant = vars.tenants[uid]
    if tenant == nil then
        return nil, 'Attempt to apply config for unexisting tenant'
    end

    if tenant.state ~= states.BLOCKED then
        local cfg, err
        if tenant.state == states.INITIAL then
            if #cartridge.rpc_get_candidates('core') == 0 then
                if type(box.cfg) == 'table' then
                    -- Apply config from local space
                    local role_common = require('roles.permanent.common')
                    cfg = role_common.local_config_get(tenant.uid)
                end
            else
                cfg, err = call_with_tenant(tenant.uid, fetch_config)
                if err ~= nil then
                    return nil, err
                end
            end
            cfg, err = cartridge_clusterwide_config.new(cfg)
        else
            cfg = tenant.cfg or cartridge_clusterwide_config.new({})
        end
        if err ~= nil then
            return nil, err
        end

        local _, err = call_with_tenant(tenant.uid, apply_config_impl, tenant, cfg)
        if err ~= nil then
            return nil, err
        end
    end

    return true
end

local function apply_config_default_tenant()
    local cfg = cartridge_confapplier.get_active_config()
    if cfg == nil then
        return true
    end

    local opts = request_context.get_options()
    local _, err = apply_config_impl(vars.tenants['default'], cfg, opts)
    if err ~= nil then
        return nil, err
    end
end

local function apply_config_all()
    init_all()

    local _, err = apply_config_default_tenant()
    if err ~= nil then
        return nil, err
    end

    if vars.is_initialized ~= true then
        return true
    end

    for uid in pairs(vars.tenants) do
        if uid ~= 'default' then
            local _, err = apply_current_config_for_tenant(uid)
            if err ~= nil then
                return nil, err
            end
        end
    end
    return true
end

local function tenant_set_state(uid, state)
    local tenant, err = get(uid)
    if tenant == nil then
        return nil, err
    end

    tenant:set_state(state)
end

local function check_type_share(owner_uid, type_name, what)
    local tenant_uid = get_tenant_uid()
    if owner_uid == nil or owner_uid == tenant_uid then
        return true
    end

    local shared_types = vars.shared_types[owner_uid]
    if shared_types == nil or
        shared_types[type_name] == nil or
        shared_types[type_name].tenants == nil or
        shared_types[type_name].tenants[tenant_uid] == nil or
        shared_types[type_name].tenants[tenant_uid] == nil or
        shared_types[type_name].tenants[tenant_uid][what] ~= true  then
        return nil, tenant_access_error:new('Access for foreign tenant data is denied')
    end

    return true
end

-- This trigger does following actions:
-- * Cleanup expiration section if type is deleted
-- * Merge old and new DDL
local function patch_clusterwide_config(cfg_new, cfg_old, cfg_opts)
    local model_ddl = require('common.model_ddl')
    local old_ddl = cfg_old:get_deepcopy('ddl')
    local new_types = cfg_new:get_readonly('types')
    -- FIXME: Remove expiration
    local new_expiration = cfg_new:get_readonly('versioning') or cfg_new:get_readonly('expiration')
    sandbox_registry.set_cfg('tmp', cfg_new:get_deepcopy())

    cfg_opts = cfg_opts or {}
    if is_tenant_default() then
        local context_opts = request_context.get_options()
        if context_opts ~= nil then
            cfg_opts = context_opts
        end
    end

    if new_expiration ~= nil then
        local new_mdl, err = model.load_string(new_types)
        if err ~= nil then
            error(err)
        end

        local deleted_types = {}
        for t_name in pairs(old_ddl or {}) do
            deleted_types[t_name] = true
        end

        for _, t in ipairs(new_mdl) do
            deleted_types[t.name] = nil
        end

        local filtered_expiration = {}
        for _, section in ipairs(new_expiration) do
            -- It's possible to drop type if it doesn't present
            -- inside new_type_map but in such way we will filter
            -- "wrong" types that could be unexpected for user.
            -- Such approach allows to drop only types that was actually deleted from config.
            if deleted_types[section.type] == nil then
                table.insert(filtered_expiration, section)
            end
        end

        filtered_expiration = yaml.encode(filtered_expiration)
        -- FIXME: Remove expiration
        if cfg_new:get_readonly('versioning') ~= nil then
            cfg_new:set_plaintext('versioning.yml', filtered_expiration)
        else
            cfg_new:set_plaintext('expiration.yml', filtered_expiration)
        end
    end

    if cfg_opts.migration == nil then
        local _, err = model.validate_config(cfg_new:get_readonly())
        if err ~= nil then
            system_log.error('Model configuration validation error: %s', err)
            error(err)
        end
    else
        local migrations_validation = require('storage.migrations.validation')
        migrations_validation.validate(cfg_new:get_readonly(), cfg_opts.migration)
        old_ddl = {}
    end

    local new_ddl = cfg_new:get_readonly('ddl')
    ---- DDL is directly patched via cartridge.config_patch_clusterwide
    if cfg_opts.patch_ddl == true and new_ddl ~= nil and utils.cmpdeeply(old_ddl, new_ddl) == false then
        cfg_new:set_plaintext('ddl.yml', yaml.encode(new_ddl))
        return cfg_new
    end

    -- DDL should be generated using old DDL and new model
    if new_types ~= nil then
        local new_ddl, err = model_ddl.migrate_ddl(
            cfg_new:get_deepcopy('types'),
            old_ddl,
        -- FIXME: Remove expiration
            cfg_new:get_readonly('versioning') or cfg_new:get_readonly('expiration')
        )
        if err ~= nil then
            error(err)
        end

        old_ddl = cfg_old:get_readonly('ddl')
        if utils.cmpdeeply(new_ddl, old_ddl) == false then
            cfg_new:set_plaintext('ddl.yml', yaml.encode(new_ddl))
        else
            cfg_new:set_plaintext('ddl.yml', cfg_old:get_readonly('ddl.yml'))
        end
    end
    return cfg_new
end

return {
    is_default = is_tenant_default,
    name = get_tenant_name,
    prefix = get_tenant_prefix,
    get_space_name = get_space_name,
    get_sequence_name = get_sequence_name,
    new = new,
    get = get,
    get_state = get_state,
    tenant_set_state = tenant_set_state,
    uid = get_tenant_uid,
    check_type_share = check_type_share,

    patch_config = patch_config,
    patch_config_with_ddl = patch_config_with_ddl,
    apply_config = apply_config,
    fiber_new = fiber_new,

    get_ddl = get_ddl,
    get_mdl = get_mdl,

    get_cwcfg = get_cwcfg,
    get_cfg = get_cfg,
    get_cfg_non_null = get_cfg_non_null,
    get_cfg_deepcopy = get_cfg_deepcopy,
    get_cfg_non_null_deepcopy = get_cfg_non_null_deepcopy,

    get_model = get_model,
    get_repository = get_repository,
    get_serializer = get_serializer,

    validate_config_local = validate_config_local,
    apply_config_local = apply_config_local,
    init_all = init_all,
    validate_current_config_for_tenant = validate_current_config_for_tenant,
    apply_current_config_for_tenant = apply_current_config_for_tenant,
    validate_config_all = validate_config_all,
    apply_config_all = apply_config_all,
    call_with_tenant = call_with_tenant,
    patch_clusterwide_config = patch_clusterwide_config,

    states = table.copy(states),
}
