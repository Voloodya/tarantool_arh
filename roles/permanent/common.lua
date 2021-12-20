local module_name = 'roles.permanent.common'

local audit_log = require('audit.log')
local common_log = require('log.log')
local task = require('common.task')
local cluster_config = require('common.admin.cluster_config')
local cartridge_clusterwide_config = require('cartridge.clusterwide-config')
local cartridge_upload = require('cartridge.upload')
local common_graphql = require('common.graphql')
local tenant = require('common.tenant')
local sandbox_registry = require('common.sandbox.registry')
local sandbox = require('common.sandbox')

local vars = require('common.vars').new(module_name)
vars:new('prepared_data')
vars:new('set_tenant_state_data')

local errors = require('errors')
local config_error = errors.new_class('Invalid cluster config')

local SPACE_NAME = 'tdg_tenant_local_config'

local SPACE_FORMAT = {
    {name = 'tenant', type = 'string'},
    {name = 'config', type = 'any'},
    {name = 'state', type = 'string'},
}

local function get_local_config_space()
    return box.space[SPACE_NAME]
end

local function init_tenant_config_space()
    if type(box.cfg) == 'function' then
        return
    end

    if box.info.ro then
        return
    end

    local space = get_local_config_space()
    if space ~= nil then
        return
    end

    box.begin()
    space = box.schema.space.create(SPACE_NAME, {is_local = true})
    space:format(SPACE_FORMAT)
    space:create_index('pk', {parts = {{field = 'tenant', type = 'string'}}})
    box.commit()
end

local function local_config_save(cfg)
    local tenant_uid = tenant.uid()
    local space = get_local_config_space()
    if space == nil then
        common_log.error('Space %q is not found created on current instance. ' ..
            'Some problems with configuration apply on instance startup is possible', SPACE_NAME)
        return
    end
    space:upsert({tenant_uid, cfg, tenant.states.ACTIVE}, {{'=', 'config', cfg}})
end

local function local_config_list()
    local space = get_local_config_space()
    if space == nil then
        return {}
    end

    local result = {}
    for _, tuple in space:pairs() do
        result[tuple.tenant] = tuple
    end

    return result
end

local function local_config_get(tenant_uid)
    local space = get_local_config_space()
    if space == nil then
        return nil
    end

    local tuple = space:get({tenant_uid})
    if tuple == nil then
        return nil
    end
    return tuple.config
end

local function local_tenant_set_state(tenant_uid, state)
    local space = get_local_config_space()
    if space == nil then
        return nil
    end

    space:update({tenant_uid}, {{'=', 'state', state}})
end

local SERVICE_USER_NAME = 'tdg_service_user'
-- No password by default. Access granted to small set of functions
local SERVICE_USER_DEFAULT_PASSWORD = ''

local allowed_functions = {
    'execute_graphql',              -- common/graphql.lua
    'call_service',                 -- input_processor/services/init.lua
    'tarantool_protocol_process',   -- connector/tarantool_protocol_server.lua
    'repository.get',               -- input_processor/model_iproto.lua
    'repository.find',
    'repository.count',
    'repository.put',
    'repository.put_batch',
    'repository.update',
    'repository.delete',
    'repository.map_reduce',
    'repository.call_on_storage',
}

local function create_service_user()
    if box.info.ro then
        return
    end

    box.schema.user.create(SERVICE_USER_NAME, {if_not_exists = true})
    box.schema.user.enable(SERVICE_USER_NAME)
    box.schema.user.passwd(SERVICE_USER_NAME, SERVICE_USER_DEFAULT_PASSWORD)

    for _, fn_name in ipairs(allowed_functions) do
        -- "setuid" here allows to run function as admin
        -- we need it at least to check authorization (access to token/tenant space).
        -- Then we need to it for access to spaces with data.
        box.schema.func.create(fn_name, {if_not_exists = true, setuid = true})
        box.schema.user.grant(SERVICE_USER_NAME, 'execute', 'function', fn_name, {if_not_exists = true})
    end
end

local function tenant_validate_config(cfg, _)
    local _, err = audit_log.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = common_log.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function tenant_apply_config(cfg, _)
    local _, err = audit_log.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = common_log.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function apply_config(cfg)
    init_tenant_config_space()
    create_service_user()

    local _, err = cluster_config.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = sandbox.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = common_graphql.init_iproto_graphql()
    if err ~= nil then
        return nil, err
    end

    local _, err = tenant.apply_config_all()
    if err ~= nil then
        return nil, err
    end

    return true
end

local function validate_config(cfg)
    local found = false
    for _, replicaset in pairs(cfg.topology.replicasets or {}) do
        if replicaset.roles['core'] == true then
            config_error:assert(found ~= true, 'Invalid config: "core" must be singleton')
            found = true
        end
    end
    config_error:assert(found == true, 'Invalid config: enabled "core" role required')

    local _, err = cluster_config.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = sandbox.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = tenant.validate_config_all(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

function _G.__tenant_validate_config(upload_id)
    local data = cartridge_upload.inbox[upload_id]
    local cfg, opts = data[1], data[2]

    local cwcfg, err = cartridge_clusterwide_config.new(cfg)
    if err ~= nil then
        return nil, err
    end

    local sb = sandbox_registry.set_cfg('tmp', cfg)
    sb:validate()

    local _, err = tenant.validate_config_local(cwcfg:get_readonly(), opts)
    if err ~= nil then
        error(err)
    end

    vars.prepared_data = data
    return true
end

local function clear_prepared()
    vars.prepared_data = nil
end

function _G.__tenant_apply_config()
    local data = vars.prepared_data
    local cfg, opts = data[1], data[2]

    local cwcfg, err = cartridge_clusterwide_config.new(cfg)
    if err ~= nil then
        return nil, err
    end

    init_tenant_config_space()
    task.init()

    local sb = sandbox_registry.set_cfg('tmp', cfg)
    sandbox_registry.set('active', sb)

    local _, err = tenant.apply_config_local(cwcfg, opts)
    if err ~= nil then
        error(err)
    end

    local_config_save(cfg)
    clear_prepared()
    return true
end

function _G.__tenant_abort_apply_config(_)
    clear_prepared()
    return true
end

local function init()
    init_tenant_config_space()
    tenant.init_all()
end

local function clear_set_tenant_state_data()
    vars.set_tenant_state_data = nil
end

function _G.__tenant_set_state_prepare(upload_id)
    vars.set_tenant_state_data = cartridge_upload.inbox[upload_id]
    return true
end

function _G.__tenant_set_state()
    local data = vars.set_tenant_state_data
    local uid, state = data[1], data[2]

    tenant.tenant_set_state(uid, state)
    if state == tenant.states.ACTIVE  then
        local _, err = tenant.validate_current_config_for_tenant(uid)
        if err ~= nil then
            return nil, err
        end

        local _, err = tenant.apply_current_config_for_tenant(uid)
        if err ~= nil then
            return nil, err
        end
    end
    local_tenant_set_state(uid, state)
    clear_set_tenant_state_data()
    return true
end

function _G.__tenant_set_state_abort()
    clear_set_tenant_state_data()
    return true
end

return {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,

    tenant_apply_config = tenant_apply_config,
    tenant_validate_config = tenant_validate_config,

    local_config_list = local_config_list,
    local_config_get = local_config_get,

    permanent = true,
    role_name = 'common',
    dependencies = {},
}
