local module_name = 'tenant_config'

local account_manager_tenant = require('account_manager.tenant')
local yaml = require('yaml')
local checks = require('checks')
local digest = require('digest')
local fun = require('fun')
local msgpack = require('msgpack')
local clock = require('clock')
local clusterwide_config = require('cartridge.clusterwide-config')
local cartridge_twophase = require('cartridge.twophase')
local cartridge_topology = require('cartridge.topology')
local cartridge = require('cartridge')
local vars = require('common.vars').new(module_name)
local tenant_config = require('roles.core.tenant.config')
local tenant = require('common.tenant')

vars:new('keep_config_count')

local BASE_SPACE_NAME = 'tdg_tenant_config'
local BASE_SEQUENCE_NAME = 'tdg_tenant_config_sequence'

local SPACE_FORMAT = {
    { name = 'section',     type = 'string' },
    { name = 'version',     type = 'unsigned' },
    { name = 'data',        type = 'any' },
    { name = 'hash',        type = 'string' },
    { name = 'timestamp',   type = 'unsigned'},
    { name = 'comment',     type = 'string', is_nullable = true },
    { name = 'active',      type = 'boolean' },
}

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_sequence_name()
    return tenant.get_sequence_name(BASE_SEQUENCE_NAME)
end

local function init()
    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        return
    end

    box.begin()
    local space = box.schema.space.create(space_name, { if_not_exists = true })
    local sequence_name = get_sequence_name()
    box.schema.sequence.create(sequence_name, { if_not_exists = true })

    space:format(SPACE_FORMAT)

    space:create_index('section_version', {
        parts = {
            { field = 'section', type = 'string' },
            { field = 'version', type = 'unsigned' },
        },
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })

    box.commit()
end

local function get_space()
    local space_name = get_space_name()
    return box.space[space_name]
end

local function get_sequence()
    local space_name = get_sequence_name()
    return box.sequence[space_name]
end

local function get_next_version()
    return get_sequence():next()
end

local function get_sections()
    local space = get_space()
    local available_sections = {}
    for _, tuple in space:pairs() do
        if available_sections[tuple.section] == nil then
            available_sections[tuple.section] = true
        end
    end

    return available_sections
end

local function config_list()
    local space = get_space()

    local result = {}
    local versions_map = {}

    for _, tuple in space:pairs() do
        local version = tuple.version
        local timestamp = tuple.timestamp
        local comment = tuple.comment

        if versions_map[version] == nil then
            versions_map[version] = true
            table.insert(result, {
                version = version,
                timestamp = timestamp,
                comment = comment,
            })
        end
    end

    table.sort(result, function(a, b) return b.version < a.version end)
    return result
end

local function config_get_raw(version)
    local space = get_space()
    local config = {}

    if version == nil then
        version = -1ULL
    end

    local last_section
    for _, tuple in space:pairs({}, {iterator = box.index.REQ}) do
        if tuple.section ~= last_section and tuple.version <= version then
            config[tuple.section] = tuple
            last_section = tuple.section
        end
    end

    return config
end

local function cleanup(keep_config_count)
    local space = get_space()
    if keep_config_count == nil then
        return
    end

    box.begin()
    local versions_list = config_list()
    local version_to_delete = versions_list[keep_config_count + 1]
    if version_to_delete == nil then
        return
    end

    local latest_kept = versions_list[keep_config_count]
    local latest_kept_config = config_get_raw(latest_kept.version)

    -- Drop old sections (tuple.version < version_to_save)
    for _, tuple in space:pairs({}, {iterator = box.index.GE}) do
        if tuple.version < latest_kept.version then
            space:delete({tuple.section, tuple.version})
        end
    end

    -- Since we use versioning over sections
    -- some section with version < version_to_save could be used in
    -- new configuration versions where version >= version_to_save
    -- here we move sections with versions to drop to version_to_save
    -- to support config consistency
    for _, tuple in pairs(latest_kept_config) do
        local updated = tuple:update({
            {'=', 'version', latest_kept.version},
            {'=', 'timestamp', latest_kept.timestamp},
            {'=', 'comment', latest_kept.comment or box.NULL},
        })
        space:replace(updated)
    end
    box.commit()
end

local function get_section_hash(section)
    return digest.sha224_hex(msgpack.encode(section))
end

local function config_get()
    local raw = config_get_raw()
    local result = {}
    for section_name, info in pairs(raw) do
        result[section_name] = info.data
    end
    return result
end

local function config_save(cfg)
    if box.info.ro == true then
        return
    end

    local space = get_space()

    local timestamp = clock.time64()
    local version = get_next_version()

    local sections = get_sections()
    for section_name in pairs(sections) do
        sections[section_name] = box.NULL
    end

    for section_name, data in pairs(cfg) do
        sections[section_name] = data
    end

    for section_name, data in pairs(sections) do
        local last_section = space.index[0]:max({section_name})
        local hash = get_section_hash(data)
        if last_section == nil or last_section.hash ~= hash then
            space:replace({
                section_name,
                version,
                data,
                hash,
                timestamp,
                box.NULL,
                false,
            })
        end
    end

    cleanup(vars.keep_config_count)

    return cfg
end

local function get_instance_uri_list()
    local topology = cartridge.config_get_readonly('topology')
    local uri_list = {}
    local refined_uri_list = cartridge_topology.refine_servers_uri(topology)
    for _, uuid in fun.filter(cartridge_topology.not_disabled, topology.servers) do
        table.insert(uri_list, refined_uri_list[uuid])
    end
    return uri_list
end

local function apply_clusterwide_config(cwcfg, opts)
    if opts == nil then
        opts = {}
    end

    init()

    local prev_cfg = config_get()
    local prev_cwcfg, err = clusterwide_config.new(prev_cfg)
    if err ~= nil then
        return nil, err
    end

    cwcfg = tenant.patch_clusterwide_config(cwcfg, prev_cwcfg, opts)
    local cwcfg_plaintext = cwcfg:get_plaintext()

    local uri_list = get_instance_uri_list()
    local _, err = cartridge_twophase.twophase_commit({
        uri_list = uri_list,
        upload_data = {cwcfg_plaintext, opts},
        fn_prepare = '_G.__tenant_validate_config',
        fn_abort = '_G.__tenant_abort_apply_config',
        fn_commit = '_G.__tenant_apply_config',
        activity_name = 'TDG tenant apply config',
    })

    if err ~= nil then
        return nil, err
    end

    config_save(cwcfg_plaintext)
end

local function apply_config(cfg, opts)
    local cwcfg, err = clusterwide_config.new(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = apply_clusterwide_config(cwcfg, opts)
    if err ~= nil then
        return nil, err
    end
end

-- https://github.com/tarantool/cartridge/blob/3fc02056152f4868d248b2f1f1d302812dde5fe9/cartridge/twophase.lua#L266
local function patch_config(patch, opts)
    checks('table', '?table')
    if patch.__type == 'ClusterwideConfig' then
        local err = "bad argument #1 to patch_clusterwide" ..
            " (table expected, got ClusterwideConfig)"
        error(err, 2)
    end

    local clusterwide_config_old = tenant.get_cwcfg()
    if clusterwide_config_old == nil then
        clusterwide_config_old = clusterwide_config.new({})
    end

    local clusterwide_config_new, err = clusterwide_config_old:copy_and_patch(patch)
    if err ~= nil then
        return nil, err
    end

    local _, err = apply_clusterwide_config(clusterwide_config_new, opts)
    if err ~= nil then
        return nil, err
    end

    -- TODO: https://github.com/tarantool/tdg2/issues/878
    if tenant.is_default() then
        local _, err = cartridge.config_patch_clusterwide(patch)
        if err ~= nil then
            return nil, err
        end
    end
end

local function get_type_ddl(type_name, timestamp)
    local space = get_space()

    for _, tuple in space:pairs({'ddl.yml'}, {iterator = box.index.REQ}) do
        if tuple.timestamp <= timestamp and tuple.data ~= nil then
            local data = yaml.decode(tuple.data)
            if data[type_name] ~= nil then
                return data[type_name]
            end
        end
    end
    return
end

local function set_tenant_state(uid, state, state_reason)
    local res, err = account_manager_tenant.set_state(uid, state, state_reason)
    if err ~= nil then
        return nil, err
    end

    local uri_list = get_instance_uri_list()
    local _, err = cartridge_twophase.twophase_commit({
        uri_list = uri_list,
        upload_data = {uid, state},
        fn_prepare = '_G.__tenant_set_state_prepare',
        fn_abort = '_G.__tenant_set_state_abort',
        fn_commit = '_G.__tenant_set_state',
        activity_name = 'TDG tenant set state',
    })
    if err ~= nil then
        return nil, err
    end

    return res
end

return {
    init = init,
    config_save = config_save,
    config_list = config_list,
    config_get = config_get,
    get_type_ddl = get_type_ddl,

    apply_config = apply_config,
    validate_config = tenant_config.validate,
    patch_config = patch_config,

    set_tenant_state = set_tenant_state,
}
