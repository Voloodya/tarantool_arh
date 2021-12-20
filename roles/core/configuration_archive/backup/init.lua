local module_name = 'backup'

local cartridge_utils = require('cartridge.utils')
local clock = require('clock')
local utils = require('common.utils')
local digest = require('digest')
local msgpack = require('msgpack')
local vars = require('common.vars').new(module_name)
local config_utils = require('common.config_utils')
local account = require('common.admin.account')
local tenant = require('common.tenant')
local cartridge_confapplier = require('cartridge.confapplier')
local cartridge_clusterwide_config = require('cartridge.clusterwide-config')
local cartridge_api_config = require('cartridge.webui.api-config')

vars:new('keep_config_count')

local DRAFT_CONFIG_VERSION = 0
local SPACE_NAME = 'tdg_backup_config'
local SEQUENCE_NAME = 'tdg_backup_config_sequence'

local HASH_FIELDNO = 4
local SPACE_FORMAT = {
    { name = 'section',     type = 'string' },
    { name = 'version',     type = 'unsigned' },
    { name = 'data',        type = 'any' },
    { name = 'hash',        type = 'string' },
    { name = 'timestamp',   type = 'unsigned' },
    { name = 'comment',     type = 'string', is_nullable = true },
    { name = 'uploaded_by', type = 'string', is_nullable = true },
    { name = 'active',      type = 'boolean' },
}

local function get_space_name()
    return tenant.get_space_name(SPACE_NAME, tenant.uid())
end

local function get_sequence_name()
    return tenant.get_sequence_name(SEQUENCE_NAME, tenant.uid())
end

local function get_space()
    return box.space[get_space_name()]
end

local function get_sequence()
    return box.sequence[get_sequence_name()]
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
        local active = tuple.active
        local uploaded_by = tuple.uploaded_by

        local version_str = tostring(version)
        if versions_map[version_str] == nil then
            versions_map[version_str] = true
            table.insert(result, {
                version = version,
                timestamp = timestamp,
                comment = comment,
                active = active,
                uploaded_by = uploaded_by,
            })
        end
    end

    table.sort(result, function(a, b) return b.version < a.version end)
    return result
end

local function config_get_draft()
    local space = get_space()
    local config = {}

    for _, tuple in space.index.version:pairs({DRAFT_CONFIG_VERSION}, {iterator = box.index.EQ}) do
        config[tuple.section] = tuple
    end

    return config
end

local function config_get_raw(version)
    if version == DRAFT_CONFIG_VERSION then
        return config_get_draft()
    end

    local space = get_space()
    local config = {}

    if version == nil then
        version = -1ULL
    end

    for _, tuple in space.index.version:pairs({version}, {iterator = box.index.LE}) do
        if tuple.version == DRAFT_CONFIG_VERSION then
            break
        end
        if config[tuple.section] == nil then
            config[tuple.section] = tuple
        end
    end

    return config
end

local function cleanup(keep_config_count)
    local space = get_space()
    if keep_config_count == nil then
        return
    end

    local versions_list = config_list()
    local version_to_delete = versions_list[keep_config_count + 1]
    if version_to_delete == nil then
        return
    end

    if version_to_delete == DRAFT_CONFIG_VERSION then
        return
    end

    local in_transaction = box.is_in_txn()
    if not in_transaction then
        box.begin()
    end

    local latest_kept = versions_list[keep_config_count]
    local latest_kept_config = config_get_raw(latest_kept.version)

    -- Drop old sections (tuple.version < version_to_save)
    -- section of config draft should be skipped
    for _, tuple in space:pairs({}, {iterator = box.index.GE}) do
        if tuple.version ~= DRAFT_CONFIG_VERSION and tuple.version < latest_kept.version then
            space:delete({tuple.section, tuple.version})
        end
    end

    -- Since we use versioning over sections
    -- some section with version < version_to_save could be used in
    -- new configuration versions where version >= version_to_save
    -- here we move sections with versions to drop to version_to_save
    -- to support config consistency

    -- E.g. we have following space:
    -- {section, version}
    -- ['timeout', 3]
    -- ['timeout', 2]
    -- ['timeout', 1]
    -- ['memory', 2]
    -- ['data', 1]
    -- That means we have three variants of config:
    -- {timeout(3), memory(2), data(1)}
    -- {timeout(2), memory(2), data(1)}
    -- {timeout(1), data(1)}
    -- E.g. keep_config_count = 2
    -- We can't simply drop versions < 2 - we will lose "data(1)"
    -- But we could get config for version = 2 (the oldest version that we should keep)
    -- Then remove all versions < 2
    -- And return our "oldest config" back with updated - "2" version.
    -- Finally result is:
    -- ['timeout', 3]
    -- ['timeout', 2]
    -- ['memory', 2]
    -- ['data', 2]
    for _, tuple in pairs(latest_kept_config) do
        local updated = tuple:update({
            {'=', 'version', latest_kept.version},
            {'=', 'timestamp', latest_kept.timestamp},
            {'=', 'comment', latest_kept.comment or box.NULL},
            {'=', 'uploaded_by', latest_kept.uploaded_by or box.NULL},
            {'=', 'active', latest_kept.active or false},
        })
        space:replace(updated)
    end

    if not in_transaction then
        box.commit()
    end
end

local function get_section_hash(section)
    return digest.sha224_hex(msgpack.encode(section))
end

local function get_account_name()
    local name
    if account.is_empty() == false then
        name = account.name()
    end
    return name or box.NULL
end

local function mark_all_as_inactive()
    local space = get_space()
    for _, tuple in space:pairs() do
        if tuple.active == true then
            space:update({tuple.section, tuple.version}, {{'=', 'active', false}})
        end
    end
end

local function config_remove_draft()
    local space = get_space()
    local sections = {}
    for _, tuple in space.index.version:pairs({0}) do
        sections[tuple.section] = tuple
        space:delete({tuple.section, tuple.version})
    end
    return sections
end

local function config_get_impl(version)
    local raw = config_get_raw(version)
    local result = {}
    for section_name, info in pairs(raw) do
        result[section_name] = info.data
    end
    return result
end

local function config_get(version)
    local config = config_get_impl(version)
    return config_utils.strip_system_config_sections(config)
end

local function config_get_active_version()
    local space = get_space()
    local version
    for _, tuple in space:pairs() do
        if tuple.active == true then
            version = tuple.version
            break
        end
    end
    return version
end

local function config_get_active()
    local version = config_get_active_version()
    if version == nil then
        return nil
    end

    return config_get_impl(version)
end

-- Saves temporary config
local function config_save_draft()
    if box.info.ro then
        return
    end

    local cwcfg
    if tenant.is_default() then
        cwcfg = cartridge_confapplier.get_active_config()
    else
        cwcfg = tenant.get_cwcfg()
    end

    if cwcfg == nil then
        return
    end

    local plaintext_cfg = cwcfg:get_plaintext()
    plaintext_cfg = table.copy(plaintext_cfg)
    cartridge_utils.table_setrw(plaintext_cfg)
    config_utils.strip_cartridge_config_sections(plaintext_cfg)
    config_utils.strip_system_config_sections(plaintext_cfg)

    local prev_config = config_get_active()
    if utils.cmpdeeply(plaintext_cfg, prev_config) == true then
        return
    end

    -- In case when we upload an empty config
    -- we should insert something to persist the fact that
    -- previous version of config is changed.
    -- Here we just explicitly nullify absent sections of active config.
    if prev_config == nil then
        prev_config = {}
    end
    for k in pairs(prev_config) do
        if plaintext_cfg[k] == nil then
            plaintext_cfg[k] = box.NULL
        end
    end

    local space = get_space()
    box.begin()
    -- Remove previous temporary config
    config_remove_draft()
    -- Draft now is active config version
    mark_all_as_inactive()

    -- Insert new config
    local timestamp = clock.time64()
    for section_name, data in pairs(plaintext_cfg) do
        local hash = get_section_hash(data)
        space:replace({
            section_name,
            DRAFT_CONFIG_VERSION,
            data,
            hash,
            timestamp,
            box.NULL,
            box.NULL,
            true,
        })
    end
    box.commit()
end

local function config_save_current(comment)
    assert(box.info.ro == false)

    local space = get_space()
    if space.index.version:count({DRAFT_CONFIG_VERSION}) == 0 then
        return config_get()
    end

    box.begin()
    local timestamp = clock.time64()
    local version = get_next_version()

    -- Get current config
    local current_sections = config_remove_draft()
    for section_name, tuple in pairs(current_sections) do
        current_sections[section_name] = tuple:update({
            {'=', 'uploaded_by', get_account_name()},
            {'=', 'timestamp', timestamp},
            {'=', 'version', version},
            {'=', 'active', true},
            {'=', 'comment', comment or box.NULL},
        })
    end

    -- Prepare tombstone sections
    local sections = get_sections()
    local null_hash = get_section_hash(box.NULL)
    for section_name in pairs(sections) do
        sections[section_name] = {
            section_name,
            version,
            box.NULL,
            null_hash,
            timestamp,
            comment or box.NULL,
            get_account_name(),
            true,
        }
    end

    -- Merge tombstones and current config
    for section_name, data in pairs(current_sections) do
        sections[section_name] = data
    end

    mark_all_as_inactive()

    for section_name, tuple in pairs(sections) do
        local last_section = space.index[0]:max({section_name})
        -- Replace if it's new version of section
        if last_section == nil or last_section.hash ~= tuple[HASH_FIELDNO] then
            space:replace(tuple)
        end
    end

    cleanup(vars.keep_config_count)

    box.commit()

    return config_get()
end

local function is_config_active(version)
    local space = get_space()
    for _, tuple in space.index.version:pairs({version}) do
        if tuple.active == true then
            return true
        end
    end
    return false
end

local function config_delete(version)
    if is_config_active(version) then
        return nil, 'Impossible to delete active configuration'
    end

    local config = config_get(version)
    if version == DRAFT_CONFIG_VERSION then
        config_remove_draft()
        return config
    end

    local space = get_space()

    -- We shouldn't lose sections that weren't updated
    -- since "version". Here we rewrite them with next "version"
    local iter = space.index.version:pairs({version}, {iterator = box.index.GT})
    local _, next_cfg_section = iter(iter.param, iter.state)

    box.begin()
    for _, tuple in space.index.version:pairs({version}) do
        space:delete({tuple.section, version})

        if next_cfg_section ~= nil and space:get({tuple.section, next_cfg_section.version}) == nil then
            local updated = tuple:update({
                {'=', 'uploaded_by', next_cfg_section.uploaded_by or box.NULL},
                {'=', 'timestamp', next_cfg_section.timestamp},
                {'=', 'version', next_cfg_section.version},
                {'=', 'active', next_cfg_section.active},
                {'=', 'comment', next_cfg_section.comment or box.NULL},
            })
            space:replace(updated)
        end
    end
    box.commit()
    return config
end

local function config_apply(version)
    -- We shouldn't apply/overwrite TDG system sections such as DDL.
    -- Config_get strips such sections inside.
    local config, err = config_get(version)
    if config == nil then
        return nil, err
    end

    local cw_config, err = cartridge_clusterwide_config.new(config)
    if err ~= nil then
        return nil, err
    end

    local _, err
    if tenant.is_default() then
        _, err = cartridge_api_config.upload_config(cw_config)
    else
        _, err = tenant.apply_config(cw_config:get_plaintext())
    end
    if err ~= nil then
        return nil, err
    end

    local space = get_space()
    box.begin()
    -- Also it clears "active" flag
    config_remove_draft()
    for _, tuple in space.index.version:pairs({version}) do
        space:update({tuple.section, tuple.version}, {{'=', 'active', true}})
    end

    box.commit()

    return config_get(version)
end

local function init(cfg)
    if box.info.ro then
        return
    end

    local keep_config_count
    if cfg.backup ~= nil then
        keep_config_count = cfg.backup.keep_config_count
    end

    local is_changed = (vars.keep_config_count ~= keep_config_count)
    vars.keep_config_count = keep_config_count

    if box.space[get_space_name()] ~= nil then
        if is_changed then
            cleanup(vars.keep_config_count)
        end
        return
    end

    box.begin()
    box.schema.sequence.create(get_sequence_name(), { if_not_exists = true })
    local space = box.schema.space.create(get_space_name(), { if_not_exists = true })

    space:format(SPACE_FORMAT, { if_not_exists = true })

    space:create_index('section_version', {
        parts = {
            { field = 'section', type = 'string' },
            { field = 'version', type = 'unsigned' },
        },
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })

    space:create_index('version', {
        parts = {
            { field = 'version', type = 'unsigned' },
            -- Anyway it will be implicitly merged to the index parts
            { field = 'section', type = 'string' },
        },
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })

    box.commit()
end

return {
    init = init,
    config_save_draft = config_save_draft,

    -- Public API
    config_list = config_list,
    config_get = config_get,
    config_delete = config_delete,
    config_apply = config_apply,
    config_save_current = config_save_current,
}
