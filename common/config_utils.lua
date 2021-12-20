local module_name = 'common.config_utils'

local fio = require('fio')
local clusterwide_config = require('cartridge.clusterwide-config')
local utils = require('common.utils')
local zip = require('common.zip')
local tenant = require('common.tenant')

local log = require('log.log').new(module_name)

local DDL_SECTION = 'ddl'
local LOCKED_SECTIONS_SECTION = 'locked_sections'

local cartridge_system_sections = {
    ['auth'] = true,
    ['topology'] = true,
    ['users_acl'] = true,
    ['vshard'] = true,
    ['vshard_groups'] = true,
}

-- tdg system sections
local config_system_sections = {
    [DDL_SECTION] = true,
    [LOCKED_SECTIONS_SECTION] = true,
}

for name in pairs(cartridge_system_sections) do
    config_system_sections[name] = true
end

local config_system_files = {}
for name in pairs(config_system_sections) do
    config_system_files[name .. '.yml'] = true
end

local function strip_config_sections(cfg, blacklist)
    if cfg == nil then
        return nil
    end

    blacklist = table.copy(blacklist)

    local blacklist_with_yml = table.copy(blacklist)
    for name in pairs(blacklist) do
        blacklist_with_yml[name .. '.yml'] = true
    end

    for section, _ in pairs(cfg) do
        -- don't download yaml representation of a section
        if cfg[section .. '.yml'] then
            blacklist_with_yml[section .. '.yml'] = true
        end
    end

    for section_name in pairs(blacklist_with_yml) do
        cfg[section_name] = nil
    end

    return cfg
end

local function strip_system_config_sections(cfg)
    return strip_config_sections(cfg, config_system_sections)
end

local function strip_cartridge_config_sections(cfg)
    return strip_config_sections(cfg, cartridge_system_sections)
end

local function is_system_section(section)
    return config_system_sections[section] == true or config_system_files[section] == true
end

local function unzip_data_to_tmpdir(data)
    local tempdir = fio.tempdir()
    local tempzip = fio.pathjoin(tempdir, 'config.zip')

    local ok, err = utils.write_file(tempzip, data)
    if not ok then
        fio.rmtree(tempdir)
        return nil, err
    end

    log.info('Config saved to %s', tempzip)
    local _, err = zip.unzip(tempzip, tempdir)
    if err ~= nil then
        fio.rmtree(tempdir)
        return nil, string.format('unzip error: %s', err)
    end
    log.info('Config unzipped')

    return tempdir
end

local function load_code(base_path, src_path, cw_cfg)
    local src_dir = fio.pathjoin(base_path, src_path)
    if fio.path.is_dir(src_dir) then
        local src_cfg, err = clusterwide_config.load(src_dir)
        if src_cfg == nil then
            return false, err
        end

        src_cfg = src_cfg:get_deepcopy()
        for section, content in pairs(src_cfg) do
            cw_cfg:set_plaintext(src_path .. section, content)
        end
        cw_cfg:update_luatables()
    end
    return true
end

local function load_clusterwide_config(path, opts)
    if opts == nil then
        opts = {}
    end

    local cw_config, err = clusterwide_config.load(
        fio.pathjoin(path, 'config.yml')
    )
    if err ~= nil then
        return nil, err
    end

    local _, err = load_code(path, 'src/', cw_config)
    if err ~= nil then
        return nil, err
    end

    if opts.load_extensions == true then
        local _, err = load_code(path, 'extensions/', cw_config)
        if err ~= nil then
            return nil, err
        end
    end

    return cw_config
end

local function remove_yml_ext(name)
    if name:endswith('.yml') then
        name = string.sub(name, 1, -5)
    end
    return name
end

local function is_locked_section(section)
    local locked_sections_cfg = tenant.get_cfg(LOCKED_SECTIONS_SECTION) or {}
    local name = remove_yml_ext(section)
    return locked_sections_cfg[name] == true
end

local function list_locked_sections(_, _)
    local locked_sections_cfg = tenant.get_cfg(LOCKED_SECTIONS_SECTION) or {}

    return utils.get_table_keys(locked_sections_cfg)
end

local function add_locked_section(_, args)
    local name = remove_yml_ext(args.name)

    local locked_sections_cfg = tenant.get_cfg_deepcopy(LOCKED_SECTIONS_SECTION) or {}
    locked_sections_cfg[name] = true

    tenant.patch_config({[LOCKED_SECTIONS_SECTION] = locked_sections_cfg})

    return utils.get_table_keys(locked_sections_cfg)
end

local function delete_locked_section(_, args)
    local name = remove_yml_ext(args.name)

    local locked_sections_cfg = tenant.get_cfg_deepcopy(LOCKED_SECTIONS_SECTION) or {}
    locked_sections_cfg[name] = nil

    tenant.patch_config({[LOCKED_SECTIONS_SECTION] = locked_sections_cfg})

    return utils.get_table_keys(locked_sections_cfg)
end

return {
    is_system_section = is_system_section,
    is_locked_section = is_locked_section,
    strip_system_config_sections = strip_system_config_sections,
    strip_cartridge_config_sections = strip_cartridge_config_sections,

    unzip_data_to_tmpdir = unzip_data_to_tmpdir,
    load_clusterwide_config = load_clusterwide_config,

    list_locked_sections = list_locked_sections,
    add_locked_section = add_locked_section,
    delete_locked_section = delete_locked_section,
}
