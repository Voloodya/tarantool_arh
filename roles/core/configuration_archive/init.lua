local backup = require('roles.core.configuration_archive.backup')
local backup_config = require('roles.core.configuration_archive.backup.config')

local function validate_config(cfg)
    backup_config.validate(cfg)
    return true
end

local function apply_config(cfg)
    backup.init(cfg)

    -- Save configuration draft
    backup.config_save_draft()
end

local function init()
    return true
end

-- Backup

local function backup_config_list()
    return backup.config_list()
end

local function backup_config_get(version)
    return backup.config_get(version)
end

local function backup_config_delete(version)
    return backup.config_delete(version)
end

local function backup_config_apply(version)
    return backup.config_apply(version)
end

local function backup_config_save_current(comment)
    return backup.config_save_current(comment)
end

return {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,

    -- Config backup
    backup_config_list = backup_config_list,
    backup_config_get = backup_config_get,
    backup_config_delete = backup_config_delete,
    backup_config_apply = backup_config_apply,
    backup_config_save_current = backup_config_save_current,
}
