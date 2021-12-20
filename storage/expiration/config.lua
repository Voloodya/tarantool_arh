local json = require('json')
local fio = require('fio')
local errors = require('errors')
local utils = require('common.utils')
local cron = require('common.cron')
local config_error = errors.new_class('Invalid expiration config')
local config_checks = require('common.config_checks').new(config_error)

local available_strategies = {'file', 'cold_storage', 'permanent'}
local scheduler_expiration_strategies = require('tasks.scheduler.scheduler').allowed_expiration_strategies

local function validate_permanent_expiration(_)
    return true
end

local function validate_file_expiration(cfg, section_name)
    local t = cfg['type']

    local dir = cfg['dir']
    config_error:assert(dir ~= nil, '%s[%q].dir is mandatory for "file" strategy', section_name, t)
    config_error:assert(type(dir) == 'string', '%s[%q].dir must be a string', section_name, t)
    config_error:assert(not fio.path.exists(dir) or fio.path.is_dir(dir),
        '%s[%q].dir must be a directory if it exists', section_name, t)

    local file_size_threshold = cfg['file_size_threshold']
    if file_size_threshold ~= nil then
        config_error:assert(type(file_size_threshold) == 'number',
            '%s[%q].file_size_threshold must be a number', section_name, t)
        config_error:assert(file_size_threshold > 0,
            '%s[%q].file_size_threshold must be a positive number', section_name, t)
    end

    local filename = ('.check_tdg_access.%d'):format(math.random())
    local absname = fio.pathjoin(dir, filename)
    if not fio.path.exists(dir) then
        local _, err = fio.mktree(dir)
        if err ~= nil then
            config_checks:assert(err ~= nil, 'Not enough rights to create directory "%s": %s', dir, err)
        end
    end
    local fh, err = fio.open(absname, {'O_WRONLY', 'O_CREAT', 'O_APPEND'}, tonumber('0644', 8))
    config_checks:assert(fh ~= nil,
        'Not enough rights to create files in "%s": %s', dir, err)
    fh:close()
    fio.unlink(absname)
end

local function validate_cold_storage_expiration(_)
end

local function validate_entry(i, entry_cfg, types, section_name)
    config_error:assert(type(entry_cfg) == 'table', '%s entry[%i] must be a table', section_name, i)

    local t = entry_cfg['type']
    config_error:assert(t ~= nil, '%s entry[%i] type is mandatory', section_name, i)
    config_error:assert(type(t) == 'string', '%s entry[%i] type must be a string', section_name, i)
    config_error:assert(types[t], "Can't find type %q for %s", t, section_name)
    config_error:assert(types[t].indexes ~= nil, "Type %q for %s should contain at least one index", t, section_name)

    config_checks:check_optional_luatype(string.format('%s.enabled', section_name), entry_cfg.enabled, 'boolean')
    if entry_cfg.enabled == false then
        -- TODO: maybe it's reasonable to throw an error
        -- in case "lifetime_hours" or "keep_version_count" is specified
        return
    end

    local lifetime_hours = entry_cfg['lifetime_hours']
    if lifetime_hours ~= nil then
        config_error:assert(type(lifetime_hours) == 'number',
            '%s.lifetime_hours for type %q must be a number', section_name, t)
        config_error:assert(lifetime_hours > 0, '%s.lifetime_hours for type %q must be greater than 0', section_name, t)

        local delay_sec = entry_cfg['delay_sec']
        if delay_sec ~= nil then
            config_error:assert(type(delay_sec) == 'number',
                '%s.delay_sec for type %q must be a number', section_name, t)
            config_error:assert(delay_sec > 0, '%s.delay_sec for type %q must be greater than 0', section_name, t)
        end
    end

    local keep_version_count = entry_cfg['keep_version_count']
    if keep_version_count ~= nil then
        config_error:assert(type(keep_version_count) == 'number',
            '%s.keep_version_count for type %q must be a number', section_name, t)
        config_error:assert(keep_version_count > 0,
            '%s.keep_version_count for type %q must be greater than 0', section_name, t)
    end

    local strategy = entry_cfg['strategy'] ~= nil and entry_cfg['strategy'] or 'permanent'
    config_checks:check_luatype(string.format('%s["'.. t .. '"].strategy', section_name), strategy, 'string')
    config_checks:assert(utils.has_value(available_strategies, strategy),
        '%s[%q].strategy must be one of %s', section_name, t, json.encode(available_strategies))

    if scheduler_expiration_strategies[strategy] == true then
        local schedule = entry_cfg['schedule']
        config_error:assert(
            schedule ~= nil,
            '%s[%q].schedule is mandatory for "%s" strategy',
            section_name, t, strategy
        )
        config_error:assert(type(schedule) == 'string', '%s[%q].schedule must be a string', section_name, t)
        local ok, err = cron.validate(schedule)
        config_error:assert(ok ~= nil, '%s[%q].schedule must be correct cron expression: %s', section_name, t, err)
    end

    if strategy == 'permanent' then
        validate_permanent_expiration(entry_cfg)
    elseif strategy == 'file' then
        validate_file_expiration(entry_cfg, section_name)
    elseif strategy == 'cold_storage' then
        validate_cold_storage_expiration(entry_cfg)
    end
end

local function validate_config(types, cfg)
    local default_keep_version_count = cfg['default_keep_version_count']
    config_checks:check_optional_luatype('default_keep_version_count', default_keep_version_count, 'number')
    if default_keep_version_count ~= nil then
        config_error:assert(default_keep_version_count >= 0, 'default_keep_version_count must be non-negative')
    end

    -- FIXME: Remove expiration
    local section_name = 'versioning'
    if cfg['versioning'] == nil and cfg['expiration'] ~= nil then
        section_name = 'expiration'
    end
    local expiration_cfg = cfg[section_name]
    config_error:assert(utils.is_array(expiration_cfg), string.format('%s must be an array', section_name))
    for i, entry_cfg in ipairs(expiration_cfg) do
        validate_entry(i, entry_cfg, types, section_name)
    end
end

return {
    validate = function(mdl, cfg)
        local types = {}
        for _, t in ipairs(mdl) do
            types[t.name] = t
        end

        local ok, err = pcall(validate_config, types, cfg)
        if not ok then
            return nil, err
        end
        return true
    end
}
