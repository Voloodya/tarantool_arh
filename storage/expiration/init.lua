local module_name = 'storage.expiration'

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local model_ddl = require('common.model_ddl')
local model_accessor = require('common.model_accessor')
local expire = require('expirationd')
local version_module = require('common.version')
local file_expiration = require('storage.expiration.file')
local cold_storage_expiration = require('storage.expiration.cold_storage')
local model_expiration = require('storage.expiration.model')
local expiration_config = require('storage.expiration.config')
local expiration_utils = require('storage.expiration.utils')
local tenant = require('common.tenant')
local errors = require('errors')

local expiration_error = errors.new_class('expiration_error')

vars:new('tasks')
vars:new('tuples_per_iteration', 1000)

local function is_expired(args, tuple)
    local max_diff = args.lifetime_nsecs
    return version_module.get_diff(tuple['version']) > max_diff
end

local function stop(tasks)
    local prefix = tenant.prefix()
    for _, type_name in ipairs(tasks) do
        local space_name = model_ddl.get_space_name(type_name, prefix)
        local history_space_name = model_ddl.get_history_space_name(type_name, prefix)
        -- expire.kill could throw if task doesn't exist.
        pcall(expire.kill, space_name)
        pcall(expire.kill, history_space_name)
        log.info('Module %s: cleaning for %s stopped.', module_name, type_name)
    end
end

-- expire
local function apply_config(conf)
    -- FIXME: Remove expiration
    local cfg = conf['versioning'] or conf['expiration']

    local tasks = vars.tasks or {}
    stop(tasks)

    vars.tasks = {}
    if box.info.ro == true then
        return
    end

    if cfg == nil then
        return
    end

    local tenant_prefix = tenant.prefix()
    for _, expire_entry in ipairs(cfg) do
        local type_name = expire_entry.type

        if expire_entry.enabled == true
            and expire_entry.lifetime_hours == nil
            and expire_entry.keep_version_count == nil then
                log.warn("You don't specify keep_version_count for type %q, default value %d is used",
                    type_name, expiration_utils.get_default_keep_version_count())
        end

        if expire_entry.lifetime_hours ~= nil then
            local strategy = expire_entry.strategy
            local strategy_is_permanent = strategy == 'permanent'
            local delay_sec = expire_entry.delay_sec or 3600
            local lifetime_nsecs = expire_entry.lifetime_hours * 3600 * 1e9
            local delete_mode = model_expiration.strategy_to_delete_mode(strategy)
            local args = {lifetime_nsecs = lifetime_nsecs, mode = delete_mode}

            local space_name = model_ddl.get_space_name(type_name, tenant_prefix)
            local history_space_name = model_ddl.get_history_space_name(type_name, tenant_prefix)


            expire.start(history_space_name, history_space_name, is_expired, {
                force = true,
                atomic_iteration = strategy_is_permanent,
                args = args,
                tuples_per_iteration = vars.tuples_per_iteration,
                full_scan_time = delay_sec,
                process_expired_tuple = model_accessor.delete_tuple_by_space_id,
            })

            expire.start(space_name, space_name, is_expired, {
                force = true,
                atomic_iteration = strategy_is_permanent,
                args = args,
                tuples_per_iteration = vars.tuples_per_iteration,
                full_scan_time = delay_sec,
                process_expired_tuple = model_accessor.delete_tuple_by_space_id,
            })

            table.insert(vars.tasks, type_name)
            log.info('Module %s: cleaning for %s started.', module_name, type_name)
        end
    end

    model_expiration.apply_config(conf)
    file_expiration.apply_config(conf)
end

local function run_expiration_task(type_name)
    local strategy = model_expiration.get_strategy(type_name)
    if strategy == nil then
        return
    end

    local _, err
    if strategy == 'file' then
        _, err = file_expiration.start(type_name)
    elseif strategy == 'cold_storage' then
        _, err = cold_storage_expiration.start(type_name)
    else
        error(expiration_error:new('Unsupported expiration mode %q', strategy))
    end
    if err ~= nil then
        log.error('Expiration %s task for %q failed: %s', strategy, type_name, err)
    end
end

local function validate_config(mdl, cfg)
    -- FIXME: Remove expiration
    if (cfg.versioning or cfg.expiration) == nil then
        return
    end

    return expiration_config.validate(mdl, cfg)
end

return {
    apply_config = apply_config,
    validate_config = validate_config,
    stop = stop,
    run_expiration_task = run_expiration_task,
}
