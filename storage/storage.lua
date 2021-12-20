local module_name = 'storage.storage'

local errors = require('errors')
local checks = require('checks')
local json = require('json')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local expiration = require('storage.expiration.init')
local expiration_checks = require('storage.expiration.checks')
local tracing = require('common.tracing')

local task_storage = require('storage.task_storage')
local input_repair_storage = require('storage.repair_storage').new('tdg_input_repair')
local output_repair_storage = require('storage.repair_storage').new('tdg_output_repair')
local sandbox_storage = require('storage.sandbox_storage')
local blob_storage = require('storage.blob_storage')
local migrations = require('storage.migrations.migrations')
local output_replication_queue = require('storage.output_replication_queue')

local audit_log_storage = require('storage.audit_log.storage')
local common_log_storage = require('storage.common_log.storage')

local jobs = require('storage.jobs.jobs')

local model = require('common.model')
local model_ddl = require('common.model_ddl')
local model_accessor = require('common.model_accessor')
local model_key_def = require('storage.model_key_def')
local tenant = require('common.tenant')

local vshard_error = errors.new_class("Vshard call failed")

vars:new('master', false)
-- This two fields need for migrations.
vars:new('prev_ddl')
vars:new('prev_mdl')

local put_options_check = { version = "?number|cdata", only_if_version = "?number|cdata", if_not_exists = "?boolean" }
local put_context_check = { routing_key = "?string" }
local function put_object(type_name, flattened_entities, options, context)
    checks('string', 'table', put_options_check, put_context_check)

    local res, err = vshard_error:pcall(
        model_accessor.put_atomic, type_name, flattened_entities, options, context)
    if err ~= nil then
        return nil, err
    end

    output_replication_queue.replicate_object(type_name, res, context)

    local version_fieldno = model_accessor.get_version_fieldno(type_name)
    if version_fieldno == nil then
        return nil
    end
    return res[1][version_fieldno]
end

local function call_for_vshard_storage(bucket_ids, fn_name)
    local ref_buckets = {}
    local call = vshard.storage[fn_name]
    for bucket_id in pairs(bucket_ids) do
        local _, err = call(bucket_id)
        if err ~= nil then
            return ref_buckets, err
        end
        table.insert(ref_buckets, bucket_id)
    end
end

local function buckets_ref(bucket_ids)
    return call_for_vshard_storage(bucket_ids, 'bucket_refrw')
end

local function buckets_unref(bucket_ids)
    return call_for_vshard_storage(bucket_ids, 'bucket_unrefrw')
end

local function put_batch(type_name, tuples, bucket_ids, options, context)
    checks('string', 'table', 'table', put_options_check, put_context_check)

    local ref_buckets, err = buckets_ref(bucket_ids)
    if err ~= nil then
        buckets_unref(ref_buckets)
        return nil, err
    end

    local res, err = vshard_error:pcall(
        model_accessor.put_atomic, type_name, tuples, options, context)

    if err ~= nil then
        buckets_unref(bucket_ids)
        return nil, err
    end

    pcall(output_replication_queue.replicate_object, type_name, res, context)

    local _, err = buckets_unref(bucket_ids)
    if err ~= nil then
        return nil, err
    end

    local version_fieldno = model_accessor.get_version_fieldno(type_name)
    if version_fieldno == nil then
        return nil
    end

    local versions = {}
    for _, tuple in ipairs(res) do
        table.insert(versions, tuple[version_fieldno])
    end
    return versions
end

local function update_object(type_name, filter, updaters, options, context)
    checks('string', 'table', 'table', '?table', '?table')

    if context == nil then
        context = {}
    end

    local res, err = vshard_error:pcall(
        model_accessor.update_atomic, type_name, filter, updaters, options, context)
    if err ~= nil then
        return nil, err
    end

    output_replication_queue.replicate_object(type_name, res.tuples, context)

    return res
end

local function tenant_validate_config(cfg, opts)
    local types = cfg['types']
    if types ~= nil then
        local mdl, err = model.load_string(types)
        if err ~= nil then
            return nil, err
        end

        local ddl = cfg['ddl']
        model_ddl.validate_ddl(ddl, opts.migration)

        local _, err = model_accessor.validate_config(mdl, ddl)
        if err ~= nil then
            return nil, err
        end

        local _, err = expiration.validate_config(mdl, cfg)
        if err ~= nil then
            return nil, err
        end
    end
    return true
end

local function validate_config(cfg)
    local _, err = audit_log_storage.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = common_log_storage.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = jobs.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function tenant_apply_config(cfg, opts)
    audit_log_storage.init()
    common_log_storage.init()

    migrations.apply_config()
    migrations.add_migrations(opts.migration, vars.prev_mdl or {}, vars.prev_ddl or {})

    local mdl = tenant.get_mdl() or {}
    local ddl = cfg['ddl'] or {}

    if box.info.ro == false then
        local _, err = model_ddl.apply_ddl(ddl)
        if err ~= nil then
            error(err)
        end
    end

    expiration.apply_config(cfg)

    local rc, err = model_accessor.apply_config(mdl, ddl)
    if rc == nil then
        error(err)
    end

    output_replication_queue.apply_config(cfg)

    if vars.master == true then
        if opts.is_master ~= true then
            log.info("Became a replica")
            vars.master = false
            jobs.stop()
        end
    else
        if opts.is_master == true then
            log.info("Became a master")
            vars.master = true
            jobs.start()
        end
    end

    migrations.run_migrations()
    model_key_def.apply_config()
    task_storage.init()
    input_repair_storage:init()
    output_repair_storage:init()
    blob_storage.apply_config()
    sandbox_storage.apply_config()

    vars.prev_mdl = mdl
    vars.prev_ddl = ddl
end

local function apply_config(cfg)
    audit_log_storage.apply_config(cfg)
    common_log_storage.apply_config(cfg)
    jobs.apply_config(cfg)
end

local function init()
    sandbox_storage.init()
end

-- VSHARD global variable for remote procedure calls
_G.vshard_proxy = {
    find = function(type_name, filter, options)
        local span = tracing.start_span('storage.find')
        local result, err = vshard_error:pcall(model_accessor.find, type_name, filter, options)
        if err ~= nil then
            span:finish({error = err})
            log.error('Find for %q with filter %q returned an error: %s', type_name, json.encode(filter), err)
            error(err)
        end
        span:finish()
        return result
    end,

    find_pairs = function(type_name, filter, options)
        local span = tracing.start_span('storage.find_pairs')

        -- tuples == nil means the error occured in model_accessor call
        local result, err = vshard_error:pcall(model_accessor.find_pairs, type_name, filter, options)
        if result == nil then
            span:finish({error = err})
            log.error('Find_pairs for %q with filter %q returned an error: %s',
                type_name, json.encode(filter), err)
            error(err)
        end
        span:finish()
        -- Swap two parameters to simplify
        return result.cursor, result.tuples
    end,

    get = function(type_name, pkey, options)
        local span = tracing.start_span('storage.get')
        local result, err = vshard_error:pcall(model_accessor.get, type_name, pkey, options)
        if err ~= nil then
            span:finish({error = err})
            log.error('Get for %q with key %q returned an error: %s', type_name, json.encode(pkey), err)
            return nil, err
        end

        span:finish()
        return result
    end,

    count = function(type_name, filter, options)
        local span = tracing.start_span('storage.count')
        local result, err = vshard_error:pcall(model_accessor.count, type_name, filter, options)
        if err ~= nil then
            span:finish({error = err})
            log.error('Count for %q with filter %q returned an error: %s', type_name, json.encode(filter), err)
            error(err)
        end

        span:finish()
        return result
    end,

    put = function(type_name, flattened_entities, options, context)
        local span = tracing.start_span('storage.put')
        local result, err = vshard_error:pcall(put_object, type_name, flattened_entities, options, context)
        if err ~= nil then
            span:finish({error = err})
            log.error('Put for %q returned an error: %s', type_name, err)
            error(err)
        end
        span:finish()
        return result
    end,

    put_batch = function(type_name, tuples, bucket_ids, options, context)
        local span = tracing.start_span('storage.put_batch')
        local result, err = vshard_error:pcall(put_batch, type_name, tuples, bucket_ids, options, context)
        if err ~= nil then
            span:finish({error = err})
            log.error('Put batch for %q returned an error: %s', type_name, err)
            error(err)
        end
        span:finish()
        return result
    end,

    update = function(type_name, filter, options, context, updaters)
        local span = tracing.start_span('storage.update')
        local result, err = vshard_error:pcall(update_object, type_name, filter, updaters, options, context)
        if err ~= nil then
            span:finish({error = err})
            log.error('Update for %q with filter %q returned an error: %s', type_name, json.encode(filter), err)
            error(err)
        end
        span:finish()
        return result
    end,

    delete = function(type_name, filter, options)
        local span = tracing.start_span('storage.delete')
        local result, err = vshard_error:pcall(model_accessor.delete_atomic, type_name, filter, options)
        if err ~= nil then
            span:finish({error = err})
            log.error('Delete for %q with filter %q returned an error: %s', type_name, json.encode(filter), err)
            error(err)
        end
        span:finish()
        return result
    end,

    remove_old_versions_by_type = function(type_name)
        local span = tracing.start_span('storage.remove_old_versions_by_type')
        local result, err = vshard_error:pcall(model_accessor.remove_old_versions_by_type, type_name)
        if err ~= nil then
            span:finish({error = err})
            log.error('Remove_old_versions_by_type for %q returned an error: %s', type_name, err)
            error(err)
        end
        span:finish()
        return result
    end,

    delete_spaces = function(object_names)
        local span = tracing.start_span('storage.delete_spaces')
        local result, err = vshard_error:pcall(model_accessor.delete_spaces, object_names)
        if err ~= nil then
            span:finish({error = err})
            log.error('Delete_spaces for %s returned an error: %s', json.encode(object_names), err)
            error(err)
        end
        span:finish()
        return result
    end,

    truncate_spaces = function(object_names)
        local span = tracing.start_span('storage.truncate_spaces')
        local result, err = vshard_error:pcall(model_accessor.truncate_spaces, object_names)
        if err ~= nil then
            span:finish({error = err})
            log.error('Truncate_spaces for %s returned an error: %s', json.encode(object_names), err)
            error(err)
        end
        span:finish()
        return result
    end,

    clear_data = function()
        local span = tracing.start_span('storage.clear_data')
        local result, err = vshard_error:pcall(model_accessor.clear_data)
        if err ~= nil then
            span:finish({error = err})
            log.error('Clear_data returned an error: %s', err)
            error(err)
        end
        span:finish()
        return result
    end,

    map_reduce = function(type_name, filter, map_fn_name, reduce_fn_name, options)
        local span = tracing.start_span('storage.map_reduce')
        local result, err = vshard_error:pcall(model_accessor.map_reduce, type_name,
            filter, map_fn_name, reduce_fn_name, options)
        if err ~= nil then
            span:finish({error = err})
            log.error('Map-reduce for %q with filter %q returned an error: %s', type_name, json.encode(filter), err)
            error(err)
        end
        span:finish()
        return result
    end,

    call_on_storage = function(func_name, func_args)
        local span = tracing.start_span('model_accessor.call_on_storage')
        local result, err = vshard_error:pcall(model_accessor.call_on_storage,
            func_name, func_args)
        if err ~= nil then
            span:finish({error = err})
            log.error('Call_on_storage %q returned an error: %s', func_name, err)
            error(err)
        end
        span:finish()
        return result
    end,

    get_spaces_len = function()
        local span = tracing.start_span('model_accessor.get_spaces_len')
        local result, err = vshard_error:pcall(model_accessor.get_spaces_len)
        if err ~= nil then
            span:finish({error = err})
            log.error('Get_spaces_len returned an error: %s', err)
            error(err)
        end
        span:finish()
        return result
    end,

    start_expiration = function(type_name)
        local span = tracing.start_span('storage.start_expiration')
        local result, err = vshard_error:pcall(expiration.run_expiration_task, type_name)
        span:finish({error = err})
        return result, err
    end,

    is_dir_writable = function(path)
        return vshard_error:pcall(expiration_checks.is_dir_writable, path)
    end,

    migration_dry_run = function(cfg, migration, ratio, at_least)
        local _, err = vshard_error:pcall(migrations.dry_run, cfg, migration, ratio, at_least)
        if err ~= nil then
            log.error('Migration_dry_run returned an error: %s', err)
            error(err)
        end
        return true
    end,

    migration_stats = function()
        local res, err = vshard_error:pcall(migrations.get_stats)
        if err ~= nil then
            log.error('Migration_stats returned an error: %s', err)
            error(err)
        end
        return res
    end,
}
-- VSHARD

local function is_job_sender_enabled()
    return jobs.is_enabled()
end

local function is_master()
    return vars.master
end

return {
    init = init,
    apply_config = apply_config,
    validate_config = validate_config,

    tenant_apply_config = tenant_apply_config,
    tenant_validate_config = tenant_validate_config,

    -- for test purposes
    is_job_sender_enabled = is_job_sender_enabled,
    is_master = is_master,
}
