local tracing = require('common.tracing')
local storage_storage = require('storage.storage')
local storage_maintenance_server = require('storage.maintenance.server')
local storage_jobs = require('storage.jobs.jobs')

local function tenant_validate_config(cfg, opts)
    return storage_storage.tenant_validate_config(cfg, opts)
end

local function tenant_apply_config(cfg, opts)
    return storage_storage.tenant_apply_config(cfg, opts)
end

local function validate_config(cfg)
    local _, err = storage_storage.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function apply_config(cfg, opts)
    local _, err = storage_storage.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function get_aggregates()
    return storage_maintenance_server.get_aggregates()
end

local function push_job(name, args)
    local span = tracing.start_span('storage_jobs.push_job')
    local result, err = storage_jobs.push_job(name, args)
    span:finish({error = err})
    return result, err
end

local function set_job_result(id, status, result)
    local res, err = storage_jobs.set_job_result(id, status, result)
    return res, err
end

local function push_job_again(job)
    return storage_jobs.push_job_again(job)
end

local function init()
    return storage_storage.init()
end

return {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,
    get_aggregates = get_aggregates,
    push_job = push_job,
    set_job_result = set_job_result,
    push_job_again = push_job_again,

    tenant_apply_config = tenant_apply_config,
    tenant_validate_config = tenant_validate_config,

    role_name = 'storage',
    implies_router = true,
    implies_storage = true,
    dependencies = {'cartridge.roles.vshard-router', 'cartridge.roles.vshard-storage'},
}
