local module_name = 'storage.jobs.graphql'

local log = require('log')
local json = require('json')

local cartridge = require('cartridge')
local request_context = require('common.request_context')
local graphql = require('common.graphql')
local types = require('graphql.types')
local utils = require('common.utils')
local tenant = require('common.tenant')
local vars = require('common.vars').new(module_name)

local job_repair_storage = require('storage.repair_storage').new('tdg_jobs_repair')
local statuses = require('storage.jobs.statuses')

local DEFAULT_LIMIT = 25

vars:new('repair_all_fiber')

local function objectToGraphql(object)
    return {
        id = object.id,
        time = utils.nsec_to_iso8601_str(object.time),
        status = statuses.to_string(object.status),
        reason = object.reason,
        object = json.encode(object.object),
        cursor = object.cursor,
    }
end

local function get_job_list(_, args)
    if args.id then
        local obj, err = job_repair_storage:get(args.id)

        if err ~= nil then
            return nil, err
        end

        if obj == nil then
            return {}
        end

        return { objectToGraphql(obj) }
    end

    local result = {}

    local from = nil
    if args.from then
        from = utils.iso8601_str_to_nsec(args.from)
    end

    local to = nil
    if args.to then
        to = utils.iso8601_str_to_nsec(args.to)
    end

    local objects, err = job_repair_storage:filter(
        from, to, args.reason, args.first or DEFAULT_LIMIT, args.after)

    if err ~= nil then
        return nil, err
    end

    for _, obj in pairs(objects) do
        table.insert(result, objectToGraphql(obj))
    end

    return result
end

local function delete_job(_, args)
    local ok, err = job_repair_storage:delete(args.id)
    if ok == true then
        return 'ok'
    end
    return nil, err
end

local function delete_all_jobs(_, _)
    local ok, err = job_repair_storage:clear()
    if ok == true then
        return 'ok'
    end
    return nil, err
end

local function try_again_impl(id)
    local obj, err = job_repair_storage:get(id)
    if err ~= nil then
        return nil, err
    end

    local job = obj.object

    local ok, err = job_repair_storage:update_status(job.id, statuses.IN_PROGRESS)
    if not ok then
        return nil, err
    end

    job.context.job_repair = true

    local old_context
    if not request_context.is_empty() then
        old_context = request_context.get()
    end

    request_context.set(job.context)

    local _, err = cartridge.rpc_call('storage', 'push_job_again',
        { job }, {leader_only = true})

    if old_context then
        request_context.set(old_context)
    else
        request_context.clear()
    end

    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function try_again(_, args)
    return try_again_impl(args.id)
end

local function try_again_all_impl()
    local cursor

    while true do
        local objects, err = job_repair_storage:filter(nil, nil, nil, DEFAULT_LIMIT, cursor)
        if err ~= nil then
            log.error(err)
            return
        end

        if #objects == 0 then
            return
        end

        for _, obj in pairs(objects) do
            local _, err = try_again_impl(obj.id)
            if err ~= nil then
                log.error(err)
            end

            cursor = obj.cursor
        end
    end
end

local function try_again_all(_, _)
    if vars.repair_all_fiber == nil or vars.repair_all_fiber:status() == 'dead' then
        vars.repair_all_fiber = tenant.fiber_new(try_again_all_impl)
    end
    return 'ok'
end

local function init()
    types.object {
        name = 'Job_list',
        description = 'A list of failed jobs',
        fields = {
            id = types.string.nonNull,
            time = types.string.nonNull,
            status = types.string.nonNull,
            reason = types.string.nonNull,
            object = types.string.nonNull,
            cursor = types.string.nonNull,
        },
        schema = 'admin',
    }

    graphql.add_mutation_prefix('admin', 'jobs', 'Job management')
    graphql.add_callback_prefix('admin', 'jobs', 'Job management')

    graphql.add_callback(
        {schema='admin',
         prefix='jobs',
        name='get_list',
        callback='storage.jobs.graphql.get_job_list',
        kind=types.list('Job_list'),
        args={
            id = types.string,
            from = types.string,
            to = types.string,
            reason = types.string,
            first = types.long,
            after = types.string
    }})

    graphql.add_mutation(
        {schema='admin',
         prefix='jobs',
        name='delete_job',
        callback='storage.jobs.graphql.delete_job',
        kind=types.string.nonNull,
        args={ id = types.string.nonNull }})

    graphql.add_mutation(
        {schema='admin',
         prefix='jobs',
        name='delete_all_jobs',
        callback='storage.jobs.graphql.delete_all_jobs',
        kind=types.string.nonNull})

    graphql.add_mutation(
        {schema='admin',
         prefix='jobs',
        name='try_again',
        callback='storage.jobs.graphql.try_again',
        kind=types.string.nonNull,
        args={ id = types.string.nonNull }})

    graphql.add_mutation(
        {schema='admin',
         prefix='jobs',
        name='try_again_all',
        callback='storage.jobs.graphql.try_again_all',
        kind=types.string.nonNull})
end

return {
    get_job_list = get_job_list,
    delete_job = delete_job,
    delete_all_jobs = delete_all_jobs,
    try_again = try_again,
    try_again_all = try_again_all,
    init = init
}
