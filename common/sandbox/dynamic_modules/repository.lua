local module_name = 'common.sandbox.dynamic_modules.repository'
local tenant = require('common.tenant')
local repository_error = require('errors').new_class(module_name)


local log = require('log.log').new(module_name)

local function get(_, type_name, pkey, options, context)
    local repository = tenant.get_repository(context)
    local res, err = repository:get(type_name, pkey, options or {})
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function find(_, type_name, filter, options, context)
    local repository = tenant.get_repository(context)
    local res, err = repository:find(type_name, filter, options or {})
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function repository_pairs(_, type_name, filter, options, context)
    local repository = tenant.get_repository(context)
    -- pcall is being done just to add error in log
    local ok, gen, param, state = pcall(repository.pairs, repository, type_name, filter, options or {})
    if not ok then
        local err = repository_error:new(gen)
        log.error('Error was thrown within repository.pairs execution: %q', err)
        error(err)
    end
    return gen, param, state
end

local function count(_, type_name, filter, options, context)
    local repository = tenant.get_repository(context)
    local res, err = repository:count(type_name, filter, options or {})
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function update(_, type_name, filter, updaters, options, context)
    local repository = tenant.get_repository(context)
    context = context or {}
    context.tenant = nil
    local res, err = repository:update(type_name, filter, updaters, options or {}, context)
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function delete(_, type_name, filter, options, context)
    local repository = tenant.get_repository(context)
    local res, err = repository:delete(type_name, filter, options or {})
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function map_reduce(_, type_name, filter, map_fn, combine_fn, reduce_fn, opts)
    local repository = tenant.get_repository()
    local res, err = repository:map_reduce(
        type_name, filter, map_fn, combine_fn, reduce_fn, opts)
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function call_on_storage(_, type_name, index_name, value, func_name, func_args, options, context)
    local repository = tenant.get_repository()
    local res, err = repository:call_on_storage(
        type_name, index_name, value, func_name, func_args or {}, options or {}, context or {})
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function put(_, type_name, obj, options, context)
    local repository = tenant.get_repository(context)
    context = context or {}
    context.tenant = nil
    local res, err = repository:put(type_name, obj, options or {}, context)
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function put_batch(_, type_name, array, options, context)
    local repository = tenant.get_repository(context)
    context = context or {}
    context.tenant = nil
    local res, err = repository:put_batch(type_name, array, options or {}, context)
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

local function push_job(_, name, args)
    local repository = tenant.get_repository()
    local res, err = repository:push_job(name, args or {})
    if err ~= nil then
        log.error(err)
    end
    return res, err
end

return {
    exports = {
        get = get,
        find = find,
        pairs = repository_pairs,
        count = count,
        update = update,
        delete = delete,
        map_reduce = map_reduce,
        call_on_storage = call_on_storage,
        put = put,
        put_batch = put_batch,
        push_job = push_job,
    },
}
