local checks = require('checks')
package.loaded['checks'] = nil
local forever_checks = require('checks')
package.loaded['checks'] = checks

local errors = require('errors')
local auth = require('common.admin.auth')
local request_context = require('common.request_context')
local tenant = require('common.tenant')

local model_iproto = errors.new_class('model_iproto', { capture_stack = false })

local function authorize_request(credentials)
    credentials = credentials or {}
    forever_checks('table')

    local context, err = request_context.parse_options(credentials)
    if err ~= nil then
        return nil, err
    end

    request_context.init(context)
    if not auth.authorize_with_token(credentials.token) then
        request_context.clear()
        return nil, model_iproto:new('Authorization with token failed')
    end
end

local function get(type_name, pkey, options, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:get(type_name, pkey, options)
end

local function find(type_name, filter, options, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:find(type_name, filter, options)
end

local function count(type_name, filter, options, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:count(type_name, filter, options)
end

local function put(type_name, obj, options, context, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:put(type_name, obj, options, context)
end

local function put_batch(type_name, array, options, context, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:put_batch(type_name, array, options, context)
end

local function update(type_name, filter, updaters, options, context, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:update(type_name, filter, updaters, options, context)
end

local function delete(type_name, filter, options, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:delete(type_name, filter, options)
end

local function map_reduce(type_name, filter, map_fn_name, combine_fn_name, reduce_fn_name, options, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:map_reduce(type_name, filter, map_fn_name, combine_fn_name, reduce_fn_name, options)
end

local function call_on_storage(type_name, index_name, value, func_name, func_args, options, credentials)
    local _, err = authorize_request(credentials)
    if err ~= nil then
        return nil, err
    end

    local repository = tenant.get_repository()
    return repository:call_on_storage(type_name, index_name, value, func_name, func_args, options)
end

local function init()
    rawset(_G, 'repository', {
        get = get,
        find = find,
        count = count,
        put = put,
        put_batch = put_batch,
        update = update,
        delete = delete,
        map_reduce = map_reduce,
        call_on_storage = call_on_storage,
    })
end

return {
    init = init,
}
