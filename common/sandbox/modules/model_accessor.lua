local checks = require('checks')
package.loaded['checks'] = nil
local forever_checks = require('checks')
package.loaded['checks'] = checks

local cartridge = require('cartridge')
local errors = require('errors')
local json = require('json')
local tenant = require('common.tenant')
local model_flatten = require('common.model_flatten')
local model_accessor = require('common.model_accessor')
local model_utils = require('common.model_utils')

local model_accessor_error = errors.new_class('model_accessor_error')

local sformat = string.format

-- Check that current instance contains "storage" role
-- and function is run inside "call_on_storage".
local function assert_call_on_storage(fn_name)
    local this_rs = cartridge.config_get_readonly('topology').replicasets[box.info.cluster.uuid]
    if not this_rs.roles.storage then
        error(sformat("Function %q can not be used. There is no storage on current instance", fn_name), 0)
    end
end

local function atomic_if_not_in_transaction(fun, ...)
    local in_transaction = box.is_in_txn()
    if in_transaction == false then
        box.begin()
    end

    local data, err = model_accessor_error:pcall(fun, ...)

    if in_transaction == false then
        if err ~= nil then
            box.rollback()
        else
            box.commit()
        end
    end
    return data, err
end

local function unflatten(type_name, tuple)
    forever_checks('string', 'table|tuple')

    local serializer = tenant.get_serializer()
    local object, err = model_flatten.unflatten_record(tuple, serializer, type_name)
    if err ~= nil then
        return nil, model_accessor_error:pcall('Error during unflatten tuple %q: %s', json.encode(tuple), err)
    end

    return object
end

local function get(type_name, filter, options)
    forever_checks('string', 'table', '?table')
    assert_call_on_storage('model_accessor.get')

    local tuple, err = model_accessor_error:pcall(model_accessor.get, type_name, filter, options)
    if err ~= nil then
        return nil, err
    end

    if tuple == nil then
        return nil
    end

    if options ~= nil and options.raw == true then
        return tuple
    end

    local serializer = tenant.get_serializer()
    local object, err = model_flatten.unflatten_record(tuple, serializer, type_name)
    if err ~= nil then
        return nil, err
    end

    return object
end

local function find(type_name, filter, options)
    forever_checks('string', 'table', '?table')
    assert_call_on_storage('model_accessor.find')

    if options ~= nil then
        if options.after ~= nil then
            options.after = {scan = options.after}
        elseif options.first ~= nil and options.first < 0 then
            return nil, model_accessor_error:new('Negative first should be specified only with after option')
        end
    end

    local result, err = model_accessor_error:pcall(model_accessor.find, type_name, filter, options)
    if err ~= nil then
        return nil, err
    end

    local tuples = result.tuples
    if options ~= nil and options.raw == true then
        return tuples
    end

    local serializer = tenant.get_serializer()
    local objects, err = model_flatten.unflatten(tuples, serializer, type_name)
    if err ~= nil then
        return nil, err
    end

    for object_no, object in ipairs(objects) do
        local tuple = tuples[object_no]
        object.cursor = tuple[#tuple]
    end

    return objects
end

local function count(type_name, filter, options)
    forever_checks('string', 'table', '?table')
    assert_call_on_storage('model_accessor.count')

    local result, err = model_accessor_error:pcall(model_accessor.count, type_name, filter, options)
    if err ~= nil then
        return nil, err
    end

    return result
end

-- Some features as "auto_increment", "defaults", "default_functions" are
-- not enabled since such operation could yield
local put_option_check = { version = '?number|cdata', only_if_version = '?number|cdata', if_not_exists = '?boolean' }
local function put(type_name, object, options)
    forever_checks('string', 'table', put_option_check)
    assert_call_on_storage('model_accessor.put')

    local ddl = tenant.get_ddl()
    local serializer = tenant.get_serializer()

    local flat, err = model_flatten.flatten(object, serializer, type_name)
    if err ~= nil then
        return nil, err
    end

    local affinity = {}
    for _, index in ipairs(serializer[type_name][2].affinity) do
        table.insert(affinity, flat[1][index])
    end

    local bucket_id = model_utils.get_bucket_id_for_key(affinity)
    local replicaset, err = vshard.router.route(bucket_id)
    if err ~= nil then
        return nil, err
    end

    if replicaset.uuid ~= box.info.cluster.uuid then
        return nil, model_accessor_error:new('Unable to insert object on current storage - bucket_id mismatch')
    end

    local format = model_flatten.field_id_by_name(ddl[type_name].format, type_name)
    local bucket_id_index = format['bucket_id']
    flat[1][bucket_id_index] = bucket_id

    local _, err = atomic_if_not_in_transaction(model_accessor.put, type_name, flat, options)
    if err ~= nil then
        return nil, err
    end

    return 1
end

local update_option_check = { version = '?number|cdata', only_if_version = '?number|cdata' }
local function update(type_name, filter, updaters, options)
    forever_checks("string", "table", "table", update_option_check)
    assert_call_on_storage('model_accessor.update')

    local tuples, err = atomic_if_not_in_transaction(model_accessor.update, type_name, filter, updaters, options)
    if err ~= nil then
        return nil, err
    end
    return #tuples
end

local delete_option_check = { version = '?number|cdata', only_if_version = '?number|cdata' }
local function delete(type_name, filter, options)
    forever_checks('string', 'table', delete_option_check)
    assert_call_on_storage('model_accessor.delete')

    local tuples, err = atomic_if_not_in_transaction(model_accessor.delete, type_name, filter, options)
    if err ~= nil then
        return nil, err
    end
    return #tuples
end

local function begin_transaction()
    assert_call_on_storage('model_accessor.begin_transaction')
    return box.begin()
end

local function commit_transaction()
    assert_call_on_storage('model_accessor.commit_transaction')
    return box.commit()
end

local function rollback_transaction()
    assert_call_on_storage('model_accessor.rollback_transaction')
    return box.rollback()
end

local function is_in_transaction()
    assert_call_on_storage('model_accessor.is_in_transaction')
    return box.is_in_txn()
end

local function is_read_only()
    assert_call_on_storage('model_accessor.is_read_only')
    return box.info.ro
end

local function snapshot()
    assert_call_on_storage('model_accessor.snapshot')
    return model_accessor_error:pcall(box.snapshot)
end

return {
    -- CRUD
    find = find,
    count = count,
    update = update,
    put = put,
    delete = delete,
    get = get,

    -- TXN
    is_in_transaction = is_in_transaction,
    begin_transaction = begin_transaction,
    rollback_transaction = rollback_transaction,
    commit_transaction = commit_transaction,

    -- utils
    unflatten = unflatten,

    is_read_only = is_read_only,
    snapshot = snapshot,
}
