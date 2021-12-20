local module_name = 'storage.cold_storage'

local json = require('json').new({encode_invalid_as_nil = true, encode_deep_as_nil = true})
local msgpack = require('msgpack')
local cartridge = require('cartridge')
local errors = require('errors')

local model_ddl = require('common.model_ddl')
local utils = require('common.utils')
local tenant = require('common.tenant')
local vars = require('common.vars').new(module_name)

local TEMPORARY_SPACE_PREFIX = 'temporary_'
local cold_storage_e = errors.new_class('cold_storage_error')

vars:new('spaces'
     --[name] = {
     --    [id (json-encoded primary key)] = {
     --        [tostring(version)] = {
     --            type_name = ...,
     --            version = ...,
     --        },
     --    },
     --}
)

local function init()
    if box.info.ro then
        return
    end

    for space_name in pairs(box.space) do
        if type(space_name) == 'string' and space_name:startswith(TEMPORARY_SPACE_PREFIX) then
            box.space[space_name]:drop()
        end
    end

    vars.spaces = vars.spaces or {}
end

local function view_space_name(type_name, primary_key, version)
    return table.concat({
        TEMPORARY_SPACE_PREFIX,
        model_ddl.get_history_space_name(type_name, tenant.prefix()),
        '_',
        json.encode(primary_key),
        '_',
        tostring(version)
    })
end

local function get_view_space(type_name, primary_key, version)
    local space_name = view_space_name(type_name, primary_key, version)
    local space = box.space[space_name]
    if space == nil then
        return nil, cold_storage_e:new('View space %s for %q is not found', space_name, type_name)
    end
    return space
end

local function get_vinyl_space(type_name)
    local space_name = model_ddl.get_vinyl_space_name(type_name, tenant.prefix())
    local space = box.space[space_name]
    if space == nil then
        return nil, cold_storage_e:new('Vinyl space for %q is not found', type_name)
    end
    return space
end

local function fail(type_data, err)
    local type_name = type_data.type_name
    local id_str = json.encode(type_data.id)
    local version_str = tostring(type_data.version_str)
    if vars.spaces[type_name] ~= nil and vars.spaces[type_name][id_str] ~= nil then
        vars.spaces[type_name][id_str][version_str] = nil
    end
    return nil, err
end

local function fetch_cold_storage_data(tuple, type_data)
    local type_name = type_data.type_name
    local primary_key = type_data.primary_key
    local version = type_data.version

    local view_space, err = get_view_space(type_name, primary_key, version)
    if err ~= nil then
        return nil, err
    end

    local _, err = cold_storage_e:pcall(view_space.replace, view_space, tuple.data)
    if err ~= nil then
        return nil, err
    end
end

local function create_temporary_space(type_name, primary_key, version)
    if box.info.ro then
        return nil, cold_storage_e:new('Unable to create temporary space on read-only instance')
    end

    if not utils.is_array(primary_key) then
        return nil, cold_storage_e:new('Expected primary key will be an array, got %s', json.encode(primary_key))
    end

    local vinyl_space, err = get_vinyl_space(type_name)
    if err ~= nil then
        return nil, err
    end

    local id = msgpack.encode(primary_key)
    local id_str = json.encode(primary_key) -- doesn't consider difference between strings and decimals/uuid
    local iter, err = cold_storage_e:pcall(vinyl_space.pairs, vinyl_space, {id, version}, {iterator = box.index.LT})
    if iter == nil then
        return nil, cold_storage_e:new('Unable to create iterator for key %s: %s', id_str, err)
    end

    local _, tuple = iter(iter.param, iter.state)
    if tuple == nil or tuple.id ~= id then
        return nil, cold_storage_e:new('No data found for key %s with version %s', id_str, version)
    end

    version = tuple.version
    local version_str = tostring(version) -- w/a for ULL cdata
    local type_spaces = vars.spaces[type_name]
    if type_spaces ~= nil and type_spaces[id_str] ~= nil and type_spaces[id_str][version_str] ~= nil then
        return
    end

    vars.spaces[type_name] = vars.spaces[type_name] or {}
    vars.spaces[type_name][id_str] = vars.spaces[type_name][id_str] or {}
    vars.spaces[type_name][id_str][version_str] = {
        type_name = type_name,
        id = id,
        primary_key = primary_key,
        version = version,
    }
    local type_data = vars.spaces[type_name][id_str][version_str]

    local ddl, err = cartridge.rpc_call('core', 'core_tenant_get_type_ddl',
        {type_name, version})
    if err ~= nil then
        return fail(type_data, err)
    end
    if ddl == nil then
        err = cold_storage_e:new('Model/DDL for type %q not found', type_name)
        return fail(type_data, err)
    end

    local _, err = model_ddl.apply_type_ddl(ddl, {
        only_history_space = true,
        temporary = true,
        prefix = TEMPORARY_SPACE_PREFIX,
        postfix = table.concat({'_', id_str, '_', version_str}),
    })
    if err ~= nil then
        return fail(type_data, err)
    end

    local _, err = fetch_cold_storage_data(tuple, type_data)
    if err ~= nil then
        return fail(type_data, err)
    end
    return version
end

local function drop_temporary_space(type_name, pk, version)
    if box.info.ro then
        return nil, cold_storage_e:new('Unable to drop temporary space on read-only instance')
    end

    local id_str = json.encode(pk)
    local version_str = tostring(version)
    if vars.spaces[type_name] == nil then
        return nil, cold_storage_e:new('Spaces for %q not found', type_name)
    end

    if vars.spaces[type_name][id_str] == nil then
        return nil, cold_storage_e:new('Spaces for %q with key %s not found', type_name, id_str)
    end

    if vars.spaces[type_name][id_str][version_str] == nil then
        return nil, cold_storage_e:new('Spaces for %q with key %s and version %s not found', type_name, id_str, version)
    end

    local delete_list = {}
    local prefix = TEMPORARY_SPACE_PREFIX .. model_ddl.get_history_space_name(type_name, tenant.prefix())
    local postfix = id_str .. '_' .. version_str
    for space_name in pairs(box.space) do
        if type(space_name) == 'string' and
            space_name:startswith(prefix) and space_name:endswith(postfix) then
            table.insert(delete_list, space_name)
        end
    end

    for _, space_name in ipairs(delete_list) do
        box.space[space_name]:drop()
    end

    vars.spaces[type_name][id_str][version_str] = nil
end

local function get_temporary_space_status(type_name, pk, version)
    local id = json.encode(pk)
    local version_str = tostring(version)
    if vars.spaces[type_name] == nil or
        vars.spaces[type_name][id] == nil or
        vars.spaces[type_name][id][version_str] == nil then
        return 'NOT_FOUND'
    end

    return vars.spaces[type_name][id][version_str].status or 'UNKNOWN'
end

return {
    init = init,
    create_temporary_space = create_temporary_space,
    drop_temporary_space = drop_temporary_space,
    get_temporary_space_status = get_temporary_space_status,
}
