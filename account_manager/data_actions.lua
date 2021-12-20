local module_name = 'account_manager.data_actions'

local uuid = require('uuid')
local tenant = require('common.tenant')
local model_explorer = require('common.model_explorer')
local account_manager_server = require('account_manager.server')

local vars = require('common.vars').new(module_name)
vars:new('aggregates')
vars:new_global('access_roles')

local IS_DELETED_FIELDNO = 2
local DESCRIPTION_FIELDNO = 3
local data_action_list_format = {
    {name = 'id', type = 'string', is_nullable = false},
    {name = 'is_deleted', type = 'boolean', is_nullable = false},
    {name = 'description', type = 'string', is_nullable = false},
}

local data_actions_permissions_format = {
    {name = 'action_id', type = 'string', is_nullable = false},
    {name = 'aggregate', type = 'string', is_nullable = false},
    {name = 'read', type = 'boolean', is_nullable = false},
    {name = 'write', type = 'boolean', is_nullable = false},
}

local data_action_list_base_space_name = 'tdg_data_action_list'
local data_actions_permissions_base_space_name = 'tdg_data_actions_permissions'

local function get_data_action_list_space_name()
    return tenant.get_space_name(data_action_list_base_space_name)
end

local function get_data_actions_permissions_space_name()
    return tenant.get_space_name(data_actions_permissions_base_space_name)
end

local function get_data_action_list_space()
    return box.space[get_data_action_list_space_name()]
end

local function get_data_actions_permissions_space()
    return box.space[get_data_actions_permissions_space_name()]
end

local function get_aggregates()
    local mdl, err = tenant.get_mdl()
    if err then
        return nil, err
    end

    local objects, err = model_explorer.make_object_map(mdl)
    if objects == nil then
        return nil, err
    end

    local res = {}
    for name, obj in pairs(objects) do
        if obj.indexes ~= nil then
            res[name] = { read = false, write = false }
        end
    end

    return res
end

local function data_action_exists(action)
    if action == nil or action.is_deleted == true then
        return false
    end
    return true
end

local function update_permissions(id, aggregates)
    local permissions_space = get_data_actions_permissions_space()
    for _, aggregate in ipairs(aggregates) do
        local name = aggregate.name
        if vars.aggregates[name] == nil then
            return nil, ('Invalid argument name: %q'):format(name)
        end

        local read = aggregate.read
        if read == nil then
            read = false
        end
        local write = aggregate.write
        if write == nil then
            write = false
        end

        permissions_space:replace({id, aggregate.name, read, write})
    end
end

local function get(id)
    local data_action_list = get_data_action_list_space()
    local tuple = data_action_list:get({id})
    if not data_action_exists(tuple) then
        return nil, ('Access action "%s" does not exist'):format(id)
    end

    local aggregates = table.deepcopy(vars.aggregates)
    local result = { id = id, description = tuple.description }

    local permissions_space = get_data_actions_permissions_space()
    for _, tuple in permissions_space:pairs({id}) do
        local _, aggregate, read, write = tuple:unpack()
        if aggregates[aggregate] ~= nil then
            aggregates[aggregate] = { read = read, write = write }
        else
            permissions_space:delete({id, aggregate})
        end
    end

    local aggregates_list = {}
    for name, permissions in pairs(aggregates) do
        table.insert(aggregates_list, {name = name, read = permissions.read, write = permissions.write})
    end
    result.aggregates = aggregates_list
    return result
end

local function create(description, permissions)
    description = description:strip()
    if #description == 0 then
        return nil, 'Failed to create data action: non-zero length name is required'
    end

    local data_action_list_space = get_data_action_list_space()
    local id = uuid.str()

    box.begin()
    data_action_list_space:insert({id, false, description})
    local _, err = update_permissions(id, permissions)
    if err ~= nil then
        box.rollback()
        return nil, err
    end
    box.commit()
    return get(id)
end

local function update(id, description, permissions)
    local space = get_data_action_list_space()
    local tuple = space:get({id})

    if not data_action_exists(tuple) then
        return nil, ('Access action "%s" does not exist'):format(id)
    end

    if description ~= nil then
        description = description:strip()
        if #description == 0 then
            return nil, 'Failed to create data action: non-zero length name is required'
        end
    end

    box.begin()
    if description ~= nil then
       space:update({id}, {{'=', DESCRIPTION_FIELDNO, description}})
    end
    local _, err = update_permissions(id, permissions)
    if err ~= nil then
        box.rollback()
        return nil, err
    end
    box.commit()
    account_manager_server.notify_subscribers('data_actions',
        vars.access_roles.get_roles_by_access_action(id))
    return get(id)
end

local function list()
    local space = get_data_action_list_space()
    local result = {}

    for _, tuple in space:pairs() do
        if tuple.is_deleted == false then
            table.insert(result, {id = tuple.id, description = tuple.description})
        end
    end
    return result
end

local function delete(id)
    local space = get_data_action_list_space()
    local access_action, err = get(id)
    if err ~= nil then
        return nil, err
    end
    space:update({id}, {{'=', IS_DELETED_FIELDNO, true}})
    account_manager_server.notify_subscribers('data_actions',
        vars.access_roles.get_roles_by_access_action(id))
    return access_action
end

local function aggregate_access_list_for_role(role_id)
    local result = {}
    local actions_list = vars.access_roles.get_access_actions_list(role_id)
    local permissions_space = get_data_actions_permissions_space()
    for _, access_action_tuple in ipairs(actions_list) do
        local access_action = access_action_tuple.action
        for _, permission_tuple in permissions_space:pairs({access_action}) do
            local _, aggregate, read, write = permission_tuple:unpack()
            if result[aggregate] == nil then
                result[aggregate] = {}
            end
            if result[aggregate]['read'] ~= true then
                result[aggregate]['read'] = read
            end
            if result[aggregate]['write'] ~= true then
                result[aggregate]['write'] = write
            end
        end
    end
    return result
end

local function apply_config()
    vars.access_roles = require('account_manager.access_role')
    vars.aggregates = get_aggregates()

    if box.info.ro then
        return
    end

    local data_action_list_space_name = get_data_action_list_space_name()
    if box.space[data_action_list_space_name] ~= nil then
        return
    end

    box.begin()
    local space = box.schema.space.create(data_action_list_space_name, {
        format = data_action_list_format,
        if_not_exists = true,
    })

    space:create_index('id', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'id', type = 'string'}},
    })

    local data_actions_permissions_space_name = get_data_actions_permissions_space_name()
    local space = box.schema.space.create(data_actions_permissions_space_name, {
        format = data_actions_permissions_format,
        if_not_exists = true,
    })

    space:create_index('id', {
        type = 'TREE',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'action_id', type = 'string'}, {field = 'aggregate', type = 'string'}},
    })
    box.commit()
end

return {
    apply_config = apply_config,
    get = get,
    create = create,
    update = update,
    delete = delete,
    list = list,
    aggregate_access_list_for_role = aggregate_access_list_for_role,
}
