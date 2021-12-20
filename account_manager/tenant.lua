local module_name = 'account_manager.tenant'

local uuid = require('uuid')
local clock = require('clock')
local vars = require('common.vars').new(module_name)
local errors = require('errors')
local states = require('account_manager.states')
local tenant_lib = require('common.tenant')
local tenant_error = errors.new_class('account_manager_tenant_error')
local httpd_utils = require('common.httpd_utils')

vars:new_global('users')
vars:new_global('tokens')

local format = {
    {name = 'uid', type = 'string', is_nullable = false},
    {name = 'name', type = 'string', is_nullable = false},
    {name = 'description', type = 'string', is_nullable = true},
    {name = 'created_at', type = 'unsigned', is_nullable = false},
    {name = 'state', type = 'unsigned', is_nullable = false},
    {name = 'state_reason', type = 'string', is_nullable = true},
}

local space_name = 'tdg_tenants'

local function init()
    if box.info.ro then
        return
    end

    local space = box.space[space_name]
    if space ~= nil then
        return
    end

    box.begin()
    space = box.schema.space.create(space_name, {
        if_not_exists = true,
    })
    space:format(format)

    space:create_index('uid', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'uid', type = 'string', collation = 'unicode_ci'}},
    })

    space:create_index('name', {
        type = 'HASH',
        unique = true,
        if_not_exists = true,
        parts = {{field = 'name', type = 'string', collation = 'unicode_ci'}},
    })
    box.commit()
end

local function get_space()
    local space = box.space[space_name]
    if space == nil then
        init()
        space = box.space[space_name]
        assert(space)
    end
    return space
end

local function unflatten(tenant)
    local result = {}

    if box.tuple.is(tenant) then
        result = tenant:tomap({names_only = true})
    else
        for i ,field in ipairs(format) do
            result[field.name] = tenant[i]
        end
    end

    return result
end

local function get_impl(uid)
    local space = get_space()

    local tuple

    if tenant_lib.is_default() or tenant_lib.uid() == uid then
        tuple = space:get({uid})
    end

    if tuple == nil then
        return nil, tenant_error:new('Tenant %q is not found', uid)
    end

    return tuple
end

local function get(uid)
    local tuple, err = get_impl(uid)
    if err ~= nil then
        return nil, err
    end

    return unflatten(tuple)
end

local system_names = {
    ['default'] = true,
}

local function create(name, description)
    if not tenant_lib.is_default() then
        return nil, tenant_error:new('Tenant creation is not possible for non-default tenants')
    end

    name = name:strip()
    if #name == 0 then
        return nil, tenant_error:new('Failed to create tenant: non-zero length name is required')
    end

    if system_names[name:lower()] ~= nil then
        return nil, tenant_error:new('Failed to create tenant: tenant %q already exists', name:lower())
    end

    local space = get_space()

    local tuple = space.index.name:get({name})
    if tuple ~= nil then
        return nil, tenant_error:new('Failed to create tenant: tenant %q already exists', tuple.name)
    end

    local uid = uuid.str()
    tuple = space:replace({
        uid,
        name,
        description,
        clock.time64(),
        states.ACTIVE,
        box.NULL,
    })

    local _, err = tenant_lib.call_with_tenant(uid, tenant_lib.patch_config, {})
    if err ~= nil then
        return nil, err
    end

    local port_number, err = httpd_utils.generate_httpd_port_number(uid)
    if err ~= nil then
        return nil, err
    end

    local _, err = tenant_lib.call_with_tenant(uid, httpd_utils.set_port_number, port_number)
    if err ~= nil then
        return nil, err
    end

    return unflatten(tuple)
end

local function update(uid, name, description)
    local space = get_space()

    local tuple, err = get_impl(uid)
    if err ~= nil then
        return nil, err
    end

    local update_list = {}
    if name ~= nil then
        name = name:strip()
        if #name == 0 then
            return nil, tenant_error:new('Failed to update tenant: non-zero length name is required')
        end

        local tuple_with_name = space.index.name:get({name})
        if tuple_with_name ~= nil and tuple_with_name.uid ~= uid then
            return nil, tenant_error:new('Tenant with name %q already exists', name)
        end
        table.insert(update_list, {'=', 'name', name})
    end

    if description ~= nil then
        table.insert(update_list, {'=', 'description', description})
    end

    if #update_list > 0 then
        tuple = space:update({uid}, update_list)
    end

    return unflatten(tuple)
end

local function set_state(uid, state, state_reason)
    local space = get_space()

    local tuple, err = get_impl(uid)
    if err ~= nil then
        return nil, err
    end

    local update_list = {}
    local state_id = states.from_string(state)

    if state_id == nil then
        return nil, tenant_error:new('Unknown state value: %q', state)
    end

    if state_id == states.NEW then
        return nil, tenant_error:new('Cannot set state "NEW" for tenant %q', uid)
    end

    if state_id == tuple['state'] then
        return nil, tenant_error:new('Tenant %q is already in  %q state', uid, state)
    end

    table.insert(update_list, {'=', 'state', state_id})
    -- state_reason is nullable
    state_reason = state_reason or box.NULL
    table.insert(update_list, {'=', 'state_reason', state_reason})

    local tuple = space:update({uid}, update_list)
    return unflatten(tuple)
end

local function delete(uid)
    local space = get_space()

    local _, err = get_impl(uid)
    if err ~= nil then
        return nil, err
    end

    for _, user in vars.users.iterate() do
        if user.is_deleted ~= true and user.tenant == uid then
            return nil, tenant_error:new('Tenant %q is used by user %q', uid, user.email)
        end
    end

    for _, token in vars.tokens.iterate() do
        if token.is_deleted ~= true and token.tenant == uid then
            return nil, tenant_error:new('Tenant %q is used by token %q', uid, token.name)
        end
    end

    local tuple = space:delete({uid})

    return unflatten(tuple)
end

local function list()
    local space = get_space()

    local result
    if tenant_lib.is_default() then
        result = {}
        for _, tuple in space:pairs() do
            table.insert(result, unflatten(tuple))
        end
    else
        local tenant_uid = tenant_lib.uid()
        local tenant, err = get(tenant_uid)
        if err ~= nil then
            return nil, err
        end
        result = {tenant}
    end

    return result
end

local function apply_config()
    init()

    vars.users = require('account_manager.user')
    vars.tokens = require('account_manager.token')
end

local function get_tenant_port(uid)
    if uid == nil or uid == 'default' then
        return httpd_utils.get_default_httpd_port()
    end

    -- we do not capture errors here because nil result is acceptable
    local res = tenant_lib.call_with_tenant(uid, httpd_utils.get_port_number)
    return res
end

local function get_tenant_details(uid)
    local res
    if uid ~= nil then
        if not tenant_lib.is_default() and uid ~= tenant_lib.uid() then
            return nil, tenant_error:new("Only default tenant administrators has access to another tenant details")
        end

        res = { uid = uid, port = get_tenant_port(uid) }
    else
        if tenant_lib.is_default() then
            res = { uid = 'default', port = httpd_utils.get_default_httpd_port() }
        else
            res = { uid = tenant_lib.uid(), port = httpd_utils.get_port_number() }
        end
    end

    return res
end

local function details_list()
    local tenants, err = list()
    if err ~= nil then
        return nil, err
    end

    local res = {}

    if tenant_lib.is_default() then
        table.insert(res, {uid = 'default', port = get_tenant_port()})
    end

    for _, v in ipairs(tenants) do
        local port, err = get_tenant_port(v.uid)
        if err ~= nil then
            return nil, err
        end
        table.insert(res, {uid = v.uid, port = port})
    end

    return res
end

return {
    apply_config = apply_config,

    create = create,
    update = update,
    set_state = set_state,
    delete = delete,
    get = get,
    list = list,
    details = get_tenant_details,
    details_list = details_list,
}
