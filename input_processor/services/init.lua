local module_name = 'input_processor.services' -- luacheck: ignore

local json = require('json')
local checks = require('checks')
local decimal = require('decimal')
local uuid = require('uuid')
local request_context = require('common.request_context')
local cartridge = require('cartridge')
local cartridge_utils = require('cartridge.utils')
local avro_frontend = require('avro_schema.frontend')
local model_validation = require('common.model.validation')
local vars = require('common.vars').new('input_processor.services')
local model_graphql = require('common.model_graphql')
local model = require('common.model')
local graphql = require('common.graphql')
local config = require('input_processor.services.config')
local auth = require('common.admin.auth')
local actions_list = require('account_manager.access_actions_list').get()
local sandbox_registry = require('common.sandbox.registry')
local tenant = require('common.tenant')

vars:new('service_list')

local function toboolean(value)
    value = value:lower()

    if value == 'true' or value == '1' then
        return true
    elseif value == 'false' or value == '0' then
        return false
    end
    return nil
end

local cast_type = {
    string = tostring,
    byte = tostring,
    boolean = toboolean,
    long = tonumber64,
    int = tonumber64,
    float = tonumber,
    double = tonumber,
    decimal = function(value)
        local ok, result = pcall(decimal.new, value)
        if not ok then
            return nil
        end
        return result
    end,
    uuid = function(value)
        local ok, result = pcall(uuid.fromstr, value)
        if not ok then
            return nil
        end
        return result
    end,
    enum = tostring,
}

local function check_service_access(service_type)
    local access_action
    if service_type == 'query' then
        access_action = actions_list.service_read
    elseif service_type == 'mutation' then
        access_action = actions_list.service_write
    else -- unreachable
        error('Unsupported service type: ' .. tostring(service_type))
    end
    return auth.check_role_has_access(access_action)
end

local function add_service(name, service_cfg, mdl, type_map)
    checks('string', 'table', 'table', 'table')

    local fun = service_cfg['function']

    local arg_type_cast = {}
    local args = {}
    for arg_name, arg_type in pairs(service_cfg.args or {}) do
        local arg_graphql_type, err = model_graphql.resolve_graphql_type(mdl, type_map, arg_type, {input=true})
        if arg_graphql_type == nil then
            return nil, err
        end
        args[arg_name] = arg_graphql_type

        if model.is_nullable_defined(arg_type) then
            arg_type = arg_type[2]
        end

        if model.is_decimal(arg_type) then
            arg_type = 'decimal'
        elseif model.is_uuid(arg_type) then
            arg_type = 'uuid'
        end

        if cast_type[arg_type] ~= nil then
            arg_type_cast[arg_name] = cast_type[arg_type]
        end
    end

    local ret_type, err = model_graphql.resolve_graphql_type(mdl, type_map, service_cfg.return_type)
    if ret_type == nil then
        return nil, err
    end

    local query_arguments_parser = function(query_args)
        for arg_name, arg_value in pairs(query_args) do
            local cast_fn = arg_type_cast[arg_name]
            if arg_value == 'null' then
                query_args[arg_name] = box.NULL
            elseif cast_fn ~= nil then
                query_args[arg_name] = cast_fn(arg_value)
            end
        end
        return query_args
    end
    service_cfg.query_arguments_parser = query_arguments_parser

    local opts = {
        schema = tenant.uid(),
        name = name,
        callback = fun,
        kind = ret_type,
        args = args,
        doc = service_cfg.doc,
        auth_callback = function()
            return check_service_access(service_cfg.type)
        end,
    }

    if service_cfg.type == 'mutation' then
        graphql.add_function_mutation(opts)
    else
        graphql.add_function_callback(opts)
    end

    return true
end

local function cleanup(service_list)
    for name, service_cfg in pairs(service_list or {}) do
        if service_cfg.type == 'mutation' then
            graphql.remove_mutation(tenant.uid(), name)
        else
            graphql.remove_callback(tenant.uid(), name)
        end
    end
end

local function validate_and_call(service, args)
    args = args ~= nil and args or {}
    local ok, err = service.validator(args)
    if not ok then
        return nil, err
    end

    local sandbox = sandbox_registry.get('active')
    local fn, err = sandbox:dispatch_function(service['function'], {protected = true})
    if err ~= nil then
        return nil, err
    end

    return sandbox.call(fn, args)
end

local function call_service_impl(name, args, options)
    if not auth.authorize_with_token(options.token) then
        return nil, 'Access denied'
    end

    local service = vars.service_list[name]
    if service == nil then
        return nil, ('Service %q does not exist'):format(name)
    end

    local _, err = check_service_access(service.type)
    if err ~= nil then
        return nil, err
    end

    return validate_and_call(service, args)
end

local function call_service(name, args, options)
    options = options or {}

    local context, err = request_context.parse_options(options)
    if err ~= nil then
        return nil, err
    end

    request_context.init(context)

    local result, err = call_service_impl(name, args, options)

    request_context.clear()

    return result, err
end

local function http_error(status_code, message)
    return {
        status = status_code,
        body = json.encode({error = message}),
    }
end

local function http_handler(req)
    local service_name = req:stash('service_name')

    local service = vars.service_list[service_name]
    if service == nil then
        return http_error(404, 'Service not found')
    end

    local _, err = check_service_access(service.type)
    if err ~= nil then
        return http_error(401, err)
    end

    local ok, data
    -- Just parse ALL query params.
    -- https://github.com/tarantool/http/blob/da1407c8e82dbdfd44abab8b1bb9860c217e7e22/http/server.lua#L137
    req:query_param('')
    if next(req.query_params) ~= nil then
        data = service.query_arguments_parser(req.query_params)
    else
        local raw_body = req:read()
        if raw_body ~= nil and #raw_body > 0 then
            ok, data = pcall(json.decode, raw_body)
            if not ok then
                return http_error(400, data)
            end
        else
            data = {}
        end
    end

    local result, err = validate_and_call(service, data)
    if err ~= nil then
        return http_error(400, err)
    end

    return {
        status = 200,
        body = json.encode({result = result}),
    }
end

local function check_argument_absence(arg)
    if arg == nil then
        return true
    elseif type(arg) == 'table' and next(arg) == nil then
        return true
    end
    return false, 'Unexpected argument'
end

local function create_field_validator(entry_type, type_map)
    entry_type = table.deepcopy(entry_type)

    local is_nullable = false
    if model.is_nullable_defined(entry_type) then
        is_nullable = true
        entry_type = entry_type[2]
    elseif entry_type.type ~= nil and model.is_nullable_defined(entry_type.type) then
        entry_type.type = entry_type.type[2]
        is_nullable = true
    end

    if model.is_array(entry_type) then
        local items_type = create_field_validator(entry_type.items, type_map)
        if items_type == nil then
            return nil, ('Type for %q is not defined'):format(entry_type.items)
        end
        entry_type.items = items_type
    elseif model.is_primitive_type(entry_type) == false then
        if type(entry_type) == 'string' then
            entry_type = type_map[entry_type]
            if entry_type == nil then
                return nil, ('Type for %q is not defined'):format(entry_type)
            end
        end
    end

    entry_type = table.deepcopy(entry_type)

    if is_nullable then
        if type(entry_type) == 'string' then
            entry_type = entry_type .. '*'
        elseif type(entry_type.type) == 'string' then
            entry_type.type = entry_type.type .. '*'
        end
    end

    return entry_type
end

local function create_validator(service_name, args, type_map)
    local schema = {
        type = 'record',
        name = service_name .. '_service_schema',
        fields = {},
    }

    if args == nil or next(args) == nil then
        return check_argument_absence
    end

    for name, entry_type in pairs(args) do
        local normalized_entry_type, err = create_field_validator(entry_type, type_map)
        if err ~= nil then
            return nil, err
        end

        table.insert(schema.fields, {name = name, type = normalized_entry_type})
    end

    local exported = avro_frontend.export_helper(schema)
    local ok, handle = pcall(avro_frontend.create_schema, exported, {
        preserve_in_ast = {'logicalType'},
    })
    if ok ~= true then
        return nil, handle
    end

    return function(data)
        data = data ~= nil and data or {}
        return model_validation.validate_data(handle, data)
    end
end

local function apply_config(mdl, type_map, services_cfg)
    checks('table', 'table', '?table')

    cleanup(vars.service_list)
    vars.service_list = {}

    local service_list = {}
    if services_cfg ~= nil then
        local model_types_map = {}
        for _, record in ipairs(mdl) do
            model_types_map[record.name] = record
        end

        for name, service in pairs(services_cfg) do
            service = table.deepcopy(service)
            cartridge_utils.table_setrw(service)
            if service.type == nil then
                service.type = 'query'
            end

            local res, err = add_service(name, service, mdl, type_map)
            if res == nil then
                cleanup(service_list)
                return nil, err
            end

            local handle, err = create_validator(name, service.args, model_types_map)
            if err ~= nil then
                cleanup(service_list)
                return nil, err
            end
            service.validator = handle

            service_list[name] = service
        end
    end
    vars.service_list = service_list

    rawset(_G, 'call_service', call_service)

    local httpd = cartridge.service_get('httpd')
    httpd:route(
        { path = '/service/:service_name', method = 'POST', public = false },
        http_handler
    )

    return true
end

return {
    apply_config = apply_config,
    config = config,

    -- for tests
    create_validator = create_validator,
}
