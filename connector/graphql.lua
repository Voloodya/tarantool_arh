local cartridge = require('cartridge')
local types = require('graphql.types')
local connector_common = require('connector.common')

local graphql = require('common.graphql')

local function handle_http_request(_, args)
    local response, err = cartridge.rpc_call('connector', 'handle_http_request',
        { args.object, {is_async = args.is_async} })
    if err ~= nil then
        return nil, err
    end
    if not connector_common.is_success_status_code(response.status) then
        return nil, response.body
    end
    return response.body
end

local function handle_soap_request(_, args)
    local response, err = cartridge.rpc_call('connector', 'handle_soap_request',
        { args.object, {is_async = args.is_async} })
    if not response then
        return nil, err
    end
    if err ~= nil or (not connector_common.is_success_status_code(response.status)) then
        return nil, response.body
    end
    return response.body
end

local function init()
    graphql.add_mutation_prefix('admin', 'connector', 'Connector')

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'connector',
        name = 'http_request',
        doc = 'Send object to HTTP handler',
        args = {
            object = types.string.nonNull,
            is_async = types.boolean,
        },
        kind = types.string.nonNull,
        callback = 'connector.graphql.handle_http_request',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'connector',
        name = 'soap_request',
        doc = 'Send object to SOAP handler',
        args = {
            object = types.string.nonNull,
            is_async = types.boolean,
        },
        kind = types.string.nonNull,
        callback = 'connector.graphql.handle_soap_request',
    })
end

return {
    handle_http_request = handle_http_request,
    handle_soap_request = handle_soap_request,

    init = init,
}
