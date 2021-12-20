local classifier = require('input.classifier')
local estate_inc_handler = require('input.estate_inc_handler')
local favihome_com_handler = require('input.favihome_com_handler')
local subscribe_handler = require('input.subscribe_handler')

local DEFAULT_HANDLER = function(req) return req end

local function call(req)
    req = classifier.call(req)

    local handler = DEFAULT_HANDLER
    if req.routing_key == 'estate_inc_key' then
        handler = estate_inc_handler.call
    elseif req.routing_key == 'favihome_com_key' then
        handler = favihome_com_handler.call
    elseif req.routing_key == 'subscriber_key' then
        handler = subscribe_handler.call
    end

    return handler(req)
end

return {
    call = call,
}
