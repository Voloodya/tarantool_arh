local uuid = require('uuid')

return {
    call = function(param)
        -- Only agent has a phone
        if param.obj.phone ~= nil then
            -- Some conversions from input to storage format
            param.obj.home_id = nil

            -- New router key for storage routing
            param.routing_key = "agent_key"
        end

        -- Only estate has a square
        if param.obj.price ~= nil then
            -- Some conversions from input to storage format
            param.obj.home_id = nil

            -- New router key for storage routing
            param.routing_key = "estate_key"
        end

        -- Generate uuid if not exists
        if param.obj.uuid == nil then
            param.obj.uuid = uuid.str()
        end

        return param
    end
}
