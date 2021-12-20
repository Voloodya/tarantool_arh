local uuid = require('uuid')

return {
    call = function(param)
        -- If agent object
        if param.obj.agent ~= nil then
            -- Some conversions from input to storage format
            param.obj = param.obj.agent
            param.obj.name = param.obj.first_name .. ' ' .. param.obj.last_name
            param.obj.first_name = nil
            param.obj.last_name = nil

            -- New router key for storage routing
            param.routing_key = "agent_key"
        end

        -- If estate object
        if param.obj.estate ~= nil then
            -- Some conversions from input to storage format
            param.obj = param.obj.estate
            param.obj.address = {
                street = param.obj.street,
                building = param.obj.building,
                district = param.obj.district,
            }
            param.obj.street = nil
            param.obj.building = nil
            param.obj.district = nil

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
