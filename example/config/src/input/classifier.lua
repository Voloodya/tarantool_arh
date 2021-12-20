return {
    call = function(param)
        -- param is an object from previous step (in this case it's connector)
        -- If data from Estate Inc, then it contains field "company" with the value "estate inc"
        if param.obj.company == "estate inc" then
            param.routing_key = "estate_inc_key"
            return param
        end

        -- If data from Favihome.Com, then it contains field "home_id"
        if param.obj.home_id ~= nil then
            param.routing_key = "favihome_com_key"
            return param
        end

        -- If someone want to subscribe
        if param.obj.type == "subscribe" then
            param.routing_key = "subscriber_key"
            return param
        end

        -- If some strange data
        param.routing_key = "unknown_input"
        return param
    end
}
