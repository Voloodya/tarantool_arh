return {
    call = function(estate, arg)
        local district = estate["address.district"]

        -- If we need filter by district then arg.filter_district will be not nil
        if arg.filter_district ~= nil and district ~= arg.filter_district then
            return nil
        end

        -- Make map from one value
        return {
            [district] = {
                count = 1,
                sum = estate.price,
                min = estate.price,
                max = estate.price
            }
        }
    end
}
