local log = require('log')
local repository = require('repository')

return {
    call = function(param)
        local district = param and param.district

        -- Run map reduce for estates
        local opts = {
            map_args = { { filter_district = district } }, -- filter_district = name of district by which to filter
            combine_initial_state = {},
            reduce_initial_state = {},
        }
        local statistics, err = repository.map_reduce('Estate', {},
            'districts_stat.map.call',
            'districts_stat.combine_reduce.call',
            'districts_stat.combine_reduce.call',
        opts)

        -- If some error then return it
        if err ~= nil then
            return err
        end

        -- Round to 2 numbers
        local function round_2(x)
            return math.floor(x * 100 + 0.5) / 100
        end

        -- Save statistics in DB
        for name, stat in pairs(statistics) do
            -- Count average price
            local avg_price = 0
            if stat.count ~= 0 then
                avg_price = stat.sum / stat.count
                avg_price = round_2(avg_price)
            end

            -- Statistic for district
            local districtStat = {
                district = name,
                count = stat.count,
                avg_price = avg_price,
                min_price = stat.min,
                max_price = stat.max
            }

            -- Save result in DB
            repository.put('DistrictStat', districtStat)
        end

        if district == nil then
            log.info("Statistics for all districts calculated")
        else
            log.info("Statistics for district '" .. district .. "' calculated")
        end
        return "ok"
    end
}
