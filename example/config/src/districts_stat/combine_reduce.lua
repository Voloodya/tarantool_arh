return {
    call = function(state, statistics)
        -- Put values from arg to state
        for district, stat in pairs(statistics) do
            -- If there are no values for selected district
            if state[district] == nil then
                state[district] = {
                    count = stat.count,
                    sum = stat.sum,
                    min = stat.min,
                    max = stat.max,
                }
            else
                -- Make a magic for counting
                state[district].count = state[district].count + stat.count
                state[district].sum = state[district].sum + stat.sum
                state[district].min = math.min(state[district].min, stat.min)
                state[district].max = math.max(state[district].max, stat.max)
            end
        end

        -- At result: state = state + arg
        return state
    end
}
