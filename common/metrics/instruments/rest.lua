local metrics = require('metrics')

local time_buckets = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

local query_time = metrics.histogram(
    "tdg_rest_exec_time",
    "REST query execution time",
    time_buckets
)

-- All returning response codes from default http handler
local query_results = {
    [200] = metrics.counter(
        'tdg_rest_result_200',
        'Total successful REST queries'
    ),
    [400] = metrics.counter(
        'tdg_rest_result_400',
        'Total failed REST queries'
    ),
    [404] = metrics.counter(
        'tdg_rest_result_404',
        'Total "Not Found" REST queries'
    ),
}

local function update_metrics(response_code, exec_time, labels)
    local query_result = query_results[response_code]
    if query_result ~= nil then
        query_result:inc(1, labels)
    end

    if response_code == 200 then
        query_time:observe(exec_time, labels)
    end
end

return {
    update_metrics = update_metrics
}
