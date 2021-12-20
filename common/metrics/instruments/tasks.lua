local metrics = require('metrics')
local execution_policy = require('tasks.runner.execution_policy')

local percentils = { 0.0001, 0.00025, 0.0005,
                     0.001,  0.0025,  0.005,
                     0.01,   0.025,   0.05,
                     0.1,    0.25,    0.5,
                     1,      2.5,     5 }

local counters = {
    -- JOBS metrics
    [execution_policy.JOB] = {
        tasks_started = metrics.counter(
            'tdg_jobs_started',
            'Total jobs started'
        ),
        tasks_failed = metrics.counter(
            'tdg_jobs_failed',
            'Total jobs failed'
        ),
        tasks_succeeded = metrics.counter(
            'tdg_jobs_succeeded',
            'Total jobs succeeded'
        ),
        tasks_running = metrics.gauge(
            'tdg_jobs_running',
            'Currently running jobs'
        ),
        tasks_execution_time = metrics.histogram(
            'tdg_jobs_execution_time',
            'Jobs execution time statistics',
            percentils
        )
    },
    -- TASK metrics
    [execution_policy.TASK] = {
        tasks_started = metrics.counter(
            'tdg_tasks_started',
            'Total tasks started'
        ),
        tasks_failed = metrics.counter(
            'tdg_tasks_failed',
            'Total tasks failed'
        ),
        tasks_succeeded = metrics.counter(
            'tdg_tasks_succeeded',
            'Total tasks succeeded'
        ),
        tasks_stopped = metrics.counter(
            'tdg_tasks_stopped',
            'Total tasks stopped'
        ),
        tasks_running = metrics.gauge(
            'tdg_tasks_running',
            'Currently running tasks'
        ),
        tasks_execution_time = metrics.histogram(
            'tdg_tasks_execution_time',
            'Tasks execution time statistics',
            percentils
        )
    },
    -- SYSTEM metrics
    [execution_policy.SYSTEM] = {
        tasks_started = metrics.counter(
            'tdg_system_tasks_started',
            'Total system tasks started'
        ),
        tasks_succeeded = metrics.counter(
            'tdg_system_tasks_succeeded',
            'Total system tasks succeeded'
        ),
        tasks_failed = metrics.counter(
            'tdg_system_tasks_failed',
            'Total system tasks failed'
        ),
        tasks_running = metrics.gauge(
            'tdg_system_tasks_running',
            'Currently running system tasks'
        ),
        tasks_execution_time = metrics.histogram(
            'tdg_system_tasks_execution_time',
            'System tasks execution time statistics',
            percentils
        )
    }
}

-- Internal methods
local function get_metrics_group(policy)
    local metrics_group = counters[policy]
    if metrics_group == nil then
        error('Invalid policy given')
    end
    return metrics_group
end

local function change_running(metrics_group, labels, value)
    metrics_group.tasks_running:inc(value, labels)
end

local function update_exec_time(metrics_group, labels, exec_time)
    metrics_group.tasks_execution_time:observe(exec_time, labels)
end

-- API methods
local function start(labels, policy)
    local metrics_group = get_metrics_group(policy)
    change_running(metrics_group, labels, 1)
    metrics_group.tasks_started:inc(1, labels)
end

local function succeed(labels, policy, exec_time)
    local metrics_group = get_metrics_group(policy)
    change_running(metrics_group, labels, -1)
    update_exec_time(metrics_group, labels, exec_time)
    metrics_group.tasks_succeeded:inc(1, labels)
end

local function fail(labels, policy, exec_time)
    local metrics_group = get_metrics_group(policy)
    change_running(metrics_group, labels, -1)
    update_exec_time(metrics_group, labels, exec_time)
    metrics_group.tasks_failed:inc(1, labels)
end

local function stop(labels)
    local metrics_group = counters[execution_policy.TASK]
    change_running(metrics_group, labels, -1)
    metrics_group.tasks_stopped:inc(1, labels)
end

return {
    start = start,
    succeed = succeed,
    fail = fail,
    stop = stop,
}
