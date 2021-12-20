local checks = require('checks')
local metrics = require('metrics')

local processed_count = metrics.counter(
    'tdg_connector_input_file_processed_count',
    'Total files processed'
)

local processed_objects = metrics.counter(
    'tdg_connector_input_file_processed_objects_count',
    'Total objects processed'
)

local failed_count = metrics.counter(
    'tdg_connector_input_file_failed_count',
    'Total files failed to process'
)

local path = metrics.gauge(
    'tdg_connector_input_file_path',
    'File watching'
)

local format = metrics.gauge(
    'tdg_connector_input_file_format',
    'File watching'
)

local function increment_counters(objects, is_failed)
    processed_count:inc()
    failed_count:inc(is_failed and 1 or 0)
    processed_objects:inc(objects)
end

local function success(opts)
    checks({ objects_processed = 'number' })
    increment_counters(opts.objects_processed)
end

local function fail(opts)
    checks({ objects_processed = 'number' })
    increment_counters(opts.objects_processed, true)
end

local function update(opts)
    checks({ path = 'string', format = 'string' })
    path:set(opts.path)
    format:set(opts.format)
end

return {
    success = success,
    fail = fail,
    update = update,
}
