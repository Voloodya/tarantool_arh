local module_name = 'connector.filewatcher'

local checks = require('checks')
local fiber = require('fiber')
local fio = require('fio')
local request_context = require('common.request_context')
local errors = require('errors')
local auth = require('common.admin.auth')
local vars = require('common.vars').new(module_name)
local log = require('log.log').new(module_name)
local task = require('common.task')
local csv = require('connector.iterators.csv')
local jsonl = require('connector.iterators.jsonl')
local metrics = require("common.metrics.instruments.file_connector")
local tenant = require('common.tenant')

local WATCHER_TIMEOUT = 60
local OBJECTS_BUFFER_SIZE = 2048
local IMPORT_WORKERS_COUNT = 8
local DEFAULT_CHUNK_SIZE = 4096
local DEFAULT_DELAY = 0 -- seconds
local DEFAULT_LOCK_TIMEOUT = 5 * 60 -- seconds
local DEFAULT_LOCK_UPDATE_PERIOD = 60 -- seconds

local ITERATORS = {
    jsonl = jsonl,
    csv = csv,
}

local e_filewatcher = errors.new_class('file_connector_watcher_error')
local e_check = errors.new_class('file_connector_check_error')
local e_lock = errors.new_class('file_connector_lock_error')
local e_import = errors.new_class('file_connector_import_error')
local e_replicate = errors.new_class('file_connector_replicate_error')

vars:new('server')
vars:new('communication_ch')
vars:new('watchers')
vars:new('tasks')
vars:new('delay', DEFAULT_DELAY) -- how long to wait between records
vars:new('lock_timeout', DEFAULT_LOCK_TIMEOUT) -- lock becomes expired if timeout reached
vars:new('lock_update_period', DEFAULT_LOCK_UPDATE_PERIOD) -- update atime period
vars:new('is_initialized')

-- Retries are not supported yet
local function extract_attempt(_)
    return 1
end

local function assert_cancel()
    local ok = pcall(fiber.testcancel)
    if not ok then
        log.warn('Fiber "%s" has been canceled. Exiting...', fiber.self():name())
        fiber.testcancel()
    end
end

local function log_if_error(fn, ...)
    local ok, err = fn(...)
    if not ok then
        log.error(e_filewatcher:new(err))
    end
end

local function lock_import(filename)
    local lockname = filename..'.lock'
    local stat = fio.stat(filename)
    if stat then
        local is_staled = (fiber.time() - stat.atime) > vars.lock_timeout
        if is_staled then
            log.warn('Staled lock "%s" found while acquiring. Removing it and proceed...', lockname)
            fio.rmdir(lockname)
        end
    end

    local fh, err = fio.mkdir(lockname)

    if not fh then
        return nil, e_lock:new(err)
    end
    return true
end

local function unlock_import(filename)
    return fio.rmdir(filename..'.lock')
end

--- Update utime periodically while file exists
local function lock_updater(lockname, period)
    while fio.path.exists(lockname) do
        fio.utime(lockname)
        fiber.sleep(period)
        fiber.testcancel()
    end
end

local function replicate(from, to, offset)
    offset = offset or 1
    checks('string', 'string', 'number')

    local src, err = fio.open(from, {'O_RDONLY'})
    if not src then
        return nil, e_replicate:new('error opening "%s": %s', from, err)
    end
    local dst, err = fio.open(to, {'O_WRONLY', 'O_CREAT', 'O_TRUNC'}, tonumber('0644', 8))
    if not dst then
        log_if_error(src.close, src)
        return nil, e_replicate:new('error opening "%s": %s', to, err)
    end

    src:seek(offset - 1, 'SEEK_SET')
    local eof = false
    while not eof do
        local chunk = src:read(DEFAULT_CHUNK_SIZE)
        local chunk_size = #chunk
        if chunk_size < DEFAULT_CHUNK_SIZE then
            eof = true
        end

        local ok, err = dst:write(chunk)
        if not ok then
            log_if_error(src.close, src)
            log_if_error(dst.close, dst)
            return nil, e_replicate:new('error writing to "%s": %s', to, err)
        end
    end

    log_if_error(src.close, src)
    log_if_error(dst.close, dst)

    return true
end

local function write_to_file(filename, msg)
    local f, err = fio.open(filename, {'O_WRONLY', 'O_CREAT', 'O_TRUNC'},
        tonumber('0644', 8))
    if not f then
        return nil, e_import:new('error opening "%s": %s', filename, err)
    end

    local ok, err = f:write(msg)
    if not ok then
        log_if_error(f.close, f)
        return nil, e_import:new('error writing to "%s": %s', filename, err)
    end

    log_if_error(f.close, f)
    return true
end

-- This function stores error message in .1.error file and
-- replicates rest of unprocessed data in .1.data file
local function bury_attempt(filename, pos, errmsg)
    local attempt = extract_attempt(filename)
    local prefix = string.format('%s.%d', filename, attempt)

    local ok, err = replicate(filename, prefix..'.data', pos)
    if not ok then return nil, err end

    return write_to_file(prefix..'.error', errmsg)
end

local function graceful_fiber_stop(f)
    if type(f) == 'userdata' and f:status() ~= 'dead' then
        f:cancel() -- maybe add fiber.join()
    end
end

local function connector_handle_request(obj, routing_key, token_name, opts)
    local ctx = request_context.get()
    if token_name ~= nil and not auth.authorize_with_token_name(token_name) then
        request_context.set(ctx)
        return nil, e_import:new('Handling request with token %q failed: request is not authorised', token_name)
    end

    local rc, err = vars.server.handle_request(obj, routing_key, opts)
    request_context.set(ctx)
    return rc, err
end

local function handle_obj(obj, options)
    if options.is_async == false then
        return connector_handle_request(obj, options.routing_key, options.token_name, { is_async = false })
    end

    vars.communication_ch:put({
        obj = obj,
        routing_key = options.routing_key,
        token_name = options.token_name,
    })
end

local function import_internal(filename, options)
    checks('string', 'table')

    local fh, err = fio.open(filename, {'O_RDONLY'})
    if not fh then
        return nil, e_import:new('fio.open error: %s', err)
    end

    local delay = vars.delay
    local format = options.format
    local it = ITERATORS[format].new(fh)
    for _, obj in it.call, it.state do
        _, err = handle_obj(obj, options)
        if err ~= nil then
            log.error('Import of %q failed: %s', filename, err)
            break
        end
        if not pcall(fiber.sleep, delay) then
            err = e_import:new('import has been interrupted')
            break
        end
    end

    log_if_error(fh.close, fh)

    if it.state.err ~= nil or err ~= nil then
        metrics.fail({objects_processed = it.state.count})
        log_if_error(bury_attempt, filename, it.state.offset, it.state.err)
        return nil, e_import:new('error importing "%s": %s', filename, it.state.err or err)
    end

    metrics.success({objects_processed = it.state.count})

    return true
end

local function import(filename, options)
    -- Do not remove file if open failed
    local ok, err = lock_import(filename)
    if not ok then
        return nil, e_import:new('failed to lock import for "%s": %s', filename, err)
    end
    tenant.fiber_new(lock_updater, filename, vars.lock_update_period)

    local fh, err = fio.open(filename, {'O_RDONLY'})
    if not fh then
        local errfname = ('%s.%d.error'):format(filename, extract_attempt(filename))
        err = e_import:new('fio.open error: %s', err)
        log_if_error(write_to_file, errfname, err.str)
        log_if_error(unlock_import, filename)
        return nil, e_import:new('fio.open error: %s', err)
    end
    fh:close()

    ok, err = e_import:pcall(import_internal, filename, options)

    log_if_error(fio.unlink, filename)
    log_if_error(unlock_import, filename)

    return ok, err
end

local function watcher_fn(filename, options)
    log.info('Starting a watcher for "%s"...', filename)
    local self = fiber.self()
    self.storage.check = self.storage.check or fiber.cond()
    self.storage.notify = self.storage.notify or fiber.cond()
    filename = fio.abspath(filename)

    while true do
        if fio.path.is_file(filename) then
            log.info('Importing file "%s" with format "%s"', filename, options.format)

            local ok, err = e_import:pcall(import, filename, options)
            if ok then
                log.info('Import of "%s" has been finished', filename)
            else
                log.error('Import of "%s" failed: %s', filename, err)
            end
            self.storage.notify:broadcast()
            assert_cancel() -- import may be interrupted by fiber.cancel
        end

        self.storage.check:wait(WATCHER_TIMEOUT)
        assert_cancel()
    end
end

local function handle_obj_async(input_ch)
    if input_ch:is_closed() then return end
    local data = input_ch:get()
    local rc, err = connector_handle_request(data.obj, data.routing_key, data.token_name, { is_async = true })
    if not rc then
        log.error('Error handling object: %s', err)
    end
end

local function init()
    vars.watchers = vars.watchers or {}
    vars.tasks = vars.tasks or {}
    vars.server = require('connector.server')
end

local function setup(input)
    if vars.is_initialized == nil then
        log.info('Initializing a file connector...')
        vars.communication_ch = vars.communication_ch or fiber.channel(OBJECTS_BUFFER_SIZE)

        log.info('Setting up import workers')
        for _ = 1,IMPORT_WORKERS_COUNT,1 do
            local task_id = task.start('connector.filewatcher', 'handle_obj_async',
                { interval = 0 }, vars.communication_ch)
            table.insert(vars.tasks, task_id)
        end
        log.info('File connector has been successfully initialized')
        vars.is_initialized = true
    end

    if vars.watchers[input.name] == nil then
        log.info('Setting up input "%s"', input.name)

        local workdir = input.workdir or fio.cwd()
        local filename = fio.abspath(fio.pathjoin(workdir, input.filename))

        local f = tenant.fiber_new(watcher_fn, filename, input)
        f:name(input.filename..'_watcher', {truncate=true})

        metrics.update({ path = filename, format = input.format })

        vars.watchers[input.name] = f

        log.info('Setup for input "%s" done', input.name)
    end

    return true
end

local function cleanup(name)
    graceful_fiber_stop(vars.watchers[name])
    vars.watchers[name] = nil
    return true
end

local function stop()
    log.info('Stopping import workers...')
    for _, task_id in pairs(vars.tasks) do
        task.stop(task_id)
    end

    if vars.communication_ch ~= nil then
        vars.communication_ch:close()
        vars.communication_ch = nil
    end
    vars.is_initialized = nil
end

local function check(name)
    if #vars.tasks == 0 or vars.communication_ch == nil then
        return nil, e_check:new('Module is not yet initialized')
    end

    local checked = false

    for n, f in pairs(vars.watchers) do
        if not name or name == n then
            checked = true
            f.storage.check:signal()
        end
    end

    return checked
end

return {
    handle_obj_async = handle_obj_async,
    import = import,
    replicate = replicate,
    init = init,
    stop = stop,
    setup = setup,
    cleanup = cleanup,
    check = check,
}
