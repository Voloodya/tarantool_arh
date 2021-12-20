local module_name = 'storage.expiration.file'

local fio = require('fio')
local json = require('json')
local fiber = require('fiber')
local icu_date = require('icu-date')

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local model_accessor = require('common.model_accessor')
local errors = require('errors')

local model_accessor_error = errors.new_class('Model accessor error')

vars:new('config')
vars:new('tasks')

local function apply_config(cfg)
    vars.config = {}

    if vars.tasks == nil then
        vars.tasks = {}
    end

    -- FIXME: Remove expiration
    for _, record in pairs(cfg['versioning'] or cfg['expiration'] or {}) do
        if record.strategy == 'file' then
            vars.config[record.type] = record
        end
    end

    return true
end

local function check_file_size(file, size_limit)
    if size_limit == 0 then
        return true
    end
    return file.fd:stat().size < size_limit
end

local date_pattern = icu_date.formats.pattern("-yyyy-MM-dd'T'HH:mm:ss")
local open_flags = {'O_CREAT', 'O_WRONLY', 'O_APPEND'}
local open_mode = tonumber('644', 8)
local FILE_SIZE_LIMIT = 100 * 2^20

local function get_archive_name(type_name)
    local date = icu_date.new({locale = 'ru_RU'})
    local instance_name = box.cfg.custom_proc_title
    instance_name = instance_name and instance_name .. '-' or ''
    return instance_name .. type_name .. date:format(date_pattern) .. '.jsonl'
end

local function create_file(type_name, dir)
    fio.mktree(dir)

    local basename = get_archive_name(type_name)
    local filename = fio.pathjoin(dir, basename)
    local file, err = fio.open(filename, open_flags, open_mode)
    if err ~= nil then
        err = tostring(err) .. ': ' .. tostring(filename)
        return nil, err
    end

    return file
end

local function get_dumper(type_name, file, cfg)
    return function(tuple)
        local err
        if file.fd == nil or file.fd.fh < 0 then
            file.fd, err = create_file(type_name, cfg['dir'])
        elseif not check_file_size(file, cfg['file_size_threshold'] or FILE_SIZE_LIMIT) then
            file.fd:close()
            file.fd, err = create_file(type_name, cfg['dir'])
        end
        if err ~= nil then
            log.error('%s dump error: %s', type_name, err)
            return nil, err
        end
        file.fd:write(json.encode(tuple) .. '\n')
    end
end

local function start(type_name)
    if vars.tasks[type_name] ~= nil then
        log.warn('File expiration task is already running for %q', type_name)
        return
    end

    vars.tasks[type_name] = true
    fiber.self():name('data_expiration_' .. tostring(type_name), {truncate = true})

    local file = { fd = nil }
    local cfg = vars.config[type_name]

    local dump_callback = get_dumper(type_name, file, cfg)
    local _, err = model_accessor_error:pcall(model_accessor.run_expiration_task,
        type_name, 'file', dump_callback)
    vars.tasks[type_name] = nil

    if file.fd ~= nil then
        file.fd:close()
    end

    if err ~= nil then
        return nil, err
    end

    return true
end

return {
    start = start,
    apply_config = apply_config,
}
