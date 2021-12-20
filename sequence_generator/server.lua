local module_name = 'sequence_generator.server'

local errors = require('errors')
local config_error = errors.new_class('invalid sequence generator config')
local get_range_error = errors.new_class('get range of sequence error')
local config_checks = require('common.config_checks').new(config_error)

local checks = require('checks')
local defaults = require('common.defaults')
local tenant = require('common.tenant')
local vars = require('common.vars').new(module_name)

local BASE_SPACE_NAME = 'tdg_sequences'

local FIELDS = {
    { name = 'name', type = 'string' },
    { name = 'next_id', type = 'unsigned' },
}

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_space()
    local space_name = get_space_name()
    return box.space[space_name]
end

local NAME_FIELD = 1
local NEXT_ID_FIELD = 2

local SEQUENCE_MAX_VALUE = 0xFFFFFFFFFFFFFFFFULL

vars:new('starts_with', defaults.SEQUENCE_STARTS_WITH)
vars:new('range_width', defaults.SEQUENCE_RANGE_WIDTH)

local function create_space()
    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        return box.space[space_name]
    end

    box.begin()
    local space = box.schema.space.create(space_name, { if_not_exists = true })
    space:format(FIELDS)
    space:create_index('name', { parts = { NAME_FIELD, 'string' }, type = 'HASH', unique = true, if_not_exists = true })
    box.commit()

    return space
end

local function validate_config(cfg)
    checks('table')

    local conf = cfg['sequence_generator']
    if not conf then
        return true
    end

    if conf.starts_with ~= nil then
        config_checks:check_luatype('config.starts_with', conf.starts_with, 'number')
        config_error:assert(conf.starts_with >= 0, 'starts_with must be not negative')
    end

    if conf.range_width ~= nil then
        config_checks:check_luatype('config.range_width', conf.range_width, 'number')
        config_error:assert(conf.range_width >= 1, 'range_width must be greater than 0')
    end

    return true
end

local function apply_config(cfg)
    checks('table')

    create_space()

    local conf = cfg.sequence_generator or {}

    vars.starts_with = conf.starts_with or defaults.SEQUENCE_STARTS_WITH
    vars.range_width = conf.range_width or defaults.SEQUENCE_RANGE_WIDTH
end

local function get_range(name, opts)
    checks('string', {
        starts_with = '?number|cdata',
        range_width = '?number|cdata',
    })
    opts = opts or {}

    local starts_with = opts.starts_with or vars.starts_with
    if starts_with < 0 then
        return nil, get_range_error:new('starts_with = %s must be not negative', starts_with)
    end

    local range_width = opts.range_width or vars.range_width
    if range_width < 1 then
        return nil, get_range_error:new('range_width = %s must be greater than 0', range_width)
    end

    -- Difference between begin and end of range
    local diff = range_width - 1

    local s = get_space()

    local first
    local seq = s:get(name)

    -- do not update the record, because we need to check for overflow
    if seq == nil then
        first = starts_with
    else
        first = seq[NEXT_ID_FIELD]
    end

    if first > SEQUENCE_MAX_VALUE - range_width then
        return nil, get_range_error:new('overflow of current sequence value')
    end

    s:upsert({ name, starts_with + range_width }, { { '+', NEXT_ID_FIELD, range_width } })

    local last = first + diff
    return { first, last }
end

return {
    validate_config = function(...)
        return config_error:pcall(validate_config, ...)
    end,
    apply_config = apply_config,

    get_range = get_range,
}
