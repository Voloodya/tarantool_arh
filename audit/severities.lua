local errors = require('errors')

local audit_log_severities_error = errors.new_class('audit_log_severities_error')

local INVALID_VALUE = -1
local VERBOSE = 1
local INFO = 2
local WARN = 3
local ALARM = 4

local function from_string(value)
    value = string.upper(value)
    if value == 'VERBOSE' then
        return VERBOSE
    elseif value == 'INFO' then
        return INFO
    elseif value == 'WARNING' then
        return WARN
    elseif value == 'ALARM' then
        return ALARM
    end
    return INVALID_VALUE
end

local function to_string(value)
    if value == VERBOSE then
        return 'VERBOSE'
    elseif value == INFO then
        return 'INFO'
    elseif value == WARN then
        return 'WARNING'
    elseif value == ALARM then
        return 'ALARM'
    end
    return nil, audit_log_severities_error:new('Invalid audit log severity %q', value)
end

local function is_valid_value(value)
    if value < VERBOSE or value > ALARM then
        return false
    end
    return true
end

return {
    VERBOSE = VERBOSE,
    INFO = INFO,
    WARN = WARN,
    ALARM = ALARM,

    is_valid_value = is_valid_value,

    from_string = from_string,
    to_string = to_string
}
