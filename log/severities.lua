local checks = require('checks')
local errors = require('errors')

local common_log_severities_error = errors.new_class('common_log_severities_error')

local INVALID_VALUE = -1
local ERROR = 2
local CRITICAL = 3
local WARNING = 4
local INFO = 5
local VERBOSE = 6
local DEBUG = 7

local function from_string(value)
    value = string.lower(value)
    if value == 'verbose' then
        return VERBOSE
    elseif value == 'info' then
        return INFO
    elseif value == 'warning' then
        return WARNING
    elseif value == 'debug' then
        return DEBUG
    elseif value == 'error' then
        return ERROR
    elseif value == 'critical' then
        return CRITICAL
    end

    return INVALID_VALUE
end

local function to_string(value)
    if value == VERBOSE then
        return 'verbose'
    elseif value == INFO then
        return 'info'
    elseif value == WARNING then
        return 'warning'
    elseif value == DEBUG then
        return 'debug'
    elseif value == ERROR then
        return 'error'
    elseif value == CRITICAL then
        return 'critical'
    end
    return nil, common_log_severities_error:new('Invalid common log severity %q', value)
end

local function is_valid_value(value)
    checks('number')

    return ERROR <= value and value <= DEBUG
end

local function is_valid_string_value(value)
    local numeric = from_string(value)
    return is_valid_value(numeric)
end

return {
    ERROR = ERROR,
    CRITICAL = CRITICAL,
    WARNING = WARNING,
    INFO = INFO,
    VERBOSE = VERBOSE,
    DEBUG = DEBUG,

    is_valid_value = is_valid_value,
    is_valid_string_value = is_valid_string_value,

    from_string = from_string,
    to_string = to_string,
}
