#!/usr/bin/env tarantool

local utils = require('common.utils')
local clock = require('clock')

local icu_date = require('icu-date')
local icu_date_instance = icu_date.new()

local function get_icu_date_instance()
    icu_date_instance:set_time_zone_id('UTC')
    return icu_date_instance
end

local NSEC_IN_SEC = 1000000000LL
local NSEC_IN_MILLISEC = 1000000LL
local NSEC_IN_DAY = 86400 * NSEC_IN_SEC
local MILLISEC_IN_SEC = 1000

-- converts seconds to a string 'yyyy-MM-dd'
local function sec_to_iso_8601_date(sec)
    return utils.nsec_to_date_str(sec * NSEC_IN_SEC)
end

-- converts nanoseconds to a string 'yyyy-MM-ddTHH:mm:ss.SSSZ'
local function nsec_to_iso_8601_datetime(nsec)
    return utils.nsec_to_iso8601_str(nsec)
end

-- converts nanoseconds to a string 'yyyy-MM-dd'
local function nsec_to_iso_8601_date(nsec)
    return utils.nsec_to_date_str(nsec)
end

-- converts a date-time string to nanoseconds
local function iso_8601_datetime_to_nsec(iso_8601_datetime)
    return utils.iso8601_str_to_nsec(iso_8601_datetime)
end

-- converts a date string to nanoseconds
local function iso_8601_date_to_nsec(iso_8601_date)
    return utils.date_str_to_nsec(iso_8601_date)
end

-- converts a date or date-time string in a custom format to nanoseconds
local function custom_datetime_str_to_nsec(date_str, format_str)
    return utils.custom_datetime_str_to_nsec(date_str, format_str)
end

-- converts nanoseconds to seconds and casts result (cdata) as number type, useful for further math calculations
local function to_sec(nsec)
    return tonumber(nsec / NSEC_IN_SEC)
end

-- converts nanoseconds to milliseconds and casts result (cdata) as number type, useful for further math calculations
local function to_millisec(nsec)
    return tonumber(nsec / NSEC_IN_MILLISEC)
end

-- returns number of seconds in GMT, elapsed from the start of the current day
local function seconds_since_midnight()
    local date = get_icu_date_instance()
    date:set_millis(date:now())
    return math.floor(date:get(icu_date.fields.MILLISECONDS_IN_DAY) / MILLISEC_IN_SEC)
end

-- returns current gmt time in nanoseconds
local function now()
    return clock.time64()
end

-- Returns the start of the current UTC date (zero hours, minutes, seconds, etc.)
-- The result is in nanoseconds
local function curr_date_nsec()
    local date = get_icu_date_instance()

    date:set_millis(date:now())
    local year = date:get(icu_date.fields.YEAR)
    local month = date:get(icu_date.fields.MONTH)
    local day_of_month = date:get(icu_date.fields.DAY_OF_MONTH)

    date:set_millis(0)
    date:set(icu_date.fields.YEAR, year)
    date:set(icu_date.fields.MONTH, month)
    date:set(icu_date.fields.DAY_OF_MONTH, day_of_month)

    return date:get_millis() * NSEC_IN_MILLISEC
end

-- returns day of week of a given date-time (nsec) as a number in range 1-7, where 1 - sunday, 7 - saturday
local function nsec_to_day_of_week(nsec)
    return utils.nsec_to_day_of_week(nsec)
end

-- converts given iso 8601 time string to nanoseconds. Supported formats: 'HH:mm:ss.SSS', 'HH:mm:ss'
local function iso_8601_time_to_nsec(iso_8601_time)
    return utils.time_str_to_nsec(iso_8601_time)
end

-- converts given date-time in nanoseconds (nsec) to the time string in format 'HH:mm:ss.SSS'
local function nsec_to_iso_8601_time(nsec)
    return utils.nsec_to_time_str(nsec)
end

-- converts given day of week string in iso 8601 format (e.g., "Sunday", "Sun", "Su")
--  to a number in range 1-7, where 1 - sunday, 7 - saturday
local function iso_8601_day_of_week_to_number(iso_8601_day_of_week)
    return utils.day_of_week_str_to_number(iso_8601_day_of_week)
end

-- converts given timestamp to a datetime string formatted with the datetime_format_str template
local function millisec_to_formatted_datetime(datetime_millisec, datetime_format_str)
    local date = get_icu_date_instance()
    date:set_millis(datetime_millisec)
    local pattern, err = icu_date.formats.pattern(datetime_format_str)
    if pattern == nil then
        return nil, err
    end
    return date:format(pattern)
end

return {
    sec_to_iso_8601_date = sec_to_iso_8601_date,
    nsec_to_iso_8601_datetime = nsec_to_iso_8601_datetime,
    nsec_to_iso_8601_date = nsec_to_iso_8601_date,
    iso_8601_datetime_to_nsec = iso_8601_datetime_to_nsec,
    iso_8601_date_to_nsec = iso_8601_date_to_nsec,
    custom_datetime_str_to_nsec = custom_datetime_str_to_nsec,
    to_sec = to_sec,
    to_millisec = to_millisec,
    seconds_since_midnight = seconds_since_midnight,
    now = now,
    curr_date_nsec = curr_date_nsec,
    NSEC_IN_SEC = NSEC_IN_SEC,
    NSEC_IN_MILLISEC = NSEC_IN_MILLISEC,
    NSEC_IN_DAY = NSEC_IN_DAY,
    nsec_to_day_of_week = nsec_to_day_of_week,
    iso_8601_time_to_nsec = iso_8601_time_to_nsec,
    nsec_to_iso_8601_time = nsec_to_iso_8601_time,
    iso_8601_day_of_week_to_number = iso_8601_day_of_week_to_number,
    millisec_to_formatted_datetime = millisec_to_formatted_datetime,
}
