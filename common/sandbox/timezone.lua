#!/usr/bin/env tarantool

local checks = require('checks')
local clock = require('clock')
local errors = require('errors')

local wrong_argument_error = errors.new_class("wrong_argument_error")

local icu_date = require('icu-date')
local icu_date_instance = icu_date.new()

local function get_icu_date_instance(timezone_id)
    checks("string")

    icu_date_instance:set_time_zone_id(timezone_id)
    if icu_date_instance:get_time_zone_id() ~= timezone_id then
        return nil, wrong_argument_error:new("Unknown time zone id: '%s'", timezone_id)
    end
    return icu_date_instance
end

local NSEC_IN_MILLISEC = 1000000LL
local MILLISEC_IN_SEC = 1000

-- returns current local time in nanoseconds for time zone <timezone_id>
local function now(timezone_id)
    local date, err = get_icu_date_instance(timezone_id)
    if date == nil then
        return nil, err
    end

    return clock.time64() + date:get(icu_date.fields.ZONE_OFFSET) * NSEC_IN_MILLISEC
end

-- returns number of seconds for time zone <timezone_id>, elapsed from the start of the current day
local function seconds_since_midnight(timezone_id)
    local date, err = get_icu_date_instance(timezone_id)
    if date == nil then
        return nil, err
    end

    date:set_millis(date:now())
    return math.floor(date:get(icu_date.fields.MILLISECONDS_IN_DAY) / MILLISEC_IN_SEC)
end

-- Returns the start of the current local date (zero hours, minutes, seconds, etc.) for the time zone <timezone_id>.
-- The result is in nanoseconds
local function curr_date_nsec(timezone_id)
    local date, err = get_icu_date_instance(timezone_id)
    if date == nil then
        return nil, err
    end

    date:set_millis(date:now())
    local year = date:get(icu_date.fields.YEAR)
    local month = date:get(icu_date.fields.MONTH)
    local day_of_month = date:get(icu_date.fields.DAY_OF_MONTH)

    date:set_millis(0)
    date:set_time_zone_id('UTC')
    date:set(icu_date.fields.YEAR, year)
    date:set(icu_date.fields.MONTH, month)
    date:set(icu_date.fields.DAY_OF_MONTH, day_of_month)

    return date:get_millis() * NSEC_IN_MILLISEC
end

return {
    now = now,
    seconds_since_midnight = seconds_since_midnight,
    curr_date_nsec = curr_date_nsec
}
