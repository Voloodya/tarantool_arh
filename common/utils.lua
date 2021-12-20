local ffi = require('ffi') -- for iscdata
local fio = require('fio')
local fun = require('fun')
local errors = require('errors')
local errno = require('errno')
local checks = require('checks')
local icu_date = require('icu-date')
local icu_date_instance = icu_date.new()
local msgpack = require('msgpack')
local digest = require('digest')

local function get_icu_date_instance()
    icu_date_instance:set_time_zone_id('UTC')
    return icu_date_instance
end

local read_file_error_open = errors.new_class('read_file_err_open')
local read_file_error_read = errors.new_class('read_file_error_read')

local write_file_error_open = errors.new_class('write_file_err_open')
local write_file_error_write = errors.new_class('write_file_error_write')

local time_conversion_error = errors.new_class("time_conversion_error")

local function append_table(where, from)
    for _, item in pairs(from) do
        table.insert(where, item)
    end
    return where
end

local function get_table_keys(tbl)
    return fun.iter(tbl):totable()
end

local function merge_maps(...)
    local res = {}
    for i = 1, select('#', ...) do
        local t = select(i, ...)
        for k, v in pairs(t) do
            res[k] = v
        end
    end
    return res
end

local function cmpdeeply(got, expected, extra)
    if extra == nil then
        extra = {}
    end

    if type(expected) == "number" or type(got) == "number" then
        extra.got = got
        extra.expected = expected
        if got ~= got and expected ~= expected then
            return true -- nan
        end
        return got == expected
    end

    if ffi.istype('bool', got) then got = (got == 1) end
    if ffi.istype('bool', expected) then expected = (expected == 1) end

    if extra.strict and type(got) ~= type(expected) then
        extra.got = type(got)
        extra.expected = type(expected)
        return false
    end

    if type(got) ~= 'table' or type(expected) ~= 'table' then
        extra.got = got
        extra.expected = expected
        return got == expected
    end

    local path = extra.path or '/'
    local visited_keys = {}

    for i, v in pairs(got) do
        visited_keys[i] = true
        extra.path = path .. '/' .. i
        if not cmpdeeply(v, expected[i], extra) then
            return false
        end
    end

    -- check if expected contains more keys then got
    for i, v in pairs(expected) do
        if visited_keys[i] ~= true and (extra.strict or v ~= box.NULL) then
            extra.expected = 'key ' .. tostring(i)
            extra.got = 'nil'
            return false
        end
    end

    extra.path = path

    return true
end

local function sorted(array)
    local res = table.copy(array)
    table.sort(res)
    return res
end

local function arrays_equal(lhs, rhs)
    if #lhs ~= #rhs then
        return false
    end

    for i,_ in ipairs(lhs) do
        if lhs[i] ~= rhs[i] then
            return false
        end
    end

    return true
end

local function is_array(table, empty_table_is_not_array)
    if type(table) ~= 'table' then
        return false
    end

    local max = 0
    local count = 0
    for k, _ in pairs(table) do
        if type(k) == "number" then
            if k > max then max = k end
            count = count + 1
        else
            return false
        end
    end
    if max > count * 2 then
        return false
    end

    if count == 0 then
        if empty_table_is_not_array == true then
            return false
        end
    end

    return true
end

local function is_map(tbl)
    if type(tbl) ~= 'table' then
        return false
    end

    local key, _ = next(tbl)

    return type(key) == 'string'
end

local function to_array(tbl)
    local new_tbl = table.deepcopy(tbl)

    if is_map(new_tbl) then
        local new_value = {}

        for key, value in pairs(new_tbl) do
            if type(value) == 'table' then
                value = to_array(value)
            end
            table.insert(new_value, {key, value})
        end

        table.sort(new_value, function(l, r)
            return tostring(l[1]) < tostring(r[1])
        end)
        new_tbl = new_value
    else
        for key, value in pairs(new_tbl) do
            if type(value) == 'table' then
                new_tbl[key] = to_array(value)
            end
        end
    end

    return new_tbl
end

local function has_value(array, value)
    checks("?table", "?")
    if not array then
        return false
    end

    for _, v in ipairs(array) do
        if v == value then
            return true
        end
    end

    return false
end

local function table_count(table)
    checks("table")

    local cnt = 0
    for _, _ in pairs(table) do
        cnt = cnt + 1
    end
    return cnt
end

local function read_file(path)
    local file = fio.open(path)
    if file == nil then
        return nil, read_file_error_open:new('Failed to open file %s: %s', path, errno.strerror())
    end
    local buf = {}
    while true do
        local val = file:read(1024)
        if val == nil then
            return nil, read_file_error_read:new('Failed to read from file %s: %s', path, errno.strerror())
        elseif val == '' then
            break
        end
        table.insert(buf, val)
    end
    file:close()
    return table.concat(buf, '')
end

local function write_file(path, data)
    local file = fio.open(path, {'O_CREAT', 'O_WRONLY', 'O_TRUNC', 'O_SYNC'}, tonumber(644, 8))
    if file == nil then
        return nil, write_file_error_open:new('Failed to open file %s: %s', path, errno.strerror())
    end

    local res = file:write(data)

    if not res then
        return nil, write_file_error_write:new('Failed to write to file %s: %s', path, errno.strerror())
    end

    file:close()
    return data
end

local function to_milliseconds(nsec)
    return nsec / 1000000LL
end

local function to_nanoseconds(millis)
    return millis * 1000000LL
end

local function parse_datetime_str_to_nsec(datetime_str, format_list)
    checks("string", "table")
    local date = get_icu_date_instance()
    for _, format in ipairs(format_list) do
        local _, err = time_conversion_error:pcall(date.parse, date, format, datetime_str)
        if err == nil then
            local millis = date:get_millis()
            return to_nanoseconds(millis)
        end
    end
    return nil
end

local iso8601_str_to_nsec_formats = {
    assert(icu_date.formats.iso8601()),
    assert(icu_date.formats.pattern('yyyy-MM-dd\'T\'HH:mm:ss.SSSZZZZZ')),
    assert(icu_date.formats.pattern('yyyy-MM-dd\'T\'HH:mm:ssZZZZZ')),
    assert(icu_date.formats.pattern('yyyy-MM-dd\'T\'HH:mm:ss.SSS')),
    assert(icu_date.formats.pattern('yyyy-MM-dd\'T\'HH:mm:ss')),
    assert(icu_date.formats.pattern('yyyy-MM-dd\'T\'HH-mm-ss')),
    assert(icu_date.formats.pattern('yyyyMMdd\'T\'HHmmss.SZZZZZ')),
    assert(icu_date.formats.pattern('yyyyMMdd\'T\'HHmmssZZZZZ')),
    assert(icu_date.formats.pattern('yyyyMMdd\'T\'HHmmss.SSS')),
    assert(icu_date.formats.pattern('yyyyMMdd\'T\'HHmmss')),
    assert(icu_date.formats.pattern('yyyyMMdd-HH:mm:ss.SSS')),
}
local function iso8601_str_to_nsec(iso8601_str)
    checks("string")

    local res = parse_datetime_str_to_nsec(iso8601_str, iso8601_str_to_nsec_formats)
    if res == nil then
        return nil, time_conversion_error:new(
            "Failed to parse iso8601 date/time: %q", tostring(iso8601_str))
    end

    return res
end

local nsec_to_iso8601_str_format = assert(icu_date.formats.iso8601())
local function nsec_to_iso8601_str(nsec)
    checks("number|cdata")

    local millis = to_milliseconds(nsec)
    local date = get_icu_date_instance()
    date:set_millis(millis)
    return date:format(nsec_to_iso8601_str_format)
end

local date_str_to_nsec_formats = {
    assert(icu_date.formats.pattern('yyyy-MM-dd')),
    assert(icu_date.formats.pattern('yyyyMMdd')),
}
local function date_str_to_nsec(date_str)
    checks("string")

    local res = parse_datetime_str_to_nsec(date_str, date_str_to_nsec_formats)
    if res == nil then
        return nil, time_conversion_error:new(
            "Failed to parse iso8601 date: %q", tostring(date_str))
    end

    return res
end

local function custom_datetime_str_to_nsec(datetime_str, format_str)
    checks("string", "string")

    local format, err = icu_date.formats.pattern(format_str)
    if format == nil then
        return nil, time_conversion_error:new(
            "Failed to create format object for %q: %q",
            tostring(format_str), tostring(err))
    end

    local date = get_icu_date_instance()
    local _, err = time_conversion_error:pcall(date.parse, date, format, datetime_str)
    if err ~= nil then
        return nil, time_conversion_error:new(
            "Failed to parse date/time string %q in format %q",
            tostring(datetime_str), tostring(format_str))
    end

    local millis = date:get_millis()
    return to_nanoseconds(millis)
end

local nsec_to_date_str_format = assert(icu_date.formats.pattern('yyyy-MM-dd'))
local function nsec_to_date_str(nsec)
    checks("number|cdata")

    local millis = to_milliseconds(nsec)
    local date = get_icu_date_instance()
    date:set_millis(millis)
    return date:format(nsec_to_date_str_format)
end

local time_str_to_nsec_formats = {
    assert(icu_date.formats.pattern('HH:mm:ss.SSS')),
    assert(icu_date.formats.pattern('HH:mm:ss')),
}
local function time_str_to_nsec(time_str)
    checks("string")

    local res = parse_datetime_str_to_nsec(time_str, time_str_to_nsec_formats)
    if res == nil then
        return nil, time_conversion_error:new("Failed to parse time: %q", tostring(time_str))
    end

    return res
end

local nsec_to_time_str_format = assert(icu_date.formats.pattern("HH:mm:ss.SSS"))
local function nsec_to_time_str(nsec)
    checks("number|cdata")

    local millis = to_milliseconds(nsec)
    local date = get_icu_date_instance()
    date:set_millis(millis)
    return date:format(nsec_to_time_str_format)
end

local day_of_week_str_to_number_formats = {
    assert(icu_date.formats.pattern('E')),
    assert(icu_date.formats.pattern('EE')),
    assert(icu_date.formats.pattern('EEE')),
    assert(icu_date.formats.pattern('EEEE')),
    assert(icu_date.formats.pattern('EEEEE')),
    assert(icu_date.formats.pattern('EEEEEE')),
}
local function day_of_week_str_to_number(day_of_week_str)
    checks("string")

    local date = get_icu_date_instance()
    for _, format in ipairs(day_of_week_str_to_number_formats) do
        local _, err = time_conversion_error:pcall(date.parse, date, format, day_of_week_str)
        if err == nil then
            return date:get(icu_date.fields.DAY_OF_WEEK)
        end
    end

    return nil, time_conversion_error:new(
        "Failed to parse day of week: %q", tostring(day_of_week_str))
end

local function nsec_to_day_of_week(nsec)
    checks("number|cdata")
    local millis = to_milliseconds(nsec)
    local date = get_icu_date_instance()
    date:set_millis(millis)
    return date:get(icu_date.fields.DAY_OF_WEEK)
end

local function parse_unix_time(nsec)
    checks("number|cdata")
    local millis = to_milliseconds(nsec)
    local date = get_icu_date_instance()
    date:set_millis(millis)
    return {
        year = date:get(icu_date.fields.YEAR),
        month = date:get(icu_date.fields.MONTH) + 1, -- starts with 0
        day = date:get(icu_date.fields.DAY_OF_MONTH),
    }
end

local function is_unsigned(value)
    local value_type = type(value)
    if value_type == 'number' then
        return value >= 0 and value < 2^53 and math.floor(value) == value
    elseif value_type == 'cdata' then
        return ffi.istype('uint64_t', value)
    else
        return false
    end
end

local function is_email_valid(str)
    if str == nil then return nil end
    if (type(str) ~= 'string') then
        error("Expected string")
        return nil
    end

    if #str == 0 then
        return nil, 'Empty string is passed instead of email'
    end

    local lastAt = str:find("[^%@]+$")
    local localPart = str:sub(1, (lastAt - 2)) -- Returns the substring before '@' symbol
    local domainPart = str:sub(lastAt, #str) -- Returns the substring after '@' symbol
    -- we weren't able to split the email properly
    if localPart == nil then
        return nil, "Local name is invalid"
    end

    if domainPart == nil then
        return nil, "Domain is invalid"
    end
    -- local part is maxed at 64 characters
    if #localPart > 64 then
        return nil, "Local name must be less than 64 characters"
    end
    -- domains are maxed at 253 characters
    if #domainPart > 253 then
        return nil, "Domain must be less than 253 characters"
    end
    -- something is wrong
    if lastAt >= 65 then
        return nil, "Invalid @ symbol usage"
    end
    -- quotes are only allowed at the beginning of a the local name
    local quotes = localPart:find("[\"]")
    if type(quotes) == 'number' and quotes > 1 then
        return nil, "Invalid usage of quotes"
    end
    -- no @ symbols allowed outside quotes
    if localPart:find("%@+") and quotes == nil then
        return nil, "Invalid @ symbol usage in local part"
    end
    -- no dot found in domain name
    if not domainPart:find("%.") then
        return nil, "No TLD found in domain"
    end
    -- only 1 period in succession allowed
    if domainPart:find("%.%.") then
        return nil, "Too many periods in domain"
    end
    if localPart:find("%.%.") then
        return nil, "Too many periods in local part"
    end
    -- just a general match
    if not str:match('^[%w\'._+-]+%@[%w%.-]+%.%a+$') then
        return nil, "Email pattern test failed"
    end
    -- all our tests passed, so we are ok
    return true
end

local function reverse_table(t)
    local len = #t
    for i = 1, len / 2 do
        t[i], t[len - i + 1] = t[len - i + 1], t[i]
    end
    return t
end

local function tree(root, opts)
    opts = opts or {}
    local level = opts.level or -1
    local prefix = opts.prefix or ''
    local nodes = fio.listdir(fio.pathjoin(root, prefix))
    if #prefix > 0 then
        nodes = fun.iter(nodes):map(function(n) return fio.pathjoin(prefix, n) end):totable()
    end

    if level == 1 then
        return nodes
    end

    local new = table.deepcopy(nodes)
    for _, node in pairs(nodes) do
        if fio.path.is_dir(fio.pathjoin(root, node)) then
            local subtree = tree(root, { prefix = node, level = level - 1 })
            append_table(new, subtree)
        end
    end

    return new
end

local function calc_hash(tbl)
    return digest.md5_hex(msgpack.encode(tbl))
end

return {
    has_value = has_value,
    read_file = read_file,
    write_file = write_file,
    is_array = is_array,
    is_map = is_map,
    to_array = to_array,
    sorted = sorted,
    arrays_equal = arrays_equal,
    cmpdeeply = cmpdeeply,
    table_count = table_count,
    append_table = append_table,
    get_table_keys = get_table_keys,
    merge_maps = merge_maps,
    iso8601_str_to_nsec = iso8601_str_to_nsec,
    nsec_to_iso8601_str = nsec_to_iso8601_str,
    date_str_to_nsec = date_str_to_nsec,
    custom_datetime_str_to_nsec = custom_datetime_str_to_nsec,
    nsec_to_date_str = nsec_to_date_str,
    time_str_to_nsec = time_str_to_nsec,
    nsec_to_time_str = nsec_to_time_str,
    day_of_week_str_to_number = day_of_week_str_to_number,
    nsec_to_day_of_week = nsec_to_day_of_week,
    parse_unix_time = parse_unix_time,
    is_unsigned = is_unsigned,
    is_email_valid = is_email_valid,
    reverse_table = reverse_table,
    tree = tree,
    calc_hash = calc_hash,
}
