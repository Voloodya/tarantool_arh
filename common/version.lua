local clock = require('clock')

local MIN_POSSIBLE_VERSION = tonumber64(0)
local MAX_POSSIBLE_VERSION = tonumber64(0xFFFFFFFFFFFFFFFFULL)

local function get_new()
    return clock.time64()
end

local function get_diff(version)
    return get_new() - version
end

return {
    get_new = get_new,
    get_diff = get_diff,
    MIN_POSSIBLE_VERSION = MIN_POSSIBLE_VERSION,
    MAX_POSSIBLE_VERSION = MAX_POSSIBLE_VERSION,
}
