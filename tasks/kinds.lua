--luacheck: globals checks

local SINGLE_SHOT = 'single_shot'
local CONTINUOUS = 'continuous'
local PERIODICAL = 'periodical'
local PERIODICAL_SYSTEM = 'periodical_system'

local function is_valid(kind)
    checks('string')

    return kind == SINGLE_SHOT
        or kind == CONTINUOUS
        or kind == PERIODICAL
        or kind == PERIODICAL_SYSTEM
end

local function is_system(kind)
    checks('string')
    return kind == PERIODICAL_SYSTEM
end

return {
    SINGLE_SHOT = SINGLE_SHOT,
    CONTINUOUS = CONTINUOUS,
    PERIODICAL = PERIODICAL,
    PERIODICAL_SYSTEM = PERIODICAL_SYSTEM,

    is_valid = is_valid,
    is_system = is_system,
}
