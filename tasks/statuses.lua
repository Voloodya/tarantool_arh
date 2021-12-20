--luacheck: globals checks

local UNKNOWN_TASK      = 0
local DID_NOT_START     = 1
local PENDING           = 2
local RUNNING           = 3
local STOPPED           = 4
local FAILED            = 5
local COMPLETED         = 6
local LOST              = 7

local as_text = {
    'unknown task',
    'did not start',
    'pending',
    'running',
    'stopped',
    'failed',
    'completed',
    'lost',
}

local function to_string(status)
    checks('number')

    if status <= #as_text + 1 then
        return as_text[status + 1]
    end

    return nil, 'invalid status ' .. status
end

local function is_running(status)
    checks('number')
    return status == PENDING
        or status == RUNNING
end

return {
    UNKNOWN_TASK    = UNKNOWN_TASK,
    DID_NOT_START   = DID_NOT_START,
    PENDING         = PENDING,
    RUNNING         = RUNNING,
    STOPPED         = STOPPED,
    FAILED          = FAILED,
    COMPLETED       = COMPLETED,
    LOST            = LOST,

    to_string = to_string,

    is_running = is_running
}
