local FAILURE_STATUS = 1
local FAILURE_AGAIN_STATUS = 2
local IN_PROGRESS = 3

local STATUSES = { 'Failure', 'Failure Again', 'In Progress' }

local function to_string(status)
    if status == FAILURE_STATUS then
        return STATUSES[FAILURE_STATUS]
    elseif status == FAILURE_AGAIN_STATUS then
        return STATUSES[FAILURE_AGAIN_STATUS]
    elseif status == IN_PROGRESS then
        return STATUSES[IN_PROGRESS]
    else
        return 'Invalid status: ' .. status
    end
end

return {
    FAILURE_STATUS = FAILURE_STATUS,
    FAILURE_AGAIN_STATUS = FAILURE_AGAIN_STATUS,
    IN_PROGRESS = IN_PROGRESS,
    to_string = to_string
}
