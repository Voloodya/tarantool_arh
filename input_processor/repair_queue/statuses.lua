local NEW = 1
local REWORKED = 2
local IN_PROGRESS = 3

local STATUSES = { 'New', 'Reworked', 'In progress' }

local function to_string(status)
    if status == NEW then
        return STATUSES[NEW]
    elseif status == REWORKED then
        return STATUSES[REWORKED]
    elseif status == IN_PROGRESS then
        return STATUSES[IN_PROGRESS]
    else
        return 'Invalid status: ' .. status
    end
end

return {
    NEW = NEW,
    REWORKED = REWORKED,
    IN_PROGRESS = IN_PROGRESS,
    to_string = to_string
}
