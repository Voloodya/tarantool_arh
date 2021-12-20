local PREPROCESSING_ERROR = 1
local SENDING_ERROR = 2
local IN_PROGRESS = 3
local REPOSTPROCESSED_PREPROCESSING_ERROR = 4
local REPOSTPROCESSED_SENDING_ERROR = 5

local function to_string(status)
    if status == PREPROCESSING_ERROR then
        return 'Preprocessing error'
    elseif status == SENDING_ERROR then
        return 'Sending error'
    elseif status == IN_PROGRESS then
        return 'In progress'
    elseif status == REPOSTPROCESSED_PREPROCESSING_ERROR then
        return 'Rereplicated (Preprocessing error)'
    elseif status == REPOSTPROCESSED_SENDING_ERROR then
        return 'Rereplicated (Sending error)'
    else
        return 'Invalid status: ' .. status
    end
end

return {
    PREPROCESSING_ERROR = PREPROCESSING_ERROR,
    SENDING_ERROR = SENDING_ERROR,
    IN_PROGRESS = IN_PROGRESS,
    REPOSTPROCESSED_PREPROCESSING_ERROR = REPOSTPROCESSED_PREPROCESSING_ERROR,
    REPOSTPROCESSED_SENDING_ERROR = REPOSTPROCESSED_SENDING_ERROR,
    to_string = to_string
}
