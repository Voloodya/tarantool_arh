local states = {
    ACTIVE = 1,
    BLOCKED = 2,
    NEW = 3,
}

function states.to_string(state)
    if state == states.ACTIVE then
        return 'active'
    elseif state == states.BLOCKED then
        return 'blocked'
    elseif state == states.NEW then
        return 'new'
    end
    return nil
end

function states.from_string(state)
    if state == nil then
        return nil
    end
    return states[state:upper()]
end

return states
