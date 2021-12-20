local function generator(param)
    local states = param.states
    local state1 = states[1]
    local state2 = states[2]

    local tuples = param.tuples

    local comparator = param.comparator

    if tuples[1] == nil and state1.state ~= nil then
        state1.state, tuples[1] = state1.gen(state1.param, state1.state)
    end

    if tuples[2] == nil and state2.state ~= nil then
        state2.state, tuples[2] = state2.gen(state2.param, state2.state)
    end

    if state1.state == nil and state2.state == nil then
        return nil
    end

    local index
    local tuple1 = tuples[1]
    local tuple2 = tuples[2]
    if tuple1 == nil then
        index = 2
    elseif tuple2 == nil then
        index = 1
    else
        if comparator(tuple1, tuple2, state1.state, state2.state) then
            index = 1
        else
            index = 2
        end
    end

    local tuple = tuples[index]
    tuples[index] = nil
    local state = states[index].state

    return state, tuple
end

local function merge2(state1, state2, comparator)
    if state2 == nil then
        return state1
    end

    local param = {
        comparator = comparator,
        tuples = {nil, nil},
        states = {state1, state2},
    }

    return {
        state = {},
        gen = generator,
        param = param,
    }
end

return {
    merge2 = merge2,
}
