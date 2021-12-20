local module_name = 'common.sandbox.sequence'
local cartridge = require('cartridge')
local lock_with_timeout = require('common.lock_with_timeout')

local vars = require('common.vars').new(module_name)

vars:new('range_width', 10)
vars:new('sequences'
   --[name] = {
   --    range = {
   --        next_value = ...,
   --        last_value = ...,
   --    },
   --}
)

vars:new('locks')

local LOCK_TIMEOUT = 0.5

local function get_sequence_name(name)
    return 'sandbox_sequence_' .. name
end

local function is_limit_exhausted(sequence)
    local range = sequence.range
    return range == nil or range.next_value > range.last_value
end

local function get_next_value(name)
    local sequence = vars.sequences[name]
    if sequence == nil then
        return nil, 'Sequence is not started'
    end

    while is_limit_exhausted(sequence) do
        vars.locks = vars.locks or {}
        local lock = vars.locks[name]
        if lock == nil or lock:released() then
            lock = lock_with_timeout.new(LOCK_TIMEOUT)
            vars.locks[name] = lock

            local new_range, err = cartridge.rpc_call('core', 'get_range',
                { get_sequence_name(name), { range_width = vars.range_width } },
                { leader_only = true })

            vars.locks[name]:broadcast_and_release()

            if err ~= nil then
                return nil, err
            end

            sequence.range = {
                next_value = new_range[1],
                last_value = new_range[2],
            }
        else
            lock:wait()
        end
    end

    local result = sequence.range.next_value
    sequence.range.next_value = sequence.range.next_value + 1
    return result
end

local function create_sequence_object(name)
    return {
        next = function()
            return get_next_value(name)
        end,
    }
end

local function get(sequence_name)
    if vars.sequences == nil then
        vars.sequences = {}
    end

    if vars.sequences[sequence_name] == nil then
        vars.sequences[sequence_name] = create_sequence_object(sequence_name)
    end

    return vars.sequences[sequence_name]
end

return {
    get = get,
}
