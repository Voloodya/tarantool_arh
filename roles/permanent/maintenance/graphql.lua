local cartridge = require('cartridge')
local types = require('graphql.types')

local graphql = require('common.graphql')

local clock_delta_obj = types.object {
    name = 'ClockDelta',
    description = 'Max clock delta (difference between remote clock and the current one) in cluster in seconds',
    fields = {
        value = types.float.nonNull,
        is_threshold_exceeded = types.boolean.nonNull,
    }
}

local function init()
    graphql.add_callback_prefix('admin', 'maintenance', 'Maintenance api')
    graphql.add_mutation_prefix('admin', 'maintenance', 'Maintenance api')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'maintenance',
        name = 'clock_delta',
        doc = 'Return max clock delta in cluster',
        kind = clock_delta_obj,
        callback = 'roles.permanent.maintenance.graphql.get_max_clock_delta',
    })
    graphql.add_callback({
        schema = 'admin',
        prefix = 'maintenance',
        name = 'current_tdg_version',
        doc = 'Get current TDG version',
        args = {},
        kind = types.string.nonNull,
        callback = 'common.app_version.get',
    })
end

local function get_max_clock_delta(_, _)
    return cartridge.rpc_call('maintenance', 'get_max_clock_delta')
end

return {
    init = init,
    get_max_clock_delta = get_max_clock_delta,
}
