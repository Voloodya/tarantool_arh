local spawn = require('common.sandbox.spawn')

return {
    exports = {
        -- Cannot make it static because of circular dependency on sandbox
        spawn = function(sandbox, ...)
            return spawn.spawn(sandbox, ...)
        end,
        spawn_n = function(sandbox, ...)
            return spawn.spawn_n(sandbox, ...)
        end,
    },
}
