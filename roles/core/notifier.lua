local notifier_server = require('notifier.server')
local notifier_config = require('notifier.config')
local notifier = require('notifier.notifier')

local function validate_config(cfg)
    return notifier_config.validate(cfg)
end

local function apply_config(cfg, opts)
    local _, err = notifier_server.apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end
    return true
end

local function repair_failure(id, time, reason)
    return notifier.repair_failure(id, time, reason)
end

local function new_object_added(id, time, reason)
    return notifier.new_object_added(id, time, reason)
end

return {
    validate_config = validate_config,
    apply_config = apply_config,

    -- rpc registry
    notifier_repair_failure = repair_failure,
    notifier_new_object_added = new_object_added,
}
