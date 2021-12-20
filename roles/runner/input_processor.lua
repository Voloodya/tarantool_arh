local input_processor_config = require('input_processor.config')
local input_processor_server = require('input_processor.server')

local repair_queue_server = require('input_processor.repair_queue.server')

local function tenant_apply_config(cfg)
    local _, err = input_processor_server.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end
end

local function tenant_validate_config(cfg)
    local _, err = input_processor_config.validate(cfg)
    if err ~= nil then
        return nil, err
    end
end

local function handle_input_object(...)
    return input_processor_server.handle_input_object(...)
end

local function repair_queue_get(id)
    return repair_queue_server.get(id)
end

local function repair_queue_filter(from, to, reason, first, after)
    return repair_queue_server.filter(from, to, reason, first, after)
end

local function repair_queue_delete(id)
    return repair_queue_server.delete(id)
end

local function repair_queue_clear()
    return repair_queue_server.clear()
end

local function repair_queue_try_again(id)
    return repair_queue_server.try_again(id)
end

local function repair_queue_try_all()
    return repair_queue_server.try_again_all()
end

return {
    -- Multitenancy
    tenant_apply_config = tenant_apply_config,
    tenant_validate_config = tenant_validate_config,

    -- rpc registry
    handle_input_object = handle_input_object,

    repair_queue_get = repair_queue_get,
    repair_queue_filter = repair_queue_filter,
    repair_queue_delete = repair_queue_delete,
    repair_queue_clear = repair_queue_clear,
    repair_queue_try_again = repair_queue_try_again,
    repair_queue_try_all = repair_queue_try_all,
}
