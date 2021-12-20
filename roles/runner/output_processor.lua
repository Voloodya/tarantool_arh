local checks = require('checks')
local output_processor_config = require('output_processor.config')
local output_processor = require('output_processor.output_processor')
local output_processor_list = require('output_processor.output_processor_list')

local function validate_config(cfg)
    return output_processor_config.validate(cfg)
end

local function apply_config(cfg)
    local _, err = output_processor.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end
    return true
end

local function handle_output_object(type_name, routing_key, tuple, outputs, is_async)
    checks('?string', 'string', 'table|tuple', '?table', '?boolean')
    return output_processor.handle_output_object(type_name, routing_key, tuple, outputs, is_async)
end

local function get(id)
    return output_processor_list.get(id)
end

local function filter(from, to, reason, first, after)
    return output_processor_list.filter(from, to, reason, first, after)
end

local function checked(func, ...)
    local _, err = func(...)
    if err ~= nil then
        return nil, err
    end
    return 'ok'
end

local function delete(id)
    checks('string')
    return checked(output_processor_list.delete, id)
end

local function clear()
    return checked(output_processor_list.clear)
end

local function postprocess_again(id)
    checks('string')
    return checked(output_processor.postprocess_again, id)
end

local function postprocess_again_all()
    return checked(output_processor.postprocess_again_all)
end

return {
    validate_config = validate_config,
    apply_config = apply_config,

    -- rpc registry
    handle_output_object = handle_output_object,

    output_processor_list_get = get,
    output_processor_list_filter = filter,
    output_processor_list_delete = delete,
    output_processor_list_clear = clear,
    postprocess_again = postprocess_again,
    postprocess_again_all = postprocess_again_all,
}
