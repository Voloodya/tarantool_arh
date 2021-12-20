local sequence_generator_server = require('sequence_generator.server')

local function validate_config(cfg)
    return sequence_generator_server.validate_config(cfg)
end

local function apply_config(cfg)
    local _, err = sequence_generator_server.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end

    return true
end

local function get_range(name, opts)
    return sequence_generator_server.get_range(name, opts)
end

return {
    validate_config = validate_config,
    apply_config = apply_config,

    -- rpc registry
    get_range = get_range,
}
