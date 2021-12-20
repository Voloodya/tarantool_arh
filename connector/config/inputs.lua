local yaml = require('yaml')
local tenant = require('common.tenant')

local function input_from_config(input)
    local name = input.name
    local type = input.type
    input.name = nil
    input.type = nil
    local options = ''
    if next(input) then
        options = yaml.encode(input)
    end
    return {
        name = name,
        type = type,
        options = options,
    }
end

local function get_config_inputs()
    local connector_cfg = tenant.get_cfg_deepcopy('connector') or {}
    local inputs_list = {}
    for _, input in pairs(connector_cfg.input or {}) do
        table.insert(inputs_list, input_from_config(input))
    end
    return inputs_list
end

local function find_input(inputs_cfg, name)
    if inputs_cfg == nil then
        return nil, "no input with name " .. name
    end

    for i, input in pairs(inputs_cfg) do
        if input.name == name then
            return i
        end
    end
    return nil, "no input with name " .. name
end

local function prepare_input(name, in_type, options)
    if options == nil or options == '' then
        options = '[]'
    end
    local new_input, err = yaml.decode(options)
    if type(new_input) ~= 'table' then
        return nil, err or "invalid options data"
    end
    new_input.type = in_type
    new_input.name = name
    return new_input
end

local function add_input(connector_cfg, name, in_type, options)
    connector_cfg = connector_cfg or {}
    connector_cfg.input = connector_cfg.input or {}

    local pos = find_input(connector_cfg.input, name)
    if pos ~= nil then
        return nil, 'input with name ' .. name .. ' already exists'
    end

    local new_input, err = prepare_input(name, in_type, options)
    if err ~= nil then
        return nil, err
    end
    table.insert(connector_cfg.input, new_input)
    return connector_cfg
end

local function update_input(connector_cfg, name, new_name, in_type, options)
    if connector_cfg == nil then
        return nil, "no input with name " .. name
    end

    local pos, err = find_input(connector_cfg.input, name)
    if err ~= nil then
        return nil, err
    end

    local new_input, err = prepare_input(new_name, in_type, options)
    if err ~= nil then
        return nil, err
    end

    connector_cfg.input[pos] = new_input
    return connector_cfg
end

local function delete_input(connector_cfg, name)
    if connector_cfg == nil then
        return nil, "no input with name " .. name
    end

    local pos, err = find_input(connector_cfg.input, name)
    if err ~= nil then
        return nil, err
    end

    table.remove(connector_cfg.input, pos)
    return connector_cfg
end

local function add_config_input(name, type, options)
    local connector_cfg, err = tenant.get_cfg_deepcopy('connector')
    if err ~= nil then
        return nil, err
    end

    connector_cfg, err = add_input(connector_cfg, name, type, options)
    if err ~= nil then
        return nil, err
    end
    local _, err = tenant.patch_config({ ['connector'] = connector_cfg })
    if err ~= nil then
        return nil, err
    end

    return true
end

local function update_config_input(name, type, options, new_name)
    local connector_cfg, err = tenant.get_cfg_deepcopy('connector')
    if err ~= nil then
        return nil, err
    end

    if new_name == nil then
        new_name = name
    end
    connector_cfg, err = update_input(connector_cfg, name, new_name, type, options)
    if err ~= nil then
        return nil, err
    end

    local _, err = tenant.patch_config({ ['connector'] = connector_cfg })
    if err ~= nil then
        return nil, err
    end

    return true
end

local function delete_config_input(name)
    local connector_cfg, err = tenant.get_cfg_deepcopy('connector')
    if err ~= nil then
        return nil, err
    end

    connector_cfg, err = delete_input(connector_cfg, name)
    if err ~= nil then
        return nil, err
    end

    local _, err = tenant.patch_config({ ['connector'] = connector_cfg })
    if err ~= nil then
        return nil, err
    end

    return true
end

return {
    get_config_inputs = get_config_inputs,
    add_config_input = add_config_input,
    update_config_input = update_config_input,
    delete_config_input = delete_config_input,
}
