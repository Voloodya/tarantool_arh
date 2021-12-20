local input_processor = require('roles.runner.input_processor')
local output_processor = require('roles.runner.output_processor')
local task_runner = require('roles.runner.task_runner')

local function init()
    task_runner.init()
end

local system_methods = {
    init = true,
    apply_config = true,
    validate_config = true,
    tenant_apply_config = true,
    tenant_validate_config = true,
}
local function extend_role(role, methods)
    for name, fun in pairs(methods) do
        if system_methods[name] == nil then
            role[name] = fun
        end
    end
end

local function tenant_validate_config(cfg)
    local _, err = input_processor.tenant_validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = task_runner.tenant_validate_config(cfg)
    if err ~= nil then
        return nil, err
    end

    local _, err = output_processor.validate_config(cfg)
    if err ~= nil then
        return nil, err
    end
end

local function tenant_apply_config(cfg, opts)
    local _, err = input_processor.tenant_apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    local _, err = task_runner.tenant_apply_config(cfg, opts)
    if err ~= nil then
        return nil, err
    end

    local _, err = output_processor.apply_config(cfg)
    if err ~= nil then
        return nil, err
    end
end

local function validate_config(_)
    return true
end

local function apply_config(_)
    return true
end

local role_methods = {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,
    tenant_validate_config = tenant_validate_config,
    tenant_apply_config = tenant_apply_config,

    role_name = 'runner',
    implies_router = true,
    dependencies = {'cartridge.roles.vshard-router'},
}

extend_role(role_methods, input_processor)
extend_role(role_methods, output_processor)
extend_role(role_methods, task_runner)

return role_methods
