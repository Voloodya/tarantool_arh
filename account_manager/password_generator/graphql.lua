local module_name = 'account_manager.password_generator.graphql'

local audit_log = require('audit.log').new(module_name)

local cartridge = require('cartridge')
local types = require('graphql.types')

local graphql = require('common.graphql')
local defaults = require('common.defaults')

local function get_config(_, _)
    local cfg = cartridge.config_get_deepcopy('account_manager')
    if cfg == nil then
        cfg = {} -- Just to workaround potential error in the following line
    end

    local password_policy = cfg.password_policy or {}
    local min_length = password_policy.min_length or defaults.PASSWORD_MIN_LENGTH
    local include = password_policy.include or defaults.PASSWORD_INCLUDE

    return {
        min_length = min_length,
        lower = include.lower or false,
        upper = include.upper or false,
        digits = include.digits or false,
        symbols = include.symbols or false,
    }
end

local function get_password_policy(args)
    local include_section
    if args.lower ~= nil or args.upper ~= nil or args.digits ~= nil or args.symbols ~= nil then
        include_section = {
            lower = args.lower or false,
            upper = args.upper or false,
            digits = args.digits or false,
            symbols = args.symbols or false,
        }

        local at_least_one_enabled = false
        for _, v in pairs(include_section) do
            if v == true then
                at_least_one_enabled = true
                break
            end
        end

        if not at_least_one_enabled then
            return nil, 'At least one option should be enabled'
        end
    end

    return {
        min_length = args.min_length,
        include = include_section,
    }
end

local function set_config(_, args)
    local password_policy, err = get_password_policy(args)
    if err ~= nil then
        return nil, err
    end

    local config = cartridge.config_get_deepcopy('account_manager')
    if config == nil then
        config = {}
    end

    config.password_policy = password_policy
    local _, err = cartridge.config_patch_clusterwide({ account_manager = config })
    if err ~= nil then
        return nil, err
    end

    audit_log.info('New configuration for password generator has been applied')
    return 'ok'
end

local function generate(_, args)
    local password_policy, err = get_password_policy(args)
    if err ~= nil then
        return nil, err
    end

    return cartridge.rpc_call('core', 'generate_password', { password_policy })
end

local function validate(_, args)
    local ok, err = cartridge.rpc_call('core', 'validate_password', { args.password })
    if ok == nil then
        return nil, err
    end
    return {
        ok = ok,
        reason = err,
    }
end

local function init()
    local password_config = types.object {
        name = 'Password_config',
        description = 'A list of included chars in passwords',
        fields = {
            min_length = types.int.nonNull,
            lower = types.boolean.nonNull,
            upper = types.boolean.nonNull,
            digits = types.boolean.nonNull,
            symbols = types.boolean.nonNull,
        }
    }

    local validate_response = types.object {
        name = 'ValidateResponse',
        description = 'A response format of password validation',
        fields = {
            ok = types.boolean.nonNull,
            reason = types.string,
        }
    }

    graphql.add_callback_prefix('admin', 'password_generator', 'Password generator')
    graphql.add_mutation_prefix('admin', 'password_generator', 'Password generator')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'password_generator',
        name = 'config',
        doc = 'Returns options of password generator',
        args = {},
        kind = password_config.nonNull,
        callback = 'account_manager.password_generator.graphql.get_config',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'password_generator',
        name = 'config',
        doc = 'Set options for password generator',
        args = {
            min_length = types.int,
            lower = types.boolean,
            upper = types.boolean,
            digits = types.boolean,
            symbols = types.boolean,
        },
        kind = types.string.nonNull,
        callback = 'account_manager.password_generator.graphql.set_config',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'password_generator',
        name = 'generate',
        doc = 'Returns new password from generator',
        args = {
            min_length = types.int,
            lower = types.boolean,
            upper = types.boolean,
            digits = types.boolean,
            symbols = types.boolean,
        },
        kind = types.string.nonNull,
        callback = 'account_manager.password_generator.graphql.generate',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'password_generator',
        name = 'validate',
        doc = 'Validates password and returns status: good or bad password',
        args = {
            password = types.string.nonNull,
        },
        kind = validate_response.nonNull,
        callback = 'account_manager.password_generator.graphql.validate',
    })
end

return {
    get_config = get_config,
    set_config = set_config,
    generate = generate,
    validate = validate,

    init = init,
}
