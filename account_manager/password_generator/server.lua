local module_name = 'account_manager.password_generator.server'

local errors = require('errors')
local checks = require('checks')
local fiber = require('fiber')

local config_error = errors.new_class('invalid password generator config')
local generator_error = errors.new_class('password generator error')

local defaults = require('common.defaults')
local vars = require('common.vars').new(module_name)
local config_checks = require('common.config_checks').new(config_error)
local config_filter = require('common.config_filter')

math.randomseed(os.time())

local LOWER_CHARS = 'abcdefghijklmnopqrstuvwxyz'
local UPPER_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
local DIGIT_CHARS = '1234567890'
local SYMBOL_CHARS_GEN = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
-- We allow to use spaces in passwords
-- however we won't generate passwords that
-- contain spaces.
local SYMBOL_CHARS_VALIDATE = SYMBOL_CHARS_GEN .. ' '

vars:new_global('min_length', defaults.PASSWORD_MIN_LENGTH)
vars:new_global('include', defaults.PASSWORD_INCLUDE)

local MAX_PASSWORD_LENGTH = 1000

local function validate_config(cfg)
    checks('table')
    local am_conf = config_filter.compare_and_get(cfg, 'account_manager', module_name)
    if am_conf == nil or am_conf.password_policy == nil then
        return true
    end
    local conf = am_conf.password_policy

    local min_length = 0

    if conf.include ~= nil then
        config_checks:check_optional_luatype('password_policy.include.lower', conf.include.lower, 'boolean')
        if conf.include.lower then
            min_length = min_length + 1
        end

        config_checks:check_optional_luatype('password_policy.include.upper', conf.include.upper, 'boolean')
        if conf.include.upper then
            min_length = min_length + 1
        end

        config_checks:check_optional_luatype('password_policy.include.digits', conf.include.digits, 'boolean')
        if conf.include.digits then
            min_length = min_length + 1
        end

        config_checks:check_optional_luatype('password_policy.include.symbols', conf.include.symbols, 'boolean')
        if conf.include.symbols then
            min_length = min_length + 1
        end
    end

    if conf.min_length ~= nil then
        config_checks:check_luatype('config.min_length', conf.min_length, 'number')
        config_error:assert(conf.min_length >= 1, 'min_length must be greater than 0')
        config_error:assert(conf.min_length <= MAX_PASSWORD_LENGTH, 'min_length must be less than %s',
            MAX_PASSWORD_LENGTH)
        config_error:assert(conf.min_length >= min_length,
                'min_length with selected include chars must be no less than %s', min_length)
    end

    return true
end

local function apply_config(config)
    checks('table')

    local conf, err = config_filter.compare_and_set(config, 'account_manager', module_name)
    if err ~= nil then
        return true
    end
    conf = conf or {}
    conf = conf.password_policy or {}

    vars.min_length = conf.min_length or defaults.PASSWORD_MIN_LENGTH
    vars.include = conf.include or defaults.PASSWORD_INCLUDE
end

local function fill_opts(opts)
    opts = opts or {}
    opts.min_length = opts.min_length or vars.min_length
    opts.include = opts.include or vars.include
    return opts
end

local function validate(password, opts)
    checks('string', {
        min_length = '?number',
        include = {
            lower = '?boolean',
            upper = '?boolean',
            digits = '?boolean',
            symbols = '?boolean',
        }
    })

    opts = fill_opts(opts)

    local password_len = utf8.len(password)
    if password_len < opts.min_length then
        return false, ("Password is less than %s characters"):format(opts.min_length)
    end

    if password_len > MAX_PASSWORD_LENGTH then
        return false, ("Password is grater than %s characters"):format(MAX_PASSWORD_LENGTH)
    end

    local has_lower = false
    local has_upper = false
    local has_digit = false
    local has_symbol = false

    for i = 1, #password do
        local c = utf8.sub(password, i, i)
        if 'a' <= c and c <= 'z' then
            has_lower = true
        elseif 'A' <= c and c <= 'Z' then
            has_upper = true
        elseif '0' <= c and c <= '9' then
            has_digit = true
        elseif string.find(SYMBOL_CHARS_VALIDATE, c, 1, true) ~= nil then
            has_symbol = true
        else
            return false, ('Incorrect symbol %q'):format(c)
        end
    end

    if opts.include.lower and not has_lower then
        return false, "Password does not include lowercase symbols"
    end
    if opts.include.upper and not has_upper then
        return false, "Password does not include uppercase symbols"
    end
    if opts.include.digits and not has_digit then
        return false, "Password does not include digits"
    end
    if opts.include.symbols and not has_symbol then
        return false, "Password does not include special characters"
    end

    return true
end

local function generate(opts)
    checks({
        min_length = '?number',
        include = {
            lower = '?boolean',
            upper = '?boolean',
            digits = '?boolean',
            symbols = '?boolean',
        }
    })

    opts = fill_opts(opts)

    if opts.min_length < 1 then
        return nil, generator_error:new('min_length must be greater than 0, got %s', opts.min_length)
    end

    if opts.min_length > MAX_PASSWORD_LENGTH then
        return nil, generator_error:new('min_length must be less than %d, got %d', MAX_PASSWORD_LENGTH, opts.min_length)
    end

    local dictionaries = {}

    if opts.include.lower then
        table.insert(dictionaries, LOWER_CHARS)
    end

    if opts.include.upper then
        table.insert(dictionaries, UPPER_CHARS)
    end

    if opts.include.digits then
        table.insert(dictionaries, DIGIT_CHARS)
    end

    if opts.include.symbols then
        table.insert(dictionaries, SYMBOL_CHARS_GEN)
    end

    if #dictionaries == 0 then
        return nil, generator_error:new('no included chars selected')
    end

    if opts.min_length < #dictionaries then
        return nil, generator_error:new('min_length with selected options: %s, got %s', #dictionaries, opts.min_length)
    end

    local password
    repeat
        password = ''
        for _ = 1, opts.min_length do
            local d = math.random(1, #dictionaries)
            local dict = dictionaries[d]

            local n = math.random(1, #dict)
            password = password .. dict:sub(n, n)
        end
        fiber.yield()
    until validate(password, opts)

    return password
end

return {
    validate_config = validate_config,
    apply_config = apply_config,

    validate = validate,
    generate = generate,
}
