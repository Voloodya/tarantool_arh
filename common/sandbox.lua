local module_name = 'common.sandbox'

local checks = require('checks')
local fun = require('fun')
local errors = require('errors')
local msgpack = require('msgpack')
local digest = require('digest')

local log = require('log.log').new(module_name)
local tenant = require('common.tenant')
local sandbox_context = require('common.sandbox.context')
local require_mod = require('common.sandbox.require')
local dispatcher = require('common.sandbox.dispatcher')
local executor = require('common.sandbox.executor')

local vars = require('common.vars').new(module_name)
local config_error = errors.new_class('Invalid sandbox config')

vars:new('sandbox_context')
vars:new_global('extensions_cfg', {})
vars:new_global('extensions_src_hash', {})
vars:new_global('extensions_global_hash')
vars:new_global('extensions_global_init_fn')
vars:new_global('extensions_global_filename', 'extensions/global.lua')

local SANDBOX_MODULES = require('common.sandbox.modules')

local function filter_by_prefix(cfg, prefix)
    checks('table', 'string')
    prefix = prefix .. '/'
    return fun.iter(cfg):filter(function(k) return k:startswith(prefix) end):tomap()
end

local function clear_cache(self)
    self.loaded = {}
    self.errors = {}
end

local function validate_name(section)
    local name = section:gsub('^src/', ''):gsub('.lua$', ''):gsub('/', '.')
    for _, reserved in pairs(SANDBOX_MODULES) do
        config_error:assert(name ~= reserved,
            'Module name "%s" is reserved, please choose another one', name)
    end
end

local function validate_module(section, code)
    config_error:assert(section:endswith('.lua'),
        'Expect "%s" to have ".lua" extension', section)
    config_error:assert(code ~= nil,
        'Expect "%s" to have string content, got "null"', section)
    config_error:assert(type(code) == 'string',
        'Expect "%s" to have string content, got "%s"', section, type(code))
end

local function validate_impl(self)
    checks('sandbox')

    local modules = filter_by_prefix(self.cfg, 'src')
    for section, code in pairs(modules) do
        validate_module(section, code)
        validate_name(section)
    end

    return true
end

local function validate(self, opts)
    opts = opts or {}
    checks('sandbox', 'table')

    local ok, err = pcall(validate_impl, self)
    if ok then
        return true
    end

    if opts.protected then
        return false, err
    end

    error(err)
end

local function update(self, cfg)
    cfg = cfg or {}
    checks('sandbox', '?table')

    -- We need to store extensions inside sandbox since
    -- we should have ability to dispatch function from
    -- extensions on validate config stage.
    local key = self.key
    if tenant.is_default() then
        self.extensions_cfg = self.extensions_cfg or {}

        local extensions_src_cfg = filter_by_prefix(cfg, 'extensions/sandbox')
        local extensions_hash = digest.md5_hex(msgpack.encode(extensions_src_cfg))
        if self.extensions_src_hash ~= extensions_hash then
            self.extensions_cfg = table.copy(extensions_src_cfg)
            self.extensions_src_hash = extensions_hash
            self:clear_cache()

            -- Update global state that will be used for other tenants
            -- self.key here could be "active" or "tmp".
            -- Such approach allows to share extensions for non-default
            -- tenants sandboxes.
            vars.extensions_cfg[key] = self.extensions_cfg
            vars.extensions_src_hash[key] = self.extensions_src_hash
        end
    else
        if self.extensions_src_hash ~= vars.extensions_src_hash[key] then
            self.extensions_cfg = vars.extensions_cfg[key]
            self.extensions_src_hash = vars.extensions_src_hash[key]
            self:clear_cache()
        end
    end

    local src_cfg = filter_by_prefix(cfg or {}, 'src')
    local hash = digest.md5_hex(msgpack.encode(src_cfg))
    if self.src_hash ~= hash then
        self.cfg = table.copy(src_cfg)
        self:clear_cache()
        self.src_hash = hash
        return true
    end

    return false
end

local instance_methods = {
    update = update,
    validate = validate,

    eval = require_mod.eval,
    require = require_mod.require,

    dispatch_function = dispatcher.get,

    call = executor.call,
    call_by_name = executor.call_by_name,
    batch_call = executor.batch_call,
    batch_accumulate = executor.batch_accumulate,
    clear_cache = clear_cache,
}

-- There are two types of keys for sandbox:
-- "active" and "tmp".
--   * "active" is ready to use sandbox instance
--   * "tmp" is used for validation
local function new(cfg, key)
    assert(key == 'active' or key == 'tmp')
    local instance = {
        key = key,
        loaded = {},
        errors = {},
        locks = {},
        cfg = nil,
        src_hash = nil,
        context = nil,
    }

    setmetatable(instance, {
        __type = 'sandbox',
        __index = instance_methods,
    })
    instance:update(cfg)
    return instance
end

local function validate_config(cfg)
    checks('table')

    local modules = filter_by_prefix(cfg, 'extensions')
    for section, code in pairs(modules) do
        validate_module(section, code)
    end

    return true
end

local function load_global_init(cfg)
    local extensions_global = cfg[vars.extensions_global_filename]
    if extensions_global == nil then
        vars.extensions_global_init_fn = nil
        vars.extensions_global_hash = nil
        return
    end

    local fn_init = vars.extensions_global_init_fn
    local extensions_global_hash = digest.md5_hex(extensions_global)
    if vars.extensions_global_hash ~= extensions_global_hash then
        local load_module, err = load(extensions_global, '@' .. vars.extensions_global_filename , 't')
        if err ~= nil then
            return nil, err
        end

        local ok, module = pcall(load_module)
        if not ok then
            return nil, module
        end

        if type(module) ~= 'table' then
            return nil, 'expected ' .. vars.extensions_global_filename .. ' returns a module (lua table)'
        end

        fn_init = module.init
        if type(fn_init) ~= 'function' then
            return nil, 'init from ' .. vars.extensions_global_filename .. ' is not a function'
        end

        vars.extensions_global_hash = extensions_global_hash
        vars.extensions_global_init_fn = fn_init
    end

    local ok, err = pcall(fn_init)
    if not ok then
        return nil, err
    end
end

-- Be aware, global config may not be automatically applied
-- to sandbox instances due to cached modules in self.loaded
local function apply_config(cfg)
    checks('table')

    if tenant.is_default() then
        local _, err = load_global_init(cfg)
        if err ~= nil then
            log.error('Error is returned when %s was processed: %s',
                vars.extensions_global_filename, err)
        end
    end

    vars.sandbox_context = vars.sandbox_context or sandbox_context.new()
    vars.sandbox_context:apply_config(cfg)

    return true
end

return {
    new = new,
    validate_config = validate_config,
    apply_config = apply_config,
}
