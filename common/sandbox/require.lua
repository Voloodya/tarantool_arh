local module_name = 'common.sandbox'
local module_path = module_name:gsub('%.', '/')

local fio = require('fio')
local utils = require('common.utils')
local lock_with_timeout = require('common.lock_with_timeout')
local builtin = require('common.sandbox.builtin')
local dynamic_loader = require('common.sandbox.dynamic_module_loader')

local BUILTIN_MODULES_PATH = fio.pathjoin(module_path, 'modules')
local BUILTIN_DYN_MODULES_PATH = fio.pathjoin(module_path, 'dynamic_modules')

local function table_has_key(t, key)
    if t == nil then
        return false
    end
    return t[key] ~= nil
end

local function raise(msg, ...)
    msg = tostring(msg)
    local err = string.format(msg, ...)
    error(err, 0)
end

local function lookup_module(self, name)
    local base_path = name:gsub('%.', '/')

    local path = 'src/' .. base_path
    local singlefile = path..'.lua'
    if table_has_key(self.cfg, singlefile) then
        return singlefile
    end

    local err = {
        string.format("module '%s' not found:", name),
        string.format("\tno file '%s'", singlefile),
    }

    local multifile = path..'/init.lua'
    if table_has_key(self.cfg, multifile) then
        return multifile
    end
    table.insert(err, string.format("\tno file '%s'", multifile))

    path = fio.pathjoin(BUILTIN_MODULES_PATH, name..'.lua')
    if fio.path.is_file(path) then
        return path
    end
    table.insert(err, string.format("\tno file '%s'", path))

    path = fio.pathjoin(BUILTIN_DYN_MODULES_PATH, name..'.lua')
    if fio.path.is_file(path) then
        return path
    end
    table.insert(err, string.format("\tno file '%s'", path))

    local ext_path = 'extensions/sandbox/' .. base_path
    local ext_singlefile = ext_path .. '.lua'
    if table_has_key(self.extensions_cfg, ext_singlefile) then
        return ext_singlefile
    end
    table.insert(err, string.format("\tno file '%s'", ext_singlefile))

    local ext_multifile = ext_path .. '/init.lua'
    if table_has_key(self.extensions_cfg, ext_multifile) then
        return ext_multifile
    end
    table.insert(err, string.format("\tno file '%s'", ext_multifile))

    local err_msg = table.concat(err, '\n')
    return nil, err_msg
end

local function not_initialized()
    raise('"require" not yet initialized')
end

local _require = not_initialized

local function eval(self, code, chunkname, load_safe, require_path)
    local env
    if not load_safe then
        env = table.deepcopy(builtin)
        local binded_require_path = function(name) return _require(self, name, require_path) end
        env.require = binded_require_path
    end

    local mod_fn, err = load(code, chunkname, 't', env)
    if not mod_fn then
        raise(err)
    end

    return mod_fn()
end

local function sandbox_initialized(self)
    return self.src_hash ~= nil
end

local function is_dynamic_module(path)
    return path:find(BUILTIN_DYN_MODULES_PATH) ~= nil
end

local function assert_no_cycles(lname, path)
    for _, rname in ipairs(path) do
        if lname == rname.name then
            table.insert(path, { name = lname })
            local trace = {}
            local source = table.remove(path, 1)
            for _, target in ipairs(path) do
                table.insert(trace, string.format('\t%s: require("%s")', source.file, target.name))
                source = target
            end
            local errmsg = 'cycle detected:\n' .. table.concat(trace, '\n')
            raise(errmsg)
        end
    end
end

local function load_code(self, name, require_path)
    local path, err = lookup_module(self, name)
    if not path then
        raise(err)
    end

    table.insert(require_path, { name = name, file = path })

    local load_safe = false
    local code
    local is_dynamic, is_extension = false, false

    if self.cfg ~= nil and self.cfg[path] ~= nil then
        code = self.cfg[path]
    elseif self.extensions_cfg ~= nil and self.extensions_cfg[path] ~= nil then
        is_extension = true
        load_safe = true
        code = self.extensions_cfg[path]
    end

    if not code then
        load_safe = true
        is_dynamic = is_dynamic_module(path)
        code, err = utils.read_file(path)
        if not code then
            self.errors[name] = err
            raise(err)
        end
    end

    local chunkname = path and '='..path
    local ok, mod_or_err = pcall(eval, self, code, chunkname, load_safe, require_path)

    if not ok then
        if not is_dynamic and not is_extension then
            self.errors[name] = mod_or_err
        end
        require_path = nil
        raise(mod_or_err)
    end

    if is_dynamic then
        if type(mod_or_err) ~= 'table' then
            raise('Dynamic module "%s" must be a table', name)
        end
        ok, mod_or_err = pcall(dynamic_loader.load, mod_or_err, self)
    end

    if not ok then
        require_path = nil
        raise(mod_or_err)
    end

    if mod_or_err == nil then
        mod_or_err = true
    end
    self.loaded[name] = mod_or_err
    self.errors[name] = nil
    table.remove(require_path)

    return mod_or_err
end

local function get_loaded(self, name)
    local loaded = self.loaded[name]
    if loaded ~= nil then
        return loaded
    end

    local err = self.errors[name]
    if err ~= nil then
        raise(err)
    end
end

local LOAD_TIMEOUT = 1
_require = function(self, name, require_path)
    if not sandbox_initialized(self) then
        raise('Module ("%s") require is not allowed before config apply', name)
    end

    if type(name) ~= 'string' and type(name) ~= 'number' then
        raise("bad argument #1 to 'require' (string expected, got %s)", type(name))
    end

    if require_path == nil then
        require_path = {}
    else
        assert_no_cycles(name, require_path)
    end

    local loaded = get_loaded(self, name)
    if loaded ~= nil then
        return loaded
    end

    -- Since modules lookup and loading uses "fio" that yields
    -- this code part is a critical section.
    while true do
        local lock = self.locks[name]
        if lock == nil or lock:released() then
            lock = lock_with_timeout.new(LOAD_TIMEOUT)
            self.locks[name] = lock

            local ok, res = pcall(load_code, self, name, require_path)
            lock:broadcast_and_release()
            if not ok then
                raise(res)
            end
            return res
        else
            self.locks[name]:wait()
            loaded = get_loaded(self, name)
            if loaded ~= nil then
                return loaded
            end
        end
    end
end

return {
    require = _require,
    eval = eval,
}
