local module_name = 'storage.model_key_def'

local key_def_lib = require('key_def')
local msgpack = require('msgpack')

local vars = require('common.vars').new(module_name)
local lru_cache = require('common.lru_cache')

local CACHE_SIZE = 300

vars:new_global('key_def', lru_cache.new(CACHE_SIZE))

local function calc_hash(type_name, index)
    return msgpack.encode({type_name, index})
end

local function get_space_key_def(space_name, index_id)
    local hash = calc_hash(space_name, index_id)
    local key_def = vars.key_def:get(hash)
    if key_def == nil then
        local parts = box.space[space_name].index[index_id].parts
        key_def = key_def_lib.new(parts)
        vars.key_def:set(hash, key_def)
    end
    return key_def
end

local function apply_config()
    vars.key_def = lru_cache.new(CACHE_SIZE)
end

return {
    apply_config = apply_config,
    get_space_key_def = get_space_key_def,
}
