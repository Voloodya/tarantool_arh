local fun = require('fun')
local utils = require('common.utils')

local modules = utils.tree('common/sandbox/modules')
local dynamic_modules = utils.tree('common/sandbox/dynamic_modules')
local all = utils.append_table(modules, dynamic_modules)

return fun.iter(all)
    :map(function(name) return name:gsub('.lua', '') end)
    :map(function(name) return name:gsub('/', '.') end)
    :totable()
