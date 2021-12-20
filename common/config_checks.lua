local utils = require('common.utils')

-- errors are used here solely for the sake of brevity
-- they are always caught in validate() function

local config_checks = {}

function config_checks:assert(cond, fmt, ...)
    if not cond then
        error(self.error_class:new(fmt, ...))
    end
end

function config_checks:check_luatype(field, value, luatype)
    self:assert(value ~= nil, '%s is mandatory', field)
    self:assert(type(value) == luatype, '%s must be a %s', field, luatype)
end

function config_checks:check_optional_luatype(field, value, luatype)
    if value ~= nil then
        self:assert(type(value) == luatype, '%s must be a %s', field, luatype)
    end
end

function config_checks:check_table_keys(field_name, tbl, expected_keys)
    self:assert(type(tbl) == 'table', '%s must be a table', field_name)
    for k in pairs(tbl) do
        self:assert(utils.has_value(expected_keys, k), '%s has unknown parameter %q', field_name, k)
    end
end

function config_checks.new(error_class)
    local instance = {
        error_class = error_class,
        assert = config_checks.assert,
        check_luatype = config_checks.check_luatype,
        check_optional_luatype = config_checks.check_optional_luatype,
        check_table_keys = config_checks.check_table_keys
    }
    return instance
end

return config_checks
