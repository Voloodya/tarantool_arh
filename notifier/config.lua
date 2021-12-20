local checks = require('checks')
local utils = require('common.utils')
local errors = require('errors')
local config_error = errors.new_class('Invalid notifier config')
local config_checks = require('common.config_checks').new(config_error)

local function validate(conf)
    checks('table')
    local cfg = conf['notifier'] or {}
    config_checks:check_luatype('notifier config', cfg, 'table')

    if cfg.mail_server then
        local cfg = cfg.mail_server
        local field = 'notifier.mail_server'

        config_checks:check_luatype(field..'.url', cfg.url, 'string')
        config_checks:check_luatype(field..'.from', cfg.from, 'string')
        config_checks:check_luatype(field..'.username', cfg.username, 'string')
        config_checks:check_luatype(field..'.password', cfg.password, 'string')
        config_checks:check_luatype(field..'.timeout', cfg.timeout, 'number')

        if cfg.skip_verify_host ~= nil then
            config_checks:check_luatype(field..'.skip_verify_host', cfg.skip_verify_host, 'boolean')
        end

        local known_keys = {
            'url', 'from', 'username', 'password', 'timeout', 'skip_verify_host'}

        config_checks:check_table_keys(field, cfg, known_keys)
    end

    if cfg.users then
        local cfg = cfg.users
        local field = 'notifier.users'
        config_checks:check_luatype(field, cfg, 'table')

        config_checks:assert(utils.is_array(cfg), '%s must be an array', field)

        for n = 1, table.maxn(cfg) do
            local field = string.format('%s[%d]', field, n)
            local user = cfg[n]

            config_checks:check_luatype(field, user, 'table')
            config_checks:check_luatype(field..'.id', user.id, 'string')
            config_checks:check_luatype(field..'.name', user.name, 'string')
            config_checks:assert(#user.name:strip() > 0, '%%.name expected to be non-empty', field)
            config_checks:check_luatype(field..'.addr', user.addr, 'string')

            config_checks:check_table_keys(field, user, { 'id', 'name', 'addr' })
        end
    end



    return true
end

return {
    validate = function(...)
        return config_error:pcall(validate, ...)
    end
}
