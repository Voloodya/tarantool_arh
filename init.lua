#!/usr/bin/env tarantool

if os.getenv('TDG_COVERAGE') then
    require('luacov')
    box.ctl.on_shutdown(function()
        require('luacov.runner').shutdown()
    end)
end

local fio = require('fio')

package.setsearchroot()
require('strict').on()
local script_dir = debug.sourcedir()
fio.chdir(script_dir)
local env = require('env')

if not env.dev_mode then
    require('strict').off()
    require('checks')
    package.loaded['checks'] = function() end
else
    -- Speedup discovery in dev environment
    require('vshard.consts').BUCKET_CHUNK_SIZE = 30000
    require('vshard.consts').DISCOVERY_WORK_INTERVAL = 0.01
end

local default_workdir = fio.pathjoin(script_dir, 'dev/output/srv/')

local argparse = require('cartridge.argparse')
local args, err = argparse.parse()
assert(args, tostring(err))

local vars = require('common.vars')
vars.init()

local system_log = require('log')

require('log.log').init(args.instance_name or '')

local audit_log = require('audit.log')
audit_log.init()

-- @monkeypatch
-- Disable ddl manager role since we don't use it
-- and it pollute config sections.

local black_list = {
    'cartridge.roles.ddl-manager',
}

require('cartridge.roles') -- Setup vars
local cartridge_roles_vars = require('cartridge.vars').new('cartridge.roles')
local implicit_roles = cartridge_roles_vars.implicit_roles
for _, role_name in ipairs(black_list) do
    for i, implicit_role in ipairs(implicit_roles) do
        if role_name == implicit_role then
            table.remove(implicit_roles, i)
        end
    end
end

local console = require('console')

local term = require('common.term')
local admin = require('common.admin')
local http = require('common.http')
local graphql = require('common.graphql')
local metrics = require('common.metrics')
local startup_tune = require('common.startup_tune')
local netbox_monkeypatch = require('common.netbox_monkeypatch')
local membership = require('membership')
local cartridge = require('cartridge')
local front = require('frontend-core')
vshard = require('vshard')

startup_tune.init()

netbox_monkeypatch.monkeypatch_netbox_call()

if rawget(_G, "is_initialized") == nil then
    _G.is_initialized = function() return false end
end

local replication_synchro_timeout = require('common.defaults').VSHARD_TIMEOUT / 2

local tdg_roles = require('common.roles')
local _, err = cartridge.cfg(
    {
        workdir = default_workdir,
        alias = args.instance_name,
        http_port = '8080',
        roles = tdg_roles,
        auth_backend_name = 'auth',
        upgrade_schema = true,
    }, {
        readahead = env.readahead,
        memtx_max_tuple_size = env.memtx_max_tuple_size,
        vinyl_max_tuple_size = env.vinyl_max_tuple_size,
        custom_proc_title = args.instance_name,
        replication_synchro_quorum = 'N / 2 + 1',
        replication_synchro_timeout = replication_synchro_timeout,
})
if err ~= nil then
    system_log.error('%s', tostring(err))
    os.exit(1)
end

local function get_self_alias()
    local opts = argparse.get_opts({ instance_name = 'string',  alias = 'string' }) or {}
    return opts.alias or opts.instance_name
end

local function init_new()
    local httpd = cartridge.service_get('httpd')
    http.init(httpd)
    graphql.init()
    metrics.init({ alias = get_self_alias() })
    admin.init()
end

local ok, err = xpcall(init_new, debug.traceback)
if not ok then
    system_log.error('%s', tostring(err))
    os.exit(1)
end

-- Frontend html, js content built-in into lua table
local tdg_front_bundle = require('tdg-front-bundle')
front.add('tdg', tdg_front_bundle)
front.set_variable('cartridge_hide_all_rw', true)

local tdg_opts, err = argparse.get_opts({
        bootstrap = 'boolean',
        watchdog_timeout = 'number'
    })

if err ~= nil then
    system_log.error('%s', tostring(err))
    os.exit(1)
end

if tdg_opts.watchdog_timeout then
    env.watchdog_timeout = tdg_opts.watchdog_timeout
end

local workdir = args.workdir or default_workdir
if tdg_opts.bootstrap and #fio.glob(workdir .. '/*.snap') == 0 then

    system_log.info('Bootstrapping in %s', workdir)

    require("membership.options").ACK_TIMEOUT_SECONDS = 0.5
    local all = {
        ['vshard-storage'] = true,
        ['vshard-router'] = true,
        ['failover-coordinator'] = true,
        ['connector'] = true,
        ['storage'] = true,
        ['runner'] = true,
        ['core'] = true,
    }

    local _, err = cartridge.admin_join_server({
            uri = membership.myself().uri,
            roles = all,
    })

    if err ~= nil then
        system_log.warn('%s', tostring(err))
    else
        local _, err = cartridge.admin_bootstrap_vshard()
        if err ~= nil then
            system_log.error('%s', tostring(err))
            os.exit(1)
        end
    end
end

_G.is_initialized = function()
    return type(box.cfg) == 'table' and box.info.status == 'running' and cartridge.is_healthy() == true
end

if term.isatty(term.STDOUT_NO) then
    console.start()
    os.exit(0)
end
