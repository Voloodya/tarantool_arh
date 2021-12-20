#!/usr/bin/env tarantool

package.setsearchroot()
local log = require('log')
local argparse = require('cartridge.argparse')
local metrics = require('metrics')
local http_server = require('http.server')

local metrics_cpu = require('metrics.psutils.cpu')
local prometheus = require('metrics.plugins.prometheus')

local function render_metrics(_)
    metrics_cpu.update()
    return prometheus.collect_http()
end

require('common.netbox_monkeypatch').monkeypatch_netbox_call()
require('cartridge.stateboard').cfg()

box.schema.func.create('__netbox_call_with_fiber_storage', {if_not_exists = true})
box.schema.user.grant('client', 'execute', 'function', '__netbox_call_with_fiber_storage', {if_not_exists = true})

local opts = argparse.get_opts({http_port = 'number', http_host = 'string'})
if opts.http_port ~= nil then
    metrics.enable_default_metrics()
    local http_host = opts.http_host or '0.0.0.0'
    local httpd = http_server.new(http_host, opts.http_port, { log_requests = false })
    httpd:route({path = '/metrics', method = 'GET'}, render_metrics)
    log.info('Start to serve metrics on %s:%s', http_host, opts.http_port)
    httpd:start()
end
