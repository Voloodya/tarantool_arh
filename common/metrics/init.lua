local metrics = require('metrics')
local metrics_cpu = require('metrics.psutils.cpu')
local cartridge_metrics = require('cartridge.roles.metrics')
local expirationd_metrics = require('common.metrics.expirationd')

local function init(global_labels)
    metrics.enable_default_metrics()
    metrics.register_callback(metrics_cpu.update)
    metrics.register_callback(expirationd_metrics.update)
    metrics.set_global_labels(global_labels or {})

    cartridge_metrics.set_export({
        {
            path = '/metrics',
            format = 'prometheus'
        },
    })
end

return {
    init = init,
}
