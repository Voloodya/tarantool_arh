local roles = {
    'cartridge.roles.vshard-storage',
    'cartridge.roles.vshard-router',
    'cartridge.roles.metrics',

    -- Storage
    'roles.storage',

    -- Connector
    'roles.connector',

    -- Runner
    'roles.runner',

    -- Core
    'roles.core',

    -- Permanent
    'roles.permanent.common',
    'roles.permanent.tracing',
    'roles.permanent.account_provider',
    'roles.permanent.maintenance',
    'roles.permanent.watchdog',
}

return roles
