package = 'tdg'
version = 'scm-1'
source  = {
    url = '/dev/null',
}
dependencies = {
    'tarantool',
    'cartridge == scm', -- from submodule
    'vshard',           -- as cartridge dependency
    'errors',           -- as cartridge dependency
    'http',             -- as cartridge dependency
    'watchdog == 1.1.1-1',
    'expirationd = 1.1.1-1',
    'metrics == 0.11.0',
    'checks == 3.1.0-1',
    'avro-schema == 3.0.6',
    'tracing == 0.1.1-1',
    'luarapidxml == 2.0.2-1',
    'icu-date == 1.4.0-1',
    'smtp == 0.0.6',
    'cron-parser == scm',
    'odbc == 1.0.1-1',
    'kafka == 1.3.1-1',
    'ldap == 1.0.2-1',
}

build = {
    type = 'make',
    makefile = 'Makefile',
    build_variables = {
        WITHOUT_FRONT = "$(WITHOUT_FRONT)",
        WITHOUT_DOC = "$(WITHOUT_DOC)",
        VERSION = "$(VERSION)"
    },
    install_pass = false,
}
