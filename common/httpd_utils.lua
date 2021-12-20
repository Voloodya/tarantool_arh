local cartridge = require('cartridge')
local socket = require('socket')
local tenant = require('common.tenant')

local httpd_utils_error = require('errors').new_class('httpd_errors')

local DEFAULT_SERVICE_NAME = 'httpd'
local MIN_TCP_PORT_NUM = 30000
local MAX_TCP_PORT_NUM = 65535

-- Returns default HTTPD service
local function get_default_httpd()
    return cartridge.service_get(DEFAULT_SERVICE_NAME)
end

-- Returns default HTTPD service port number
local function get_default_httpd_port()
    local default_httpd = get_default_httpd()
    if default_httpd == nil then
        return nil, httpd_utils_error:new('Cannot find default httpd service. Is it switched off?')
    end
    return default_httpd.port
end

local function can_use_port(port)
    local sock = socket('AF_INET', 'SOCK_STREAM', 'tcp')
    local ok = sock:bind('0.0.0.0', port)
    sock:close()
    if not ok then
        return false
    end
    return true
end

-- returns maximum number of ports assigned to tenants
local function load_tenants_busy_ports()
    local res = MIN_TCP_PORT_NUM
    local tenant_list, err = cartridge.rpc_call('core', 'tenant_details_list', {})
    if err ~= nil then
        error(err)
    end
    for _, v in ipairs(tenant_list) do
        if v.port ~= nil and v.port > res then
            res = v.port
        end
    end
    return res
end

-- finds free port number. Internal method
local function find_free_httpd_port_number()
    local max_tenant_port = load_tenants_busy_ports()

    local new_port = max_tenant_port == 0
        and get_default_httpd().port
        or max_tenant_port
    new_port = new_port + 1

    while can_use_port(new_port) ~= true do
        if new_port > MAX_TCP_PORT_NUM then
            error('Cannot find free port')
        end
        new_port = new_port + 1
    end
    return new_port
end

local function get_port_number()
    local res, err = cartridge.rpc_call('core', 'tenant_settings_get', {'port'})
    if err ~= nil then
        return nil, err
    end
    return res
end

local function set_port_number(value)
    local res, err = cartridge.rpc_call('core', 'tenant_settings_put', {'port', value}, {leader_only = true})
    if err ~= nil then
        return nil, err
    end
    return res
end

-- Returns number of free port
local function generate_httpd_port_number(uid)
    assert(tenant.is_default(), '"generate_httpd_port_number" can be called under default tenant only!')

    if uid == nil or uid == 'default' then
        return get_default_httpd().port
    end

    local port, err = httpd_utils_error:pcall(find_free_httpd_port_number)
    if err ~= nil then
        return nil, err
    end
    return port
end

return {
    -- Cartridge HTTP server internal name
    DEFAULT_SERVICE_NAME = DEFAULT_SERVICE_NAME,
    -- Returns reference to cartridge HTTPD server
    get_default_httpd = get_default_httpd,
    get_default_httpd_port = get_default_httpd_port,
    -- Returns one of free ports number
    generate_httpd_port_number = generate_httpd_port_number,
    get_port_number = get_port_number,
    set_port_number = set_port_number,
}
