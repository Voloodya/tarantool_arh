local fiber = require('fiber')

local checks = require('checks')
local errors = require('errors')

local request_context = require('common.request_context')

local funcall_error = errors.new_class("funcall_error")

local call_loadproc = box.internal.call_loadproc
local function funcall(func_name, ...)
    checks("string")
    local func, selfobj = call_loadproc(func_name)
    if selfobj ~= nil then
        return func(selfobj, ...)
    else
        return func(...)
    end
end

local storage_key = request_context.get_storage_key()

local function call_with_fiber_storage(storage, func_name, arguments)
    fiber.self().storage[storage_key] = storage
    arguments = arguments or {}
    return funcall_error:pcall(funcall, func_name, unpack(arguments))
end

_G.__netbox_call_with_fiber_storage = call_with_fiber_storage

local function conn_call_wrapped(self, call_fn, func_name, arguments, options)
    local storage = fiber.self().storage[storage_key]
    return call_fn(self, '__netbox_call_with_fiber_storage', {storage, func_name, arguments}, options)
end

local function netbox_connect_wrapped(netbox_connect_original, ...)
    local conn, err = netbox_connect_original(...)
    if not conn then
        return nil, err
    end

    local conn_call = conn.call
    function conn.call(self, ...)
        return conn_call_wrapped(self, conn_call, ...)
    end

    return conn
end

--[[ This code block hook a net.box call and
  - pass fiber.storage trough network call using additional argument
  - checks the second, and third returned object.
    If it looks like an error object, it reconstructs the metatable
    and enriches stack trace with current instance's stack.
]]
local function monkeypatch_netbox_call()
    if rawget(_G, "_error_netbox_ishooked") then
        return
    end

    rawset(_G, "_error_netbox_ishooked", true)

    local netbox = require('net.box')
    local netbox_connect_original = netbox.connect
    netbox.connect = function(...)
        return netbox_connect_wrapped(netbox_connect_original, ...)
    end
end

return {
    monkeypatch_netbox_call = monkeypatch_netbox_call,
}
