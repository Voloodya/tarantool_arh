local module_name = 'common.http'

local log = require('log.log').new(module_name)

local checks = require('checks')
local fio = require('fio')
local env = require('env')
local request_context = require('common.request_context')
local vars = require('common.vars').new('common.http')
local utils = require('common.utils')

local json = require('json').new()

local cartridge = require('cartridge')
local errors = require('errors')
local auth_error = errors.new_class('authorization_error')

json.cfg({
    encode_use_tostring   = true,
    encode_invalid_as_nil = true
})

vars:new('http_default_listen', '0.0.0.0:8080')
vars:new('build_dir', fio.pathjoin(env.binarydir, '_front_output'))
vars:new('http_server_instance', nil)

local default_charset = "utf-8"
local default_content_type = 'application/json;charset=utf-8'

local function file_mime_type(filename)
    if string.endswith(filename, ".css") then
        return string.format("text/css; charset=%s", default_charset)
    elseif string.endswith(filename, ".js") then
        return string.format("application/javascript; charset=%s", default_charset)
    elseif string.endswith(filename, ".html") then
        return string.format("text/html; charset=%s", default_charset)
    elseif string.endswith(filename, ".jpeg") then
        return string.format("image/jpeg")
    elseif string.endswith(filename, ".jpg") then
        return string.format("image/jpeg")
    elseif string.endswith(filename, ".gif") then
        return string.format("image/gif")
    elseif string.endswith(filename, ".png") then
        return string.format("image/png")
    elseif string.endswith(filename, ".svg") then
        return string.format("image/svg+xml")
    elseif string.endswith(filename, ".ico") then
        return string.format("image/x-icon")
    elseif string.endswith(filename, "manifest.json") then
        return string.format("application/manifest+json")
    end

    return "application/octet-stream"
end

local function get_file_doesnt_exist_response(path)
    return { status = 404, body = string.format("File doesn't exist: '%s'", path) }
end

local function render_file(path)
    local body = utils.read_file(path)

    if body == nil then
        return get_file_doesnt_exist_response(path)
    end

    return {
        status = 200,
        headers = {
            ['content-type'] = file_mime_type(path)
        },
        body = body
    }
end

local function add_route(httpd, options, module_name, function_name)
    checks("table", "table", "string", "string")

    if package.loaded[module_name] == nil then
        error(string.format("http.add_route(): module '%s' doesn't exist",
                            module_name))
    end

    if package.loaded[module_name][function_name] == nil then
        error(string.format("http.add_route(): Function '%s' doesn't exist in module '%s'",
                            function_name, module_name))
    end

    local callback = function(req)
        local fun = package.loaded[module_name][function_name]
        local ok, resp = pcall(fun, req, options)
        if not ok then
            log.error('Http request failed: %s', resp)
            resp = {
                status = 500,
                body = tostring(resp),
            }
        end
        return cartridge.http_render_response(resp)
    end

    httpd:route(options, callback)
end

local function remove_route(httpd, route_name)
    local route_num = httpd.iroutes[route_name]
    if route_num ~= nil then
        table.remove(httpd.routes, route_num)
        httpd.iroutes[route_name] = nil

        for i = route_num, #httpd.routes do
            local route = httpd.routes[i]
            if route.name ~= nil then
                httpd.iroutes[route.name] = i
            end
        end
    end
end

-- Monkeypatch original http handler
local function is_public_endpoint(request)
    return request.endpoint.public ~= false
end

local function handler(self, request)
    local r = self:match(request.method, request.path)
    if r == nil then
        return {
            status = 404,
            headers = {['content-type'] = default_content_type},
        }
    end

    request.endpoint = r.endpoint
    request.tstash   = r.stash

    if self.hooks.before_dispatch ~= nil then
        local _, err = self.hooks.before_dispatch(self, request)
        if err ~= nil then
            return err
        end
    end

    local ok, resp = pcall(r.endpoint.sub, request)
    if not ok then
        if not request_context.is_empty() then
            request_context.clear()
        end
        error(resp)
    end
    if self.hooks.after_dispatch ~= nil then
        self.hooks.after_dispatch(request, resp)
    end
    return resp
end

local function init(httpd)
    local admin_auth = require('common.admin.auth')
    httpd.options.handler = handler

    local cartridge_before_dispatch = httpd.hooks.before_dispatch
    -- Init request context
    -- Check auth
    local function before_dispatch(_, req)
        if cartridge_before_dispatch ~= nil then
            cartridge_before_dispatch()
        end

        local context, err = request_context.parse_options(req.headers)
        if err ~= nil then
            return nil, {
                status = 400,
                body = err,
                headers = {['content-type'] = default_content_type},
            }
        end

        request_context.init(context)
        if not is_public_endpoint(req) then
            local authorized, err = auth_error:pcall(admin_auth.authorize, req)
            if not authorized then
                if err ~= nil then
                    log.error('Authorization error: %s', err)
                end
                request_context.clear()

                local status = 401
                local body = 'invalid credentials'
                if err ~= nil then
                    status = err.is_error and 500 or status
                    body = err.message
                end

                return nil, {
                    status = status,
                    body = body,
                    headers = { ['content-type'] = default_content_type },
                }
            end
        end
    end

    local cartridge_after_dispatch = httpd.hooks.after_dispatch
    -- Clear request context
    -- Setup charset
    local function after_dispatch(_, resp)
        if cartridge_after_dispatch ~= nil then
            cartridge_after_dispatch()
        end

        resp.headers = resp.headers or {}
        resp.headers['request-id'] = request_context.get().id
        if resp.headers['content-type'] == nil then
            resp.headers['content-type'] = default_content_type
        elseif string.find(resp.headers['content-type'], 'charset') == nil then
            resp.headers['content-type'] = resp.headers['content-type'] .. ';charset=utf-8'
        end

        request_context.clear()
    end

    httpd:hook('before_dispatch', before_dispatch)
    httpd:hook('after_dispatch', after_dispatch)
end

return {
    init = init,
    add_route = add_route,
    remove_route = remove_route,
    render_file = render_file,
    file_mime_type = file_mime_type,
}
