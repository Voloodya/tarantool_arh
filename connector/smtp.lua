local module_name = 'connector.smtp'

local errors = require('errors')
local checks = require('checks')
local uri = require('uri')
local smtp = require('smtp').new()
local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local json = require('json')

vars:new('outputs')

local output_error = errors.new_class('connect output error')

local function get_output_by_name(name)
    if vars.outputs == nil then
        return nil
    end

    if vars.outputs[name] == nil then
        return nil
    end

    return vars.outputs[name]
end

local function send_mail(output, to, body, subject, from)
    checks("string|table", "string|table", "string", "string|nil", "string|nil")

    if type(output) == "string" then
        output = get_output_by_name(output)
        if output == nil then
            return nil, output_error:new("No output %s found for smtp send", output)
        end
    end

    local res = smtp:request(output.connect_url,
        from or output.from,
        to,
        body,
        {
            subject = subject or output.subject,
            username = output.username,
            password = output.password,
            timeout = output.timeout,
            ssl_cert = output.ssl_cert,
            ssl_key = output.ssl_key
        }
    )
    if res.status ~= 250 then
        local err = output_error:new(res.reason)
        log.error('%s: %s [%d]', output.name, res.reason, res.status)
        err.status = res.status
        err.body = res.body
        return nil, err
    end
    return res, nil
end

local function send_object(self, obj, options)
    checks("table", "string|table", "table")

    if options.to == nil then
        return nil, output_error:new("No recipients set in smtp.to")
    end

    local str_obj = type(obj) == "string" and obj or json.encode(obj)

    return send_mail(self,
        options.to,
        str_obj,
        options.subject or self.subject,
        options.from or self.from
    )
end

local function create_sender(output)
    local self = {}
    local u_parsed = uri.parse(output.url)

    self.name = output.name
    self.host = u_parsed.host
    self.connect_url = string.format("smtp://%s:%s", u_parsed.host, (u_parsed.service or "25"))
    self.username = u_parsed.login
    self.password = u_parsed.password
    self.from = output.from or string.format("%s@%s",
        (u_parsed.login or "anonymous"), u_parsed.host
    )
    self.subject = output.subject
    self.timeout = output.timeout or 5
    self.ssl_cert = output.ssl_cert
    self.ssl_key = output.ssl_key

    vars.outputs = vars.outputs or {}
    vars.outputs[output.name] = self
end

local function remove_sender(output_name)
    vars.outputs = vars.outputs or {}
    vars.outputs[output_name] = nil
end

return {
    send_mail = send_mail,
    send_object = send_object,
    create_sender = create_sender,
    remove_sender = remove_sender,
    get_output_by_name = get_output_by_name,
}
