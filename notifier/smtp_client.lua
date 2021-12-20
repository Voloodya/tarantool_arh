local checks = require('checks')

local smtp = require('smtp').new()

local settings

local function init(url, from, username, password, timeout, opts)
    checks('string', 'string', 'string', 'string', 'number', '?table')

    settings = {
        url = url,
        from = from,
        username = username,
        password = password,
        timeout = timeout,
        skip_verify_host = opts.skip_verify_host,
    }
end

local function send(to, subject, body)
    checks('string', 'string', 'string')

    if not settings then
        return true
    end

    local response = smtp:request(
        settings.url,
        settings.from,
        to,
        body,
        {
            timeout = settings.timeout,
            subject = subject,
            username = settings.username,
            password = settings.password,
            verify_host = not settings.skip_verify_host
        })

    if response.status == 250 then
        return true
    else
        return nil, response.reason
    end
end

return {
    init = init,
    send = send
}
