local module_name = 'notifier.server'

local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)

local notifier = require('notifier.notifier')

vars:new('is_master', false)

local function apply_config(conf, opts)
    local conf = conf['notifier']
    if not conf then
        return
    end

    if vars.is_master == true then
        if opts.is_master ~= true then
            notifier.unsubscribe_all()

            notifier.deinit()
            vars.is_master = false
        end
    else
        if opts.is_master == true then
            local smtp_client = nil

            local mail_server = conf.mail_server
            if mail_server then
                smtp_client = require('notifier.smtp_client')
                smtp_client.init(
                    mail_server.url,
                    mail_server.from,
                    mail_server.username,
                    mail_server.password,
                    mail_server.timeout,
                    {skip_verify_host=mail_server.skip_verify_host}
                )
            end

            local mail_composer = require('notifier.mail_composer')

            notifier.init(smtp_client, mail_composer, notifier.INITIAL_TIMEOUT)
            vars.is_master = true
            log.info("Notifier changed")
        end
    end

    if vars.is_master == true then
        notifier.unsubscribe_all()
        if conf.users then
            for _, user in ipairs(conf.users) do
                notifier.subscribe(user)
            end
        end
    end
end

return {
    apply_config = apply_config,
}
