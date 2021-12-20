local log = require('log')
local repository = require('repository')
local connector = require('connector')

return {
    call = function(district)
        local subscribers = repository.find('Subscriber', {{'district', '==', district}})
        for _, subscriber in ipairs(subscribers) do
            local _, err = connector.send('to_smtp', 'New estate at ' .. district .. ' district!', {
                to = subscriber.email,
                subject = 'New estate!',
            })
            if (err ~= nil) then
                log.error(err)
            end
        end

        return tostring(#subscribers) .. ' emails sent'
    end
}
