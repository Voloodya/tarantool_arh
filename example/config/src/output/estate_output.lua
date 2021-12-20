local repository = require('repository')

return {
    call = function(obj)
        repository.push_job('output.notify_about_estate.call', { obj.address.district })

        return { obj = obj }
    end
}
