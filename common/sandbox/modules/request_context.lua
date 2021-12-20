local request_context = require('common.request_context')

return {
    get = function()
        return table.deepcopy(request_context.get())
    end
}
