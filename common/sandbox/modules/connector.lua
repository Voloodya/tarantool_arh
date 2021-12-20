local connector = require('common.sandbox.connector')
local tracing = require('common.tracing')

local function send(output_name, obj, output_options)
    local span = tracing.start_span('sandbox: send_to_output')
    local res, err = connector.send_to_output(output_name, obj, output_options or {})
    span:finish({error = err})
    return res, err
end

return {
    send = send,
}
