local cartridge = require('cartridge')

local errors = require('errors')

local sandbox_send_error = errors.new_class('send from sandbox failed')

local function send_to_output(output, obj, output_options)
    return sandbox_send_error:pcall(cartridge.rpc_call, 'connector', 'handle_output',
        { output, { obj = obj, output_options = output_options } },
        { leader_only = true })
end

return {
    send_to_output = send_to_output
}
