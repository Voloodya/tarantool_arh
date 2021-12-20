local checks = require('checks')
local tracing = require('common.tracing')

local function start_span(name, ...)
    checks('string')
    return tracing.start_span('sandbox: '..name, ...)
end

return {
    start_span = start_span,
}
