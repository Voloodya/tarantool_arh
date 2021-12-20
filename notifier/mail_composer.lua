local utils = require('common.utils')

local function produce_body(template, objects)
    local body = {}

    local n = 0

    local obj = objects:pop()
    while obj do
        n = n + 1
        table.insert(body, string.format(
            template, obj.id, utils.nsec_to_iso8601_str(obj.time), obj.reason))
        obj = objects:pop()
    end

    return n, table.concat(body, '\n')
end

local function new_objects(objects)
    local subject = 'New object(s) has been added to the repair queue'

    local body_template = [[
Object ID: %s
Time: %s
Reason:
%s

]]

    local n, body = produce_body(body_template, objects)
    return { objects_num = n, subject = subject, body = body }
end

local function repair_failures(objects)
    local subject = 'The object(s) can not be repaired'

    local body_template = [[
Object ID: %s
Time: %s
Reason:
%s

]]

    local n, body = produce_body(body_template, objects)
    return { objects_num = n, subject = subject, body = body }
end

return {
    new_objects = new_objects,
    repair_failures = repair_failures
}
