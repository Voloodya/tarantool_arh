local types = require('graphql.types')
local graphql = require('common.graphql')
local utils = require('common.utils')
local tenant = require('common.tenant')

local severities = require('audit.severities')

local audit_log_storage = require('storage.audit_log.storage')

local function init()
    types.object {
        name = 'Audit_log',
        description = 'A list of audit log entries',
        fields = {
            time = types.string.nonNull,
            severity = types.string.nonNull,
            request_id = types.string.nonNull,
            subject = types.string.nonNull,
            subject_id = types.string,
            module = types.string.nonNull,
            message = types.string.nonNull,
            cursor = types.string.nonNull,
        },
        schema = 'admin',
    }

    graphql.add_callback_prefix('admin', 'audit_log', 'Audit log')
    graphql.add_mutation_prefix('admin', 'audit_log', 'Audit log')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'audit_log',
        name = 'get',
        doc = 'Get audit log',
        args = {
            cursor = types.string,
            limit = types.int,
            from = types.string,
            to = types.string,
            severity = types.string,
            request_id = types.string,
            subject = types.string,
            subject_id = types.string,
            module = types.string,
            text = types.string,
        },
        kind = types.list('Audit_log'),
        callback = 'audit.graphql.get_audit_log',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'audit_log',
        name = 'enabled',
        doc = 'Is logging enabled or disabled',
        kind = types.boolean,
        callback = 'audit.graphql.get_enabled',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'audit_log',
        name = 'enabled',
        doc = 'Enable or disable logging',
        args = { value = types.boolean.nonNull },
        kind = types.string.nonNull,
        callback = 'audit.graphql.set_enabled',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'audit_log',
        name = 'severity',
        doc = 'Setting audit log level',
        args = { value = types.string.nonNull },
        kind = types.string.nonNull,
        callback = 'audit.graphql.set_severity',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'audit_log',
        name = 'clear',
        doc = 'Clear audit log',
        kind = types.string.nonNull,
        callback = 'audit.graphql.clear',
    })
end

local function transform_to_graphql(entry)
    entry.severity = severities.to_string(entry.severity)
    entry.time = utils.nsec_to_iso8601_str(entry.time)
end

local function get_audit_log(_, args)
    local options = {
        cursor = args.cursor,
        limit = args.limit,
        request_id = args.request_id,
        subject = args.subject,
        subject_id = args.subject_id,
        module = args.module,
        text = args.text,
    }

    if args.severity ~= nil then
        local severity, err = severities.from_string(args.severity)
        if err ~= nil then
            return nil, err
        end
        options.severity = severity
    end

    if args.from ~= nil then
        options.from = utils.iso8601_str_to_nsec(args.from)
    end

    if args.to ~= nil then
        options.to = utils.iso8601_str_to_nsec(args.to)
    end

    local entries, err = audit_log_storage.filter(options)
    if err ~= nil then
        return nil, err
    end

    for _, entry in ipairs(entries) do
        transform_to_graphql(entry)
    end
    return entries
end

local function get_enabled(_, _)
    local audit_log, err = tenant.get_cfg('audit_log')
    if err ~= nil then
        return nil, err
    end

    -- If no section audit_log in config
    if audit_log == nil or audit_log.enabled == nil then
        return true
    end
    return audit_log['enabled']
end

local function set_enabled(_, args)
    local audit_log = tenant.get_cfg_deepcopy('audit_log') or {}
    audit_log.enabled = args.value

    local _, err = tenant.patch_config({ audit_log = audit_log })
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function set_severity(_, args)
    local audit_log = tenant.get_cfg_deepcopy('audit_log') or {}
    audit_log.severity = args.value

    local _, err = tenant.patch_config({ audit_log = audit_log })
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function clear(_, _)
    local _, err = audit_log_storage.clear()
    if err ~= nil then
        return nil, err
    end
    return 'ok'
end

return {
    init = init,
    get_audit_log = get_audit_log,
    get_enabled = get_enabled,
    set_enabled = set_enabled,
    set_severity = set_severity,
    clear = clear,
}
