local types = require('graphql.types')

local graphql = require('common.graphql')
local utils = require('common.utils')
local tenant = require('common.tenant')

local severities = require('log.severities')
local common_log_storage = require('storage.common_log.storage')

local function transform_to_graphql(entry)
    entry.level = severities.to_string(entry.severity)
    entry.severity = nil
    entry.time = utils.nsec_to_iso8601_str(entry.time)
end

local function get_logs(_, args)
    local options = {
        node = args.node,
        module = args.module,
        text = args.message,
        request_id = args.request_id,
        cursor = args.cursor,
        limit = args.limit,
    }

    if args.level ~= nil then
        options.severity = severities.from_string(args.level)
    end

    if args.from ~= nil then
        options.from = utils.iso8601_str_to_nsec(args.from)
    end

    if args.to ~= nil then
        options.to = utils.iso8601_str_to_nsec(args.to)
    end

    local entries, err = common_log_storage.filter(options)
    if err ~= nil then
        return nil, err
    end

    for _, entry in ipairs(entries) do
        transform_to_graphql(entry)
    end

    return entries
end

local function truncate()
    local _, err = common_log_storage.clear()
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function to_graphql_config(cfg)
    return {
        enabled = cfg.enabled or false,
        severity = cfg.severity,
        max_msg_in_log = cfg.max_msg_in_log,
        max_log_size = cfg.max_log_size,
        remove_older_n_hours = cfg.remove_older_n_hours,
    }
end

local function get_config(_, _)
    local logger = tenant.get_cfg('logger')
    if logger == nil then
        return {}
    end

    return to_graphql_config(logger)
end

local function set_config(_, args)
    local logger = tenant.get_cfg_deepcopy('logger') or {}

    if args.enabled ~= nil then
        logger.enabled = args.enabled
    end
    if args.severity ~= nil then
        logger.severity = args.severity
    end
    if args.max_msg_in_log ~= nil then
        logger.max_msg_in_log = args.max_msg_in_log
    end
    if args.max_log_size ~= nil then
        logger.max_log_size = args.max_log_size
    end
    if args.remove_older_n_hours ~= nil then
        logger.remove_older_n_hours = args.remove_older_n_hours
    end

    local _, err = tenant.patch_config({ logger = logger })
    if err ~= nil then
        return nil, err
    end

    return to_graphql_config(logger)
end

local function init()
    types.object {
        name = 'Logs',
        description = 'An application logs',
        fields = {
            time = types.string.nonNull,
            level = types.string.nonNull,
            node = types.string.nonNull,
            module = types.string.nonNull,
            message = types.string.nonNull,
            request_id = types.string,
            cursor = types.string.nonNull
        },
        schema = 'admin',
    }

    local logs_config = types.object {
        name = 'LogsConfig',
        description = 'Config of logging',
        fields = {
            enabled = types.boolean,
            severity = types.string,
            max_msg_in_log = types.long,
            max_log_size = types.float,
            remove_older_n_hours = types.float,
        },
        schema = 'admin',
    }

    graphql.add_callback_prefix('admin', 'logs', 'Logs endpoint')
    graphql.add_mutation_prefix('admin', 'logs', 'Logs endpoint')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'logs',
        name = 'get',
        doc = 'Get logs',
        args = {
            level = types.string,
            node = types.string,
            module = types.string,
            from = types.string,
            to = types.string,
            message = types.string,
            request_id = types.string,
            cursor = types.string,
            limit = types.int
        },
        kind = types.list('Logs'),
        callback = 'log.graphql.get_logs',
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'logs',
        name = 'config',
        doc = 'Get configuration of logger',
        args = {},
        kind = logs_config,
        callback = 'log.graphql.get_config',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'logs',
        name = 'config',
        doc = 'Change configuration of logger',
        args = {
            enabled = types.boolean,
            severity = types.string,
            max_msg_in_log = types.long,
            max_log_size = types.float,
            remove_older_n_hours = types.float,
        },
        kind = logs_config,
        callback = 'log.graphql.set_config',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'logs',
        name = 'truncate',
        doc = 'Clear common log',
        kind = types.string.nonNull,
        callback = 'log.graphql.truncate',
    })
end

return {
    get_logs = get_logs,
    truncate = truncate,
    get_config = get_config,
    set_config = set_config,
    init = init,
}
