local cartridge = require('cartridge')
local types = require('graphql.types')

local graphql = require('common.graphql')
local model_graphql = require('common.model_graphql')

local function global_labels_from_graphql(graphql_global_labels)
    if graphql_global_labels == nil then
        return nil
    end

    local global_labels = {}
    for _, label in ipairs(graphql_global_labels) do
        global_labels[label.name] = label.value
    end
    return global_labels
end

local function global_labels_to_graphql(global_labels)
    if global_labels == nil then
        return nil
    end

    local graphql_global_labels = {}
    for key, value in pairs(global_labels) do
        table.insert(graphql_global_labels, {
            name = key,
            value = value,
        })
    end
    return graphql_global_labels
end

local function get_config(_, _)
    local metrics_cfg = cartridge.config_get_readonly('metrics')

    if metrics_cfg == nil then
        return nil
    end

    return {
        export = metrics_cfg['export'],
        global_labels = global_labels_to_graphql(metrics_cfg['global-labels']),
        include = metrics_cfg['include'],
        exclude = metrics_cfg['exclude'],
    }
end

local function replace_box_null_with_nil(tbl)
    for key, value in pairs(tbl) do
        if value == nil then
            value = nil
        end
        tbl[key] = value
    end
end

local function set_config(_, args)
    replace_box_null_with_nil(args)

    local metrics_cfg = box.NULL
    if args.deleted ~= true then
        metrics_cfg = {
            ['export'] = args.export,
            ['global-labels'] = global_labels_from_graphql(args.global_labels),
            ['include'] = args.include,
            ['exclude'] = args.exclude,
        }
    end

    local _, err = cartridge.config_patch_clusterwide({metrics = metrics_cfg})
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function init()
    local type_metrics_export = types.object {
        name = 'MetricsExport',
        description = 'Export of metrics',
        fields = {
            format = types.string,
            path = types.string,
        }
    }
    local type_metrics_export_input = model_graphql.get_input_type({}, type_metrics_export, {})

    local type_metrics_label = types.object {
        name = 'MetricsLabel',
        description = 'Global label of metrics',
        fields = {
            name = types.string.nonNull,
            value = types.string.nonNull,
        }
    }
    local type_metrics_label_input = model_graphql.get_input_type({}, type_metrics_label, {})

    local type_metrics = types.object {
        name = 'MetricsConfig',
        description = 'Config of metrics',
        fields = {
            export = types.list(type_metrics_export),
            global_labels = types.list(type_metrics_label),
            include = types.list(types.string),
            exclude = types.list(types.string),
        }
    }

    graphql.add_callback_prefix('admin', 'metrics', 'Metrics')
    graphql.add_mutation_prefix('admin', 'metrics', 'Metrics')

    graphql.add_callback({
        schema = 'admin',
        prefix = 'metrics',
        name = 'config',
        doc = 'Get config of metrics',
        args = {},
        kind = type_metrics,
        callback = 'common.metrics.graphql.get_config',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'metrics',
        name = 'config',
        doc = 'Set config of metrics',
        callback = 'common.metrics.graphql.set_config',
        args = {
            export = types.list(type_metrics_export_input),
            global_labels = types.list(type_metrics_label_input),
            include = types.list(types.string),
            exclude = types.list(types.string),
            deleted = types.boolean,
        },
        kind = types.string.nonNull,
    })
end

return {
    get_config = get_config,
    set_config = set_config,
    init = init,
}
