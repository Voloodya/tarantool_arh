local module_name = 'common.admin.cluster_config' -- luacheck: ignore

local audit_log = require('audit.log').new(module_name)

local json = require('json')

local cartridge = require('cartridge')

local graphql = require('common.graphql')
local types = require('graphql.types')
local defaults = require('common.defaults')

local errors = require('errors')
local config_error = errors.new_class('Invalid config')
local config_checks = require('common.config_checks').new(config_error)
local config_filter = require('common.config_filter')

local connector_config = require('connector.config.graphql')
local expiration_config = require('account_manager.expiration.graphql')

local hard_limits = types.object{
    name='HardLimits',
    fields={
        scanned=types.long,
        returned=types.long,
    }
}

local function graphql_hard_limits(_, _)
    local limits = cartridge.config_get_readonly('hard-limits') or {}
    return {
        scanned = limits.scanned or defaults.HARD_LIMITS_SCANNED,
        returned = limits.returned or defaults.HARD_LIMITS_RETURNED,
    }
end

local function graphql_set_hard_limits(_, args)
    local limits, err = cartridge.config_get_deepcopy('hard-limits')
    if err ~= nil then
        return nil, err
    end

    local old_value = table.deepcopy(limits)

    if args.scanned == nil and args.returned == nil then
        return graphql_hard_limits()
    end

    if limits == nil then
        limits = {}
    end

    if args.scanned ~= nil then
        limits.scanned = args.scanned
    end

    if args.returned ~= nil then
        limits.returned = args.returned
    end

    local ok, err = cartridge.config_patch_clusterwide({['hard-limits'] = limits})
    if not ok then
        return nil, err
    end

    audit_log.info('hard-limits changed. Old value: %q, new value: %q',
        json.encode(old_value), json.encode(limits))

    return graphql_hard_limits()
end

local function graphql_vshard_timeout(_, _)
    return cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
end

local function graphql_set_vshard_timeout(_, args)
    local old_value, err = cartridge.config_get_deepcopy('vshard-timeout')
    if err ~= nil then
        return nil, err
    end

    local _, err = cartridge.config_patch_clusterwide({['vshard-timeout'] = args.seconds})
    if err ~= nil then
        return nil, err
    end

    audit_log.info('vshard-timeout changed. Old value: %q, new value: %q',
        old_value, args.seconds)

    return graphql_vshard_timeout()
end

local function graphql_default_keep_version_count(_, _)
    return cartridge.config_get_readonly('default_keep_version_count') or defaults.DEFAULT_KEEP_VERSION_COUNT
end

local function graphql_set_default_keep_version_count(_, args)
    local old_value, err = cartridge.config_get_deepcopy('default_keep_version_count')
    if err ~= nil then
        return nil, err
    end

    local _, err = cartridge.config_patch_clusterwide({['default_keep_version_count'] = args.versions})
    if err ~= nil then
        return nil, err
    end

    audit_log.info('default_keep_version_count changed. Old value: %q, new value: %q',
        old_value, args.versions)

    return graphql_default_keep_version_count()
end

local function graphql_force_yield_limits(_, _)
    return cartridge.config_get_readonly('force-yield-limit') or defaults.FORCE_YIELD_LIMIT
end

local function graphql_set_force_yield_limits(_, args)
    local old_value, err = cartridge.config_get_deepcopy('force-yield-limit')
    if err ~= nil then
        return nil, err
    end

    local _, err = cartridge.config_patch_clusterwide({['force-yield-limit'] = args.iterations})
    if err ~= nil then
        return nil, err
    end

    audit_log.info('force-yield-limit changed. Old value: %q, new value: %q',
                   old_value, args.iterations)

    return graphql_force_yield_limits()
end

local function graphql_query_cache_size(_, _)
    return cartridge.config_get_readonly('graphql-query-cache-size') or defaults.GRAPHQL_QUERY_CACHE_SIZE
end

local function graphql_set_query_cache_size(_, args)
    local old_value, err = cartridge.config_get_deepcopy('graphql-query-cache-size')
    if err ~= nil then
        return nil, err
    end

    local _, err = cartridge.config_patch_clusterwide({['graphql-query-cache-size'] = args.size})
    if err ~= nil then
        return nil, err
    end

    audit_log.info('graphql-query-cache-size changed. Old value: %q, new value: %q',
                   old_value, args.size)

    return graphql_query_cache_size()
end

local function init()
    graphql.add_mutation_prefix('admin', 'config', 'Cluster config')
    graphql.add_callback_prefix('admin', 'config', 'Cluster config')

    connector_config.init()
    expiration_config.init()

    graphql.add_callback(
        {schema='admin',
         prefix='config',
         name='vshard_timeout',
         callback='common.admin.cluster_config.graphql_vshard_timeout',
         kind=types.long,
         args={},
         doc="Vshard operations timeout in seconds"})

    graphql.add_callback(
       {schema='admin',
        prefix='config',
        name='default_keep_version_count',
        callback='common.admin.cluster_config.graphql_default_keep_version_count',
        kind=types.long,
        args={},
        doc="Default versioning limit for cluster"})

    graphql.add_mutation(
        {schema='admin',
         prefix='config',
         name='vshard_timeout',
         callback='common.admin.cluster_config.graphql_set_vshard_timeout',
         kind=types.long,
         args={seconds=types.nonNull(types.long)},
         doc="Vshard operations timeout in seconds"})

    graphql.add_mutation(
       {schema='admin',
        prefix='config',
        name='default_keep_version_count',
        callback='common.admin.cluster_config.graphql_set_default_keep_version_count',
        kind=types.long,
        args={versions=types.nonNull(types.long)},
        doc="Default versioning limit for cluster"})

    graphql.add_callback(
        {schema='admin',
         prefix='config',
         name='hard_limits',
         callback='common.admin.cluster_config.graphql_hard_limits',
         kind=hard_limits,
         args={},
         doc="Query operation hard limits"})

    graphql.add_mutation(
        {schema='admin',
         prefix='config',
         name='hard_limits',
         callback='common.admin.cluster_config.graphql_set_hard_limits',
         kind=hard_limits,
         args={scanned = types.long, returned = types.long},
         doc="Query operation hard limits"})

    graphql.add_callback(
        {schema='admin',
         prefix='config',
         name='force_yield_limits',
         callback='common.admin.cluster_config.graphql_force_yield_limits',
         kind=types.long,
         args={},
         doc="System limit for queries in case of disabled hard limits"})

    graphql.add_mutation(
        {schema='admin',
         prefix='config',
         name='force_yield_limits',
         callback='common.admin.cluster_config.graphql_set_force_yield_limits',
         kind=types.long,
         args={iterations = types.nonNull(types.long) },
         doc="System limit for queries in case of disabled hard limits"})

    graphql.add_callback(
        {schema='admin',
         prefix='config',
         name='graphql_query_cache_size',
         callback='common.admin.cluster_config.graphql_query_cache_size',
         kind=types.long,
         args={},
         doc="Graphql query cache maximum size"})

    graphql.add_mutation(
        {schema='admin',
         prefix='config',
         name='graphql_query_cache_size',
         callback='common.admin.cluster_config.graphql_set_query_cache_size',
         kind=types.long,
         args={size = types.nonNull(types.long) },
         doc="Graphql query cache maximum size"})

    graphql.add_callback({
        schema = 'admin',
        prefix = 'config',
        name = 'locked_sections_list',
        doc = 'Returns list of locked sections of config',
        args = {},
        kind = types.list(types.string.nonNull).nonNull,
        callback = 'common.config_utils.list_locked_sections',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'config',
        name = 'locked_sections_add',
        doc = 'Add locked section of config',
        args = {
            name = types.string.nonNull,
        },
        kind = types.list(types.string.nonNull).nonNull,
        callback = 'common.config_utils.add_locked_section',
    })
    graphql.add_mutation({
        schema = 'admin',
        prefix = 'config',
        name = 'locked_sections_delete',
        doc = 'Add locked section of config',
        args = {
            name = types.string.nonNull,
        },
        kind = types.list(types.string.nonNull).nonNull,
        callback = 'common.config_utils.delete_locked_section',
    })
end

local function validate_config(cfg)
    local config = config_filter.compare_and_get(cfg, 'hard-limits', module_name)

    if config ~= nil then
        config_checks:check_luatype('hard-limits', config, 'table')
        if config.scanned ~= nil then
            config_checks:check_luatype('hard-limits.scanned', config.scanned,
                                        'number')
        end
        if config.returned ~= nil then
            config_checks:check_luatype('hard-limits.returned', config.returned,
                                        'number')
        end
    end

    local vshard_timeout = cfg['vshard-timeout']

    if vshard_timeout ~= nil then
        config_checks:check_luatype('vshard-timeout', vshard_timeout,
                                    'number')
    end

    local default_keep_version_count = cfg.default_keep_version_count
    config_checks:check_optional_luatype('default_keep_version_count', default_keep_version_count, 'number')
    if default_keep_version_count ~= nil then
        config_error:assert(default_keep_version_count >= 0, 'default_keep_version_count must be non-negative')
    end

    local force_yield_limit = cfg['force-yield-limit']
    if force_yield_limit ~= nil then
        config_checks:check_luatype('force-yield-limit', force_yield_limit,
                                    'number')
    end

    local graphql_query_cache_size = cfg['graphql-query-cache-size']
    if graphql_query_cache_size ~= nil then
        config_checks:check_luatype('graphql-query-cache-size', graphql_query_cache_size,
                                    'number')

        config_error:assert(graphql_query_cache_size >= 0
                                and graphql_query_cache_size <= 10000,
                            'graphql-query-cache-size must be in range [0, 10000]')
    end

    return true
end

local function  apply_config(cfg)
    graphql.cache_reset(cfg['graphql-query-cache-size'])
    return true
end

return {
    init = init,

    graphql_vshard_timeout = graphql_vshard_timeout,
    graphql_default_keep_version_count = graphql_default_keep_version_count,
    graphql_hard_limits = graphql_hard_limits,
    graphql_set_vshard_timeout = graphql_set_vshard_timeout,
    graphql_set_default_keep_version_count = graphql_set_default_keep_version_count,
    graphql_set_hard_limits = graphql_set_hard_limits,
    graphql_force_yield_limits = graphql_force_yield_limits,
    graphql_set_force_yield_limits = graphql_set_force_yield_limits,
    graphql_query_cache_size = graphql_query_cache_size,
    graphql_set_query_cache_size = graphql_set_query_cache_size,

    validate_config = validate_config,
    apply_config = apply_config,
}
