local digest = require('digest')
local fio = require('fio')
local cartridge = require('cartridge')
local errors = require('errors')
local graphql = require('common.graphql')
local defaults = require('common.defaults')
local graphql_types = require('graphql.types')
local config_utils = require('common.config_utils')
local migrations_validation = require('storage.migrations.validation')

local dry_run_error = errors.new_class('migration_dry_run')

local function apply(_, args)
    local ok, data = pcall(digest.base64_decode, args.config_base64)
    if not ok then
        return nil, data
    end

    local comment
    if args.comment ~= nil then
        comment = args.comment
    end

    local admin = require('common.admin')
    local _, err = admin.upload_config(data, comment, {migration = args.sections})
    if err ~= nil then
        return nil, err
    end

    return true
end

local DEFAULT_DRY_RUN_TIMEOUT = 10
local function dry_run(_, args)
    local ok, data = pcall(digest.base64_decode, args.config_base64)
    if not ok then
        return nil, data
    end

    local ratio
    if args.ratio ~= nil then
        if args.ratio <= 0 then
            return nil, dry_run_error:new('"ratio" option expected to be greater than 0')
        end
        if args.ratio > 1 then
            return nil, dry_run_error:new('"ratio" option expected to be in range (0, 1]')
        end
        ratio = args.ratio
    end

    local at_least
    if args.at_least ~= nil then
        if args.at_least <= 0 then
            return nil, dry_run_error:new('"at_least" option expected to be greater than 0')
        end
        at_least = args.at_least
    end

    local tmpdir, err = config_utils.unzip_data_to_tmpdir(data)
    if err ~= nil then
        return nil, err
    end

    local cwcfg, err = config_utils.load_clusterwide_config(tmpdir)
    fio.rmtree(tmpdir)
    if err ~= nil then
        return nil, err
    end

    local migration = args.sections
    migrations_validation.validate(cwcfg:get_readonly(), migration)

    local cfg = cwcfg:get_readonly()
    local _, err = vshard.router.map_callrw('vshard_proxy.migration_dry_run', {cfg, migration, ratio, at_least},
        {timeout = DEFAULT_DRY_RUN_TIMEOUT})
    if err ~= nil then
        return nil, err
    end
    return true
end

local function stats()
    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
    local resp, err = vshard.router.map_callrw('vshard_proxy.migration_stats', {}, {timeout = timeout})
    if err ~= nil then
        return nil, err
    end

    local result = {}
    for _, value in pairs(resp) do
        table.insert(result, value[1])
    end

    return result
end

local function init()
    graphql.add_mutation_prefix('admin', 'migration', 'Migration management')
    graphql.add_callback_prefix('admin', 'migration', 'Migration management')

    local migration_type_code = graphql_types.inputObject({
        name = 'Migration_type_code',
        description = 'Code of migration for specified type',
        fields = {
            type_name = graphql_types.string.nonNull,
            code = graphql_types.string.nonNull,
        },
        schema = 'admin',
    })

    local migration_storage_stat = graphql_types.object({
        name = 'Migration_storage_stat',
        description = 'Progress of migration on storage',
        fields = {
            replicaset_uuid = graphql_types.string.nonNull,
            type_name = graphql_types.string.nonNull,
            transformed = graphql_types.long.nonNull,
            remained = graphql_types.long.nonNull,
        },
        schema = 'admin',
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'migration',
        name = 'apply',
        callback = 'storage.migrations.graphql.apply',
        kind = graphql_types.boolean.nonNull,
        args = {
            config_base64 = graphql_types.string.nonNull,
            sections = graphql_types.list(migration_type_code).nonNull,
            comment = graphql_types.string,
        },
    })

    graphql.add_mutation({
        schema = 'admin',
        prefix = 'migration',
        name = 'dry_run',
        callback = 'storage.migrations.graphql.dry_run',
        kind = graphql_types.boolean.nonNull,
        args = {
            config_base64 = graphql_types.string.nonNull,
            sections = graphql_types.list(migration_type_code).nonNull,
            at_least = graphql_types.long,
            ratio = graphql_types.float,
        },
    })

    graphql.add_callback({
        schema = 'admin',
        prefix = 'migration',
        name = 'stats',
        callback = 'storage.migrations.graphql.stats',
        kind = graphql_types.list(migration_storage_stat),
        args = {},
    })
end

return {
    init = init,
    apply = apply,
    dry_run = dry_run,
    stats = stats,
}
