local module_name = 'storage.migrations'

local fiber = require('fiber')
local key_def = require('key_def')
local vars = require('common.vars').new(module_name)
local model_flatten = require('common.model_flatten')
local model_utils = require('common.model_utils')
local model_ddl = require('common.model_ddl')
local model = require('common.model')
local tenant = require('common.tenant')
local defaults = require('common.defaults')
local log = require('log.log').new(module_name)

vars:new('active_migrations')

local BASE_SPACE_NAME = 'tdg_migrations'
local OLD_SPACE_NAME_PREFIX = 'old_'

local function get_space_name()
    return tenant.get_space_name(BASE_SPACE_NAME)
end

local function get_space()
    local space_name = get_space_name()
    local space = box.space[space_name]
    assert(space, 'migration space is not initialized')
    return space
end

local std_env = {
    assert = assert,
    error = error,
    ipairs = ipairs,
    next = next,
    pairs = pairs,
    pcall = pcall,
    xpcall = xpcall,
    select = select,
    tonumber = tonumber,
    tonumber64 = tonumber64,
    tostring = tostring,
    type = type,
    unpack = unpack,
    print = print,

    -- modules
    math = math,
    os = os,
    io = io,
    string = string,
    table = table,
    bit = require('bit'),
    decimal = require('decimal'),
    uuid = require('uuid'),
    json = require('json'),
    yaml = require('yaml'),
    box = {
        NULL = box.NULL,
    }
}

local function rename_spaces(type_name)
    local tenant_prefix = tenant.prefix()
    local space_name = model_ddl.get_space_name(type_name, tenant_prefix)
    local history_space = model_ddl.get_history_space_name(type_name, tenant_prefix)
    local expiration_space = model_ddl.get_expiration_space_name(type_name, tenant_prefix)

    if box.space[space_name] ~= nil then
        local new_space_name = OLD_SPACE_NAME_PREFIX .. space_name
        log.info('Rename %q to %q', space_name, new_space_name)
        box.space[space_name]:rename(new_space_name)
    else
        log.info('Space %q is not found', space_name)
    end
    if box.space[history_space] ~= nil then
        local new_space_name = OLD_SPACE_NAME_PREFIX .. history_space
        log.info('Rename %q to %q', history_space, new_space_name)
        box.space[history_space]:rename(new_space_name)
    else
        log.info('Space %q is not found', history_space)
    end
    if box.space[expiration_space] ~= nil then
        local new_space_name = OLD_SPACE_NAME_PREFIX .. expiration_space
        log.info('Rename %q to %q', expiration_space, new_space_name)
        box.space[expiration_space]:rename(new_space_name)
    else
        log.info('Space %q is not found', expiration_space)
    end
end

local function add_migration(type_name, code, mdl, ddl)
    if vars.active_migrations == nil then
        vars.active_migrations = {}
    end

    -- TODO: Ban migrations for types that has active migrations
    local space = get_space()
    rename_spaces(type_name)
    space:replace({nil, type_name, code, mdl, ddl})
end

local function get_bucket_id(type_name, new_serializer, tuple)
    local affinity = {}
    for _, index in ipairs(new_serializer[type_name][2].affinity) do
        table.insert(affinity, tuple[index])
    end

    return model_utils.get_bucket_id_for_key(affinity)
end

local function apply_migration(type_name, src_space, dst_space, code, old_mdl, old_ddl, new_mdl, new_ddl, opts)
    opts = opts or {}
    local migration_module = assert(load(code, '@migration_' .. type_name, 't', std_env))
    local ok, lib = pcall(migration_module)
    if not ok then
        error(lib)
    end
    local transform = assert(lib.transform)

    local len = src_space:len()
    local transform_every = 1
    if opts.dry_run == true then
        if opts.at_least > len then
            transform_every = 1
        else
            local effective_ratio = math.max(opts.ratio, opts.at_least / len)
            transform_every = math.floor(1 / effective_ratio)
        end
    end

    local old_serializer, err = model_flatten.new(old_mdl, old_ddl)
    if err ~= nil then
        error(err)
    end

    local stats = {
        transformed = 0,
        remained = len,
    }

    local new_serializer, err = model_flatten.new(new_mdl, new_ddl)
    if err ~= nil then
        error(err)
    end

    if opts.dry_run ~= true then
        assert(vars.active_migrations[type_name] ~= nil)
        vars.active_migrations[type_name].stats = stats
    end

    local start_time = fiber.time64()

    local dst_version_fieldno
    local dst_bucket_id_fieldno
    local dst_space_format = dst_space:format()
    for i, field in ipairs(dst_space_format) do
        if field.name == 'version' then
            dst_version_fieldno = i
        elseif field.name == 'bucket_id' then
            dst_bucket_id_fieldno = i
        end
    end

    box.begin()
    src_space:run_triggers(false)
    local count = 0
    local src_key_def = key_def.new(src_space.index[0].parts)
    for _, old_tuple in src_space:pairs() do
        if count % transform_every == 0 then
            local version = old_tuple.version

            -- * "old_tuple" doesn't have "version" field
            -- * "new_tuple" has "version" field
            -- It means that user enabled versioning and we should fill "version" with some default
            if version == nil and dst_version_fieldno ~= nil then
                version = start_time
            end

            local bucket_id = old_tuple.bucket_id

            local old_object, err = model_flatten.unflatten_record(old_tuple, old_serializer, type_name)
            if err ~= nil then
                error(string.format('error during unflatten of %q: %s', old_tuple, err))
            end
            local ok, new_object = pcall(transform, old_object)
            if not ok then
                error(string.format('transform threw an error for %q: %s', old_tuple, new_object))
            end
            if new_object == nil then
                error(string.format('transform returned nothing for %q', old_tuple))
            end
            if type(new_object) ~= 'table' then
                error(string.format('transform returned not a table for %q', old_tuple))
            end

            new_object.version = nil
            new_object.bucket_id = nil
            local new_tuple, err = model_flatten.flatten_record(new_object, new_serializer, type_name)
            if err ~= nil then
                error(string.format('Serialization error for %q: %s', old_tuple, err))
            end

            local new_bucket_id = get_bucket_id(type_name, new_serializer, new_tuple)
            if new_bucket_id ~= bucket_id then
                error(string.format('Attempt to change bucket_id for %q', old_tuple))
            end

            if dst_version_fieldno ~= nil then
                new_tuple[dst_version_fieldno] = version
            end
            new_tuple[dst_bucket_id_fieldno] = bucket_id
            dst_space:replace(new_tuple)

            -- Drop record from space in case if it's not dry run
            if opts.dry_run ~= true then
                src_space:delete(src_key_def:extract_key(old_tuple))
            end

            stats.remained = stats.remained - 1
            stats.transformed = stats.transformed + 1
        end

        count = count + 1
        if count % defaults.FORCE_YIELD_LIMIT == 0 then
            src_space:run_triggers(true)
            if opts.dry_run ~= true then
                box.commit() -- yield!
            else
                -- In case of dry run don't save changes
                box.rollback()
                fiber.sleep(0.1)
            end
            box.begin()
            src_space:run_triggers(false)
        end
    end
    src_space:run_triggers(true)
    if opts.dry_run ~= true then
        box.commit()
        assert(stats.remained == 0)
    else
        -- In case of dry run don't save changes
        box.rollback()
        fiber.sleep(0.1)
    end
end

local function get_space_names(type_name)
    local tenant_prefix = tenant.prefix()
    return {
        space_name = model_ddl.get_space_name(type_name, tenant_prefix),
        history_space_name = model_ddl.get_history_space_name(type_name, tenant_prefix),
        expiration_space_name = model_ddl.get_expiration_space_name(type_name, tenant_prefix),
    }
end

local function run_migrations_impl()
    local space = get_space()
    local new_mdl = tenant.get_mdl()
    local new_ddl = tenant.get_ddl()

    for _, tuple in space:pairs() do
        local type_name = tuple.type_name
        local type_spaces = get_space_names(type_name)

        local old_primary_space_name = OLD_SPACE_NAME_PREFIX .. type_spaces.space_name
        local old_history_space_name = OLD_SPACE_NAME_PREFIX .. type_spaces.history_space_name
        local old_expiration_space_name = OLD_SPACE_NAME_PREFIX .. type_spaces.expiration_space_name

        local new_primary_space = box.space[type_spaces.space_name]
        local new_history_space = box.space[type_spaces.history_space_name]
        local new_expiration_space = box.space[type_spaces.expiration_space_name]

        local old_primary_space = box.space[old_primary_space_name]
        local old_history_space = box.space[old_history_space_name]
        local old_expiration_space = box.space[old_expiration_space_name]

        local src_spaces = {}
        local dst_spaces = {}

        -- We move tuples through "primary space", they will be moved to history by triggers
        if old_history_space ~= nil and new_history_space ~= nil then
            table.insert(src_spaces, old_history_space)
            table.insert(dst_spaces, new_primary_space)
        end

        table.insert(src_spaces, old_primary_space)
        table.insert(dst_spaces, new_primary_space)

        if new_expiration_space ~= nil then
            table.insert(src_spaces, old_expiration_space)
            table.insert(dst_spaces, new_expiration_space)
        end

        for i = 1, #src_spaces do
            log.warn('Migration for type %q started: %q -> %q', type_name, src_spaces[i].name, dst_spaces[i].name)
            assert(vars.active_migrations[type_name] == nil)
            vars.active_migrations[type_name] = {fiber = fiber.self(), stats = nil}

            local ok, err = pcall(apply_migration, type_name, src_spaces[i], dst_spaces[i],
                tuple.code, tuple.mdl, tuple.ddl, new_mdl, new_ddl)
            vars.active_migrations[type_name] = nil

            if not ok then
                box.rollback()
                log.error('Migration for type %q failed: %q. Aborting.\n%s', type_name, err, debug.traceback())
                return
            end
        end

        box.begin()
        old_primary_space:drop()
        if old_history_space ~= nil then
            old_history_space:drop()
        end
        if old_expiration_space ~= nil then
            old_expiration_space:drop()
        end
        space:delete({tuple.id})
        box.commit()
    end
end

local function run_migrations()
    if box.info.ro then
        return
    end

    if vars.active_migrations ~= nil and next(vars.active_migrations) ~= nil then
        return
    end

    local space = get_space()
    if space:len() > 0 then
        local f = tenant.fiber_new(run_migrations_impl)
        f:name('storage_migration')
    end
end

local DEFAULT_RATIO = 0.01
local DEFAULT_AT_LEAST = 5000

local function dry_run(cfg, migrations, ratio, at_least)
    if ratio == nil then
        ratio = DEFAULT_RATIO
    end
    if at_least == nil then
        at_least = DEFAULT_AT_LEAST
    end

    local new_mdl, err = model.load_string(cfg.types)
    if err ~= nil then
        return nil, err
    end

    local expiration_map = {}
    -- FIXME: Remove expiration
    local cfg_versioning = cfg.versioning or cfg.expiration
    if cfg_versioning ~= nil then
        for _, val in ipairs(cfg_versioning) do
            expiration_map[val.type] = val
            expiration_map[val.type].type = nil
        end
    end

    local new_ddl, err = model_ddl.generate_ddl(new_mdl, expiration_map)
    if err ~= nil then
        return nil, err
    end

    local old_mdl = tenant.get_mdl()
    local old_ddl = tenant.get_ddl()

    for _, section in ipairs(migrations) do
        local code = section.code
        local type_name = section.type_name

        local type_ddl = new_ddl[type_name]
        if type_ddl == nil then
            return nil, string.format('Type %q is not found', code)
        end

        local space_names = get_space_names(type_name)
        local space = box.space[space_names.space_name]
        assert(space ~= nil, space_names.space_name)
        local history_space = box.space[space_names.history_space_name]
        assert(history_space ~= nil, space_names.history_space_name)
        local expiration_space = box.space[space_names.expiration_space_name]
        assert(expiration_space ~= nil, space_names.expiration_space_name)

        -- No replication, no WAL writes
        local test_spaces, err = model_ddl.apply_type_ddl(type_ddl, {prefix = 'dry_run_', temporary = true})
        if err ~= nil then
            return nil, err
        end

        local function cleanup()
            for _, space in pairs(test_spaces) do
                space:drop()
            end
        end

        local src_spaces = {
            space,
            history_space,
            expiration_space,
        }
        local dst_spaces = {
            test_spaces.space,
            test_spaces.history_space,
            test_spaces.expiration_space,
        }

        for i = 1, 3 do
            assert(src_spaces[i] ~= nil, tostring(i))
            assert(dst_spaces[i] ~= nil, tostring(i))
            local ok, err = pcall(apply_migration, type_name, src_spaces[i], dst_spaces[i],
                code, old_mdl, old_ddl, new_mdl, new_ddl, {dry_run = true, ratio = ratio, at_least = at_least})
            if not ok then
                box.rollback()
                cleanup()
                return nil, err
            end
        end

        cleanup()
    end
end

local function apply_config()
    if box.info.ro then
        return
    end

    local space_name = get_space_name()
    if box.space[space_name] ~= nil then
        return
    end

    box.begin()
    local space = box.schema.space.create(space_name)

    space:format({
        { name = 'id', type = 'unsigned' },
        { name = 'type_name', type = 'string' },
        { name = 'code', type = 'string' },
        { name = 'mdl', type = 'any' },
        { name = 'ddl', type = 'any' },
    })

    space:create_index('id', {
        parts = {
            { field = 'id', type = 'unsigned' },
        },
        sequence = true,
        type = 'TREE',
        unique = true,
        if_not_exists = true,
    })
    box.commit()
end

local function add_migrations(migrations, mdl, ddl)
    if box.info.ro then
        return
    end

    if migrations == nil then
        log.verbose('Migrations are not found')
        return
    end
    for _, section in ipairs(migrations) do
        add_migration(section.type_name, section.code, mdl, ddl)
    end
end

local function get_stats()
    if vars.active_migrations == nil then
        vars.active_migrations = {}
    end

    local type_name, info = next(vars.active_migrations)
    if type_name == nil or info == nil or info.stats == nil then
        return
    end

    return {
        replicaset_uuid = box.info.cluster.uuid,
        type_name = type_name,
        transformed = info.stats.transformed,
        remained = info.stats.remained,
    }
end

return {
    add_migrations = add_migrations,
    apply_config = apply_config,
    run_migrations = run_migrations,
    dry_run = dry_run,
    get_stats = get_stats,
}
