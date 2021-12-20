local module_name = 'common.data_type'

local actions_list = require('account_manager.access_actions_list').get()
local admin_auth = require('common.admin.auth')
local audit_log = require('audit.log').new(module_name)
local expiration_utils = require('storage.expiration.utils')
local graphql = require('common.graphql')
local checks = require('checks')
local log = require('log.log').new(module_name)
local model = require('common.model')
local model_ddl = require('common.model_ddl')
local model_explorer = require('common.model_explorer')
local model_graphql = require('common.model_graphql')
local sorted_pairs = require('common.sorted_pairs')
local tenant = require('common.tenant')
local types = require('graphql.types')

local trivial_transformation = [[
    return {
        transform = function(object)
            return object
        end
    }
]]

-- FIXME: Remove expiration
local type_expiration_args = {
    enabled = types.boolean,
    type = types.string.nonNull,
    lifetime_hours = types.int,
    keep_version_count = types.int,
    strategy = types.string,
    schedule = types.string,
    dir = types.string,
    file_size_threshold = types.int,
}
local type_expiration = types.object{
    name = 'Expiration',
    description = 'Expiration',
    fields = type_expiration_args,
}
local type_versioning = types.object{
    name = 'Versioning',
    description = 'Versioning',
    fields = type_expiration_args,
}

local type_expiration_list = types.list(type_expiration)
local type_versioning_list = types.list(type_versioning)

local type_expiration_list_input = model_graphql.get_input_type({}, type_expiration_list, {})
local type_versioning_list_input = model_graphql.get_input_type({}, type_versioning_list, {})

local type_data_type = types.object {
    name = 'DataType',
    fields = {
        model = types.string,
        -- FIXME: Remove expiration
        expiration = type_expiration_list,
        versioning = type_versioning_list,
    }
}

local function get_expiration_list(_, _)
    local mdl, err = tenant.get_mdl()
    if err then
        return nil, err
    end

    local objects, err = model_explorer.make_object_map(mdl)
    if err ~= nil then
        return nil, err
    end

    local expiration_by_type = {}
    for typename, obj in pairs(objects) do
        if obj.indexes ~= nil then
            expiration_by_type[typename] = {
                enabled = false,
                type = typename,
                lifetime_hours = box.NULL,
                keep_version_count = box.NULL,
                strategy = box.NULL,
                schedule = box.NULL,
                dir = box.NULL,
                file_size_threshold = box.NULL,
            }
        end
    end

    -- FIXME: Remove expiration
    local expiration_cfg = tenant.get_cfg_non_null_deepcopy('versioning', 'expiration') or {}
    for _, expiration in ipairs(expiration_cfg) do
        local info = expiration_by_type[expiration.type]
        info.enabled = expiration.enabled == true
        info.lifetime_hours = expiration.lifetime_hours
        info.keep_version_count = expiration_utils.get_version_count(expiration)
        info.strategy = expiration.strategy
        info.schedule = expiration.schedule
        info.dir = expiration.dir
        info.file_size_threshold = expiration.file_size_threshold
    end

    local res = {}
    for _, expiration in sorted_pairs(expiration_by_type) do
        table.insert(res, expiration)
    end

    return res
end

local function set_expiration(new_expiration, new_mdl)
    checks('table', '?string')
    local old_mdl = tenant.get_mdl()
    local old_types, err = model_explorer.make_object_map(old_mdl)
    if err ~= nil then
        return nil, err
    end

    -- FIXME: Remove expiration
    local old_expiration = tenant.get_cfg_non_null_deepcopy('versioning', 'expiration') or {}

    local new_expiration_map = {}
    for _, v in ipairs(new_expiration) do
        new_expiration_map[v.type] = v
    end

    local old_expiration_enabled = {}
    for _, v in ipairs(old_expiration) do
        old_expiration_enabled[v.type] = v.enabled == true
    end

    -- Prepare mdl, ddl and opts
    local mdl
    local ddl
    if new_mdl ~= nil then
        mdl = model.load_string(new_mdl)
        ddl, err = model_ddl.generate_ddl(mdl, new_expiration_map)
        if err ~= nil then
            return nil, err
        end
    else
        mdl = old_mdl
        ddl = tenant.get_cfg_deepcopy('ddl')
    end

    if ddl == nil then
        ddl = {}
    end
    local opts = {migration = {}}

    -- Check data types
    for _, v in ipairs(new_expiration) do
        if ddl[v.type] == nil then
            return nil, string.format("Can't find type %q for versioning", v.type)
        end

        -- Clear all data if expiration is turned off
        if v.lifetime_hours == nil and v.keep_version_count == nil then
            v.strategy = nil
            v.schedule = nil
            v.dir = nil
            v.file_size_threshold = nil
        end

        -- Prepare migrations for old types or unlinked spaces
        if old_types[v.type] ~= nil then
            local old_enabled = old_expiration_enabled[v.type] == true
            local enabled_is_changed = v.enabled ~= old_enabled
            if enabled_is_changed then
                local new_ddl, err = model_ddl.generate_record_ddl(v.type, mdl, v.enabled)
                if err ~= nil then
                    return nil, err
                end
                ddl[v.type] = new_ddl
                table.insert(opts.migration, {type_name = v.type, code = trivial_transformation})
            end
        end
    end
    return new_expiration, ddl, opts
end

local function get_data_type(_, _)
    local _, err = admin_auth.check_role_has_access(actions_list.data_type_read)
    if err ~= nil then
        return nil, err
    end

    local model = tenant.get_model()

    local expiration = get_expiration_list()

    local res = {
        model = model,
        -- FIXME: Remove expiration
        expiration = expiration,
        versioning = expiration,
    }
    return res
end

local function set_data_type(_, args)
    local _, err = admin_auth.check_role_has_access(actions_list.data_type_write)
    if err ~= nil then
        return nil, err
    end

    local new_model = args.model
    -- FIXME: Remove expiration
    local new_expiration = args.versioning or args.expiration

    local new_config = {}
    local patch_opts = {}
    if type(new_model) ~= 'nil' then
        new_model = new_model ~= nil and new_model or ''
        new_config = {
            types = {__file = "model.avsc"},
            ['model.avsc'] = new_model,
        }
    end

    if type(new_expiration) ~= 'nil' then
        log.info('Versioning will be updated')
        new_expiration = new_expiration ~= nil and new_expiration or {}
        local expiration, ddl, opts = set_expiration(new_expiration, new_model)
        if expiration == nil then
            return nil, ddl -- ddl in this case contains error string
        end

        -- FIXME: Remove expiration
        if args.versioning ~= nil then
            new_config.versioning = expiration
            new_config.expiration = box.NULL
        else
            new_config.expiration = expiration
            new_config.versioning = box.NULL
        end
        new_config.ddl = ddl
        patch_opts = opts
    end

    local _, err = tenant.patch_config_with_ddl(new_config, patch_opts)
    if err ~= nil then
        return nil, err
    end

    audit_log.warn('Model/versioning was updated')
    return get_data_type()
end

local function init()
    graphql.add_callback({
        schema = 'admin',
        name = 'data_type',
        doc = 'Get current model and versioning',
        args = {},
        kind = type_data_type,
        callback = 'common.data_type.get_data_type',
    })

    graphql.add_mutation({
        schema='admin',
        name='data_type',
        doc='Set model and versioning',
        callback='common.data_type.set_data_type',
        args={
            model=types.string,
            -- FIXME: Remove expiration
            expiration=type_expiration_list_input,
            versioning=type_versioning_list_input,
        },
        kind = type_data_type,
    })
end

return {
    init = init,
    get_data_type = get_data_type,
    set_data_type = set_data_type,
}
