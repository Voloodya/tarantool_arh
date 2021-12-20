local checks = require('checks')
package.loaded['checks'] = nil
local forever_checks = require('checks')
package.loaded['checks'] = checks

local fun = require('fun')
local cartridge = require('cartridge')
local cartridge_utils = require('cartridge.utils')

local utils = require('common.utils')
local model = require('common.model')
local sandbox_registry = require('common.sandbox.registry')

local model_flatten = require('common.model_flatten')
local tracing = require('common.tracing')

local errors = require('errors')
local defaults = require('common.defaults')
local model_defaults = require('common.model_defaults')
local auth = require('common.admin.auth')
local document = require('common.document')
local document_utils = require('common.document.utils')
local query_plan = require('common.document.query_plan')
local fiber = require('fiber')
local model_utils = require('common.model_utils')
local vshard_utils = require('common.vshard_utils')
local tenant = require('common.tenant')

local buffer = require('buffer')
local merger_lib = require('merger')
local key_def_lib = require('key_def')
local msgpack = require('msgpack')

local repository_error = errors.new_class('repository_error')
local push_job_error = errors.new_class('push_job_error')

local function get_bucket_id_for_query(plan, serializer)
    -- Optimize map request to use only one shard in case of full
    -- specified primary key for scanning table

    local map_to_one = document.is_select_specified_by_fields_eq(plan, serializer[2].affinity)
    if map_to_one then
        local affinity_value = document.select_affinity_from_scan_value(plan, serializer[2].affinity)
        return model_utils.get_bucket_id_for_key(affinity_value)
    end
    return nil
end

local function vshard_parallel_call_factory(call)
    return function(replicasets, function_name, args, buffers)
        local results = {}
        for _, replicaset in pairs(replicasets) do
            local replicaset_uuid = replicaset.uuid
            local vshard_call_options = { is_async = true }
            if buffers ~= nil then
                local buf = buffer.ibuf()
                buffers[replicaset_uuid] = buf
                vshard_call_options.buffer = buf
                vshard_call_options.skip_header = true
            end
            local future, err = replicaset[call](replicaset, function_name, args, vshard_call_options)
            if err ~= nil then
                return nil, err
            end

            results[replicaset_uuid] = future
        end
        return results
    end
end

local vshard_parallel = {
    ['callbro'] = vshard_parallel_call_factory('callbro'),
    ['callbre'] = vshard_parallel_call_factory('callbre'),
    ['callro'] = vshard_parallel_call_factory('callro'),
    ['callre'] = vshard_parallel_call_factory('callre'),
    ['callrw'] = vshard_parallel_call_factory('callrw'),
}

local function gather_merge_subtables(responses, timeout)
    local tuples = {}
    local metrics = {}

    local deadline = fiber.clock() + timeout
    for replicaset_uuid, resp in pairs(responses) do
        timeout = deadline - fiber.clock()
        if timeout < 0 then
            timeout = 0
        end
        local result, err = resp:wait_result(timeout)
        if err ~= nil then
            return nil, err
        end

        local res, err = result[1], result[2]
        if err ~= nil then
            return nil, err
        end
        metrics[replicaset_uuid] = res.metrics
        table.move(res.tuples, 1, #res.tuples, #tuples + 1, tuples)
    end
    return {tuples = tuples, metrics = metrics}
end

local function gather_map_call_results(responses)
    local tuples = {}
    local metrics = {}

    for replicaset_uuid, result in pairs(responses) do
        local res, err = result[1], result[2]
        if err ~= nil then
            return nil, err
        end
        metrics[replicaset_uuid] = res.metrics
        table.move(res.tuples, 1, #res.tuples, #tuples + 1, tuples)
    end
    return {tuples = tuples, metrics = metrics}
end

local function truncate_result(objects, first)
    if first == nil then
        return objects
    end

    if first < 0 then
        first = math.abs(first)
    end

    local len = #objects
    if len <= first then
        return objects
    end

    for i = first + 1, len, 1 do
        objects[i] = nil
    end
    return objects
end

local function make_options(options)
    options = options or {}

    local after = options.after
    if after ~= nil then
        local cursor, err = document_utils.decode_cursor(options.after)
        if cursor == nil then
            return nil, repository_error:new(err)
        end
        after = cursor
    end

    local first = options.first
    if first ~= nil and first < 0 and after == nil then
        local err_obj = repository_error:new("Negative first should be specified only with after option")
        -- This is 'bad request', not 'internal server' error
        err_obj.code = 400
        return nil, err_obj
    end

    return {
        first = options.first,
        after = after,
        version = options.version,
        only_if_version = options.only_if_version,
        if_not_exists = options.if_not_exists,
        secured_delete = options.secured_delete,
        all_versions = options.all_versions,
    }
end

local function validate_request(self, type_name, request_type)
    local serializer = self.serializer
    if serializer == nil then
        return nil, repository_error:new("Serializer not loaded")
    end

    if serializer[type_name] == nil then
        return nil, repository_error:new("Type %q not found", type_name)
    end

    if request_type == 'write' and self.aggregates[type_name] == nil then
        return nil, repository_error:new("Type '%s' is not an aggregate", type_name)
    end

    local _, err
    if tenant.uid() == self.tenant then
        _, err = auth.check_permission(type_name, request_type)
    else
        _, err = tenant.check_type_share(self.tenant, type_name, request_type)
    end
    if err ~= nil then
        return nil, err
    end

    return true
end

local DEFAULT_LIMIT = 10
local function find(self, type_name, filter, options)
    forever_checks('table', 'string', 'table', '?table')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local serializer = self.serializer
    local _, err = validate_request(self, type_name, 'read')
    if err ~= nil then
        return nil, err
    end

    options = options or {}
    options.first = options.first or DEFAULT_LIMIT

    local opts, err = make_options(options)
    if opts == nil then
        return nil, err
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout')
        or defaults.VSHARD_TIMEOUT

    local space_ddl = self.ddl[type_name]
    local plan = query_plan.new(space_ddl, filter, opts)

    local bucket_id = get_bucket_id_for_query(plan, serializer[type_name])
    local span = tracing.start_span('vshard_proxy.find')
    local args = { type_name, filter, opts }

    local result
    local flat, metrics

    local vshard_call_name = vshard_utils.get_call_name(options)
    if bucket_id ~= nil then
        result, err = tenant.call_with_tenant(self.tenant,
            vshard.router[vshard_call_name], bucket_id, 'vshard_proxy.find', args, {timeout = timeout})
        if err ~= nil then
            span:finish({error = err})
            return nil, err
        end
        flat = result.tuples

        if result.metrics ~= nil then
            local replicaset, err = vshard.router.route(bucket_id)
            if err ~= nil then
                span:finish({error = err})
                return nil, err
            end
            metrics = {[replicaset.uuid] = result.metrics}
        end
    else
        local responses, err
        if vshard_call_name ~= 'callrw' then
            -- Map query over replicasets
            local replicasets = vshard.router.routeall()
            responses, err = tenant.call_with_tenant(self.tenant,
                vshard_parallel[vshard_call_name],
                replicasets, 'vshard_proxy.find', args)

            if err ~= nil then
                span:finish({error = err})
                return nil, err
            end

            result, err = gather_merge_subtables(responses, timeout)
            if err ~= nil then
                span:finish({error = err})
                return nil, err
            end
        else
            responses, err = tenant.call_with_tenant(self.tenant,
                vshard.router.map_callrw, 'vshard_proxy.find', args, {timeout = timeout})
            if err ~= nil then
                span:finish({error = err})
                return nil, err
            end
            result, err = gather_map_call_results(responses)
            if err ~= nil then
                span:finish({error = err})
                return nil, err
            end
        end

        flat = result.tuples
        metrics = result.metrics
        local size = utils.table_count(responses)
        if size > 1 then
            document.sort_tuples(plan, flat)
            flat = truncate_result(flat, opts.first)
        end
    end

    if opts.first ~= nil and opts.first < 0 then
        utils.reverse_table(flat)
    end

    local objects, err = model_flatten.unflatten(flat, serializer, type_name)
    if err ~= nil then
        span:finish({error = err})
        return nil, err
    end

    -- Pagination
    for object_no, object in ipairs(objects) do
        local flat_obj = flat[object_no]
        object.cursor = document_utils.encode_cursor({scan = flat_obj[#flat_obj]})
    end

    span:finish()
    return objects, nil, metrics
end

--  Make merger key_def and merger object from sources
local function merge_tuple_sources(ddl, sources, query, opts)
    local sort_opts = {
        first = opts and opts.first,
        all_versions = opts and opts.all_versions,
    }
    local plan = query_plan.new(ddl, query, sort_opts)

    local scan_key_def = {}
    local fieldno = #ddl.format + 1

    for i, part in ipairs(plan.scan_index.parts) do
        table.insert(scan_key_def, {
            fieldno = fieldno,
            path = '[' .. i .. ']',
            type = part.type,
            is_nullable = plan.scan_index.name ~= plan.pkey_index.name
        })
    end

    local key_def = key_def_lib.new(scan_key_def)

    local reverse = false
    if plan.op == 'LE' or plan.op == 'LT' or plan.op == 'REQ' then
        reverse = true
    end

    local merger = merger_lib.new(key_def, sources, {reverse = reverse})
    return merger:pairs()
end

local function decode_metainfo(buf)
    -- Skip an array around a call return values.
    local len
    len, buf.rpos = msgpack.decode_array_header(buf.rpos, buf:size())
    if len ~= 2 then
        error('Unexpected size of data in repository.pairs incoming buffer')
    end

    -- Decode a first return value (cursor).
    local res
    res, buf.rpos = msgpack.decode(buf.rpos, buf:size())
    return res
end

-- Generator function for merger
local function fetch_buffer_chunk(context, state)
    -- Get parameters from passed context and state
    local buf = context.buffer
    local timeout = context.timeout
    local next_func_args = context.call_args
    local replicaset = context.replicaset
    local vshard_call_name = context.vshard_call_name
    local future = state.future
    local vshard_call_opts = { is_async = true, skip_header = true, buffer = buf }

    -- The source was entirely drained.
    if future == nil then
        return nil
    end

    -- Wait for requested data.
    local res, err = future:wait_result(timeout)
    if res == nil then
        error(err)
    end

    -- Get cursor from returned data
    local cursor = decode_metainfo(buf)
    if cursor == nil then
        return {}, buf
    end

    -- change context.func_args too, but it does not matter
    next_func_args[3].after = cursor
    local next_future = replicaset[vshard_call_name](replicaset,
        'vshard_proxy.find_pairs',
        next_func_args,
        vshard_call_opts)
    -- Return next_future and
    return {future = next_future}, buf
end

-- Unflatten tuples in pairs
local function merger_tuple_unflattener_factory(type_name, serializer)
    return function(tuple)
        local result, err = model_flatten.unflatten_record(tuple, serializer, type_name)
        if err ~= nil then
            error(string.format('Error occurred while unflattening %q object: %q', type_name, err))
        end
        return result
    end
end

local function repository_pairs(self, type_name, filter, options)
    forever_checks('table', 'string', 'table', {
        after = '?string',
        version = '?number',
        all_versions = '?boolean',

        mode = '?string',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',
    })

    if not vshard_utils.vshard_is_bootstrapped() then
        error("Cluster isn't bootstrapped yet")
    end

    local vshard_call_name = vshard_utils.get_call_name(options)

    local timeout = cartridge.config_get_readonly('vshard-timeout')
        or defaults.VSHARD_TIMEOUT

    local span = tracing.start_span('repository.pairs')

    local opts, err = make_options(options)
    if opts == nil then
        span:finish({error = err})
        error(err)
    end

    -- Determine if all data is on one replicaset and it's possible to do call without map/reduce
    local space_ddl = self.ddl[type_name]
    local plan = query_plan.new(space_ddl, filter, opts)
    local serializer = self.serializer
    local bucket_id = get_bucket_id_for_query(plan, serializer[type_name])

    local replicasets
    -- In case there is exact replicaset for query there is no need in map/reduce
    if bucket_id ~= nil then
        -- Get replicaset
        local replicaset, err = vshard.router.route(bucket_id)
        if err ~= nil then
            err = repository_error:new(err)
            span:finish({error = err})
            error(err)
        end
        replicasets = { [replicaset.uuid] = replicaset }
    else
        replicasets = vshard.router.routeall()
    end

    local buffers = {}

    -- Prepare arguments for remote calls on storages
    local fun_args = { type_name, filter, opts }
    -- Prepare context for fetching functions
    local context = {
        timeout = timeout,
        vshard_call_name = vshard_call_name,
        call_args = fun_args,
    }

    -- Map query over replicasets
    local responses, err = tenant.call_with_tenant(self.tenant,
        vshard_parallel[vshard_call_name],
        replicasets, 'vshard_proxy.find_pairs', fun_args, buffers)
    if err ~= nil then
        err = repository_error:new(err)
        span:finish({error = err})
        error(err)
    end

    local sources = {}

    -- Make buffer_sources table for merger
    for replicaset_uuid, replicaset in pairs(replicasets) do
        -- Context should be unique for all replicasets
        local ctx = table.copy(context)
        ctx.buffer = buffers[replicaset_uuid]
        ctx.replicaset = replicaset

        local state = { future = responses[replicaset_uuid] }
        local source = merger_lib.new_buffer_source(fetch_buffer_chunk, ctx, state)
        table.insert(sources, source)
    end

    -- Get merger:pairs sorted object
    local merger_pairs = merge_tuple_sources(self.ddl[type_name], sources, filter, options)
    span:finish()
    return merger_pairs:map(merger_tuple_unflattener_factory(type_name, serializer))
end

local function map_call(self, vshard_call_name, call_fn_name, args, timeout)
    local replicasets = vshard.router.routeall()
    local responses, err = tenant.call_with_tenant(self.tenant,
        vshard_parallel[vshard_call_name], replicasets, call_fn_name, args)
    if err ~= nil then
        return nil, err
    end

    local count = utils.table_count(responses)
    local deadline = fiber.clock() + timeout
    local results_on_storage = table.new(count, 0)
    for replicaset_uuid, response in pairs(responses) do
        timeout = deadline - fiber.clock()
        if timeout < 0 then
            timeout = 0
        end

        response, err = response:wait_result(timeout)
        if err ~= nil then
            return nil, err
        end

        results_on_storage[replicaset_uuid] = response
    end
    return results_on_storage
end

local function map_reduce_internal(self, function_name, type_name, filter, args, options, timeout)
    local plan = query_plan.new(self.ddl[type_name], filter)
    local bucket_id = get_bucket_id_for_query(plan, self.serializer[type_name])

    local results, err
    local vshard_call_name = vshard_utils.get_call_name(options)
    if bucket_id ~= nil then
        local response, err = tenant.call_with_tenant(self.tenant,
            vshard.router[vshard_call_name], bucket_id, function_name, args, {is_async = true})
        if err ~= nil then
            return nil, err
        end

        -- Timeout error should be handled separately
        local result, err = response:wait_result(timeout)
        if err ~= nil then
            err = err.message or err
            return nil, err
        end
        results = result
    else
        results = {}
        local result_on_storages
        if options.mode == 'write' then
            result_on_storages, err = tenant.call_with_tenant(self.tenant,
                vshard.router.map_callrw, function_name, args, {timeout = timeout})
        else
            result_on_storages, err = map_call(self, vshard_call_name,
                function_name, args, timeout)
        end

        if err ~= nil then
            return nil, err
        end

        for _, response in pairs(result_on_storages) do
            local res, err = response[1], response[2]
            if err ~= nil then
                return nil, err
            end
            table.insert(results, res)
        end
    end
    return results
end

local function count(self, type_name, filter, options)
    forever_checks('table', 'string', 'table', '?table')

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local _, err = validate_request(self, type_name, 'read')
    if err ~= nil then
        return nil, err
    end

    options = options or {}
    local opts, err = make_options(options)
    if opts == nil then
        return nil, err
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT

    local span = tracing.start_span('vshard_proxy.count')
    local args = { type_name, filter, opts }

    local results_on_storage, err = map_reduce_internal(self, 'vshard_proxy.count',
        type_name, filter, args, options, timeout)
    if err ~= nil then
        errors.wrap(err)
        span:finish({error = err})
        return nil, err
    end

    local result = fun.sum(results_on_storage)
    span:finish()
    return result
end

local function pk_to_affinity(pkey, model)
    -- this means pkey is simple field
    if type(pkey) ~= 'table' then
        return { pkey }
    end

    -- this means pk == affinity and neither transformation nor copying is required
    if model.pk_related_affinity == nil then
        return pkey
    end

    local res = {}
    for _, v in ipairs(model.pk_related_affinity) do
        table.insert(res, pkey[v])
    end
    return res
end

local get_options_check = {
    version = '?number|cdata',
    prefer_replica = '?boolean',
    balance = '?boolean',
    mode = '?string',
}
local function get(self, type_name, pkey, options)
    forever_checks("table", "string", "?", get_options_check)

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local serializer = self.serializer
    local _, err = validate_request(self, type_name, 'read')
    if err ~= nil then
        return nil, err
    end

    options = options or {}

    local timeout = cartridge.config_get_readonly('vshard-timeout')
        or defaults.VSHARD_TIMEOUT

    local affinity_value = pk_to_affinity(pkey, serializer[type_name][2])
    local bucket_id = model_utils.get_bucket_id_for_key(affinity_value)

    local vshard_call_name = vshard_utils.get_call_name(options)
    local span = tracing.start_span('vshard_proxy.get')
    local flat, err = tenant.call_with_tenant(self.tenant,
        vshard.router[vshard_call_name], bucket_id, 'vshard_proxy.get',
        { type_name, pkey, options },
        { timeout = timeout })

    if err ~= nil then
        span:finish({ error = err })
        return nil, err
    end

    span:finish()

    if flat == nil then
        return nil
    end

    local result, err = model_flatten.unflatten_record(flat, serializer, type_name)

    if result == nil then
        return nil, err
    end

    return result
end

local put_options_check = { version = '?number|cdata', only_if_version = '?number|cdata', if_not_exists = '?boolean' }
local put_context_check = { routing_key = '?string' }
local function put(self, type_name, obj, options, context)
    forever_checks('table', 'string', 'table', put_options_check, put_context_check)

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local _, err = validate_request(self, type_name, 'write')
    if err ~= nil then
        return nil, err
    end

    local _, err = model_defaults.fill_defaults(self.defaults, obj, type_name)
    if err ~= nil then
        return nil, err
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout')
        or defaults.VSHARD_TIMEOUT

    local serializer = self.serializer
    local flattened_entities, err = model_flatten.flatten(obj, serializer, type_name)
    if flattened_entities == nil then
        return nil, err
    end

    local affinity = {}
    for _, index in ipairs(serializer[type_name][2].affinity) do
        table.insert(affinity, flattened_entities[1][index])
    end

    local bucket_id = model_utils.get_bucket_id_for_key(affinity)
    local format = model_flatten.field_id_by_name(self.ddl[type_name].format, type_name)
    local bucket_id_index = format['bucket_id']
    flattened_entities[1][bucket_id_index] = bucket_id

    local span = tracing.start_span('vshard_proxy.put')
    local version, err = tenant.call_with_tenant(self.tenant,
        vshard.router.call, bucket_id, "write", "vshard_proxy.put",
        { type_name, flattened_entities, options, context },
        { timeout = timeout })

    if err ~= nil then
        span:finish({ error = err })
        return nil, err
    end
    span:finish()

    if format['version'] ~= nil then
        obj.version = version
    end
    return {obj}
end


local function put_batch(self, type_name, array, options, context)
    forever_checks('table', 'string', 'table', put_options_check, put_context_check)

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local _, err = validate_request(self, type_name, 'write')
    if err ~= nil then
        return nil, err
    end

    if #array == 0 then
        return {}
    end

    local span = tracing.start_span('vshard_proxy.put_batch')

    local replicasets = {}

    local timeout = cartridge.config_get_readonly('vshard-timeout')
        or defaults.VSHARD_TIMEOUT

    local format = model_flatten.field_id_by_name(self.ddl[type_name].format, type_name)
    local bucket_id_index = format['bucket_id']
    local version_index = format['version']
    local serializer = self.serializer

    local tuple_to_id
    if version_index ~= nil then
        tuple_to_id = {}
    end

    for i, obj in ipairs(array) do
        local _, err = model_defaults.fill_defaults(self.defaults, obj, type_name)
        if err ~= nil then
            span:finish({error = err})
            return nil, err
        end

        local tuple, err = model_flatten.flatten_record(obj, serializer, type_name)
        if tuple == nil then
            span:finish({error = err})
            return nil, err
        end

        if version_index ~= nil then
            tuple_to_id[tuple] = i
        end

        local affinity = {}
        for _, index in ipairs(serializer[type_name][2].affinity) do
            table.insert(affinity, tuple[index])
        end

        local bucket_id = model_utils.get_bucket_id_for_key(affinity)
        tuple[bucket_id_index] = bucket_id

        local rs = vshard.router.route(bucket_id)
        local tuples_by_replicasets = replicasets[rs]
        if tuples_by_replicasets == nil then
            replicasets[rs] = {tuples = {tuple}, bucket_ids = {[bucket_id] = true}}
        else
            table.insert(tuples_by_replicasets.tuples, tuple)
            tuples_by_replicasets.bucket_ids[bucket_id] = true
        end
    end

    local futures = {}
    for replicaset, tuples_by_replicasets in pairs(replicasets) do
        local future = tenant.call_with_tenant(self.tenant,
            replicaset.callrw, replicaset, 'vshard_proxy.put_batch',
            {type_name, tuples_by_replicasets.tuples, tuples_by_replicasets.bucket_ids, options, context},
            {is_async = true}
        )
        table.insert(futures, {f = future, tuples = tuples_by_replicasets.tuples})
    end

    local deadline = timeout + fiber.clock()
    for _, future in ipairs(futures) do
        timeout = deadline - fiber.clock()
        if timeout < 0 then
            timeout = 0
        end
        local result, err = future.f:wait_result(timeout)
        if err ~= nil then
            span:finish({error = err})
            return nil, err
        end
        local versions, err = result[1], result[2]
        if err ~= nil then
            span:finish({error = err})
            return nil, err
        end

        if version_index ~= nil then
            for i, tuple in ipairs(future.tuples) do
                local index = tuple_to_id[tuple]
                local version = versions[i]
                array[index].version = version
            end
        end
    end

    if err ~= nil then
        span:finish({error = err})
        return nil, err
    end
    span:finish()

    return array
end

local function set_mdl_related_config(self, mdl)
    local aggregates = {}
    for _, entry in ipairs(mdl) do
        if entry.indexes ~= nil then
            aggregates[entry.name] = true
        end
    end

    local err
    self.defaults, err = model_defaults.fill_mdl_defaults(mdl)

    if err ~= nil then
        return nil, err
    end

    self.aggregates = aggregates
    self.mdl = mdl

    return true
end

local function set_ddl_related_config(self, ddl)
    if ddl ~= nil then
        local available_cmp = {'==', '>=', '>', '<', '<='}
        ddl = table.deepcopy(ddl)
        cartridge_utils.table_setrw(ddl)
        for _, ddl_entry in pairs(ddl) do
            local ddl_entry_indexes
            if ddl_entry.history_indexes ~= nil then
                ddl_entry_indexes = ddl_entry.history_indexes
            else
                ddl_entry_indexes = ddl_entry.indexes
            end

            for _, index in ipairs(ddl_entry_indexes) do
                index.cmp = {}
                for _, op in ipairs(available_cmp) do
                    index.cmp[op] = document.create_row_lessthan(op, index)
                end
            end
        end
    end

    self.ddl = ddl

    return true
end

-- JIT doesn't optimize vararg so explicitly pass arg1...argN
local function call_dml_request(function_name, self, type_name, filter, options, context, arg1)
    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local _, err = validate_request(self, type_name, 'write')
    if err ~= nil then
        return nil, err
    end

    local opts, err = make_options(options)
    if opts == nil then
        return nil, err
    end

    local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT

    -- Prepare which replicasets is used for map query
    local plan = query_plan.new(self.ddl[type_name], filter, opts)
    local bucket_id = get_bucket_id_for_query(plan, self.serializer[type_name])
    local span = tracing.start_span(function_name)

    local flat
    if bucket_id ~= nil then
        local responses, err = tenant.call_with_tenant(self.tenant,
            vshard.router.callrw, bucket_id, function_name,
            { type_name, filter, opts, context, arg1 }, {timeout = timeout})
        if err ~= nil then
            span:finish({error = err})
            return nil, err
        end
        flat = responses.tuples
    else
        -- Map update over replicasets
        local responses, err = tenant.call_with_tenant(self.tenant,
            vshard.router.map_callrw, function_name,
            { type_name, filter, opts, context, arg1 }, {timeout = timeout})
        if err ~= nil then
            span:finish({error = err})
            return nil, err
        end

        local result, err = gather_map_call_results(responses)
        if err ~= nil then
            span:finish({error = err})
            return nil, err
        end
        flat = result.tuples
        -- End of map

        -- Reduce resultsets and sort it (using query planner inside)
        local size = utils.table_count(responses)
        if size > 1 then
            document.sort_tuples(plan, flat)
        end
        -- End of reduce
    end

    local objects, err = model_flatten.unflatten(flat, self.serializer, type_name)
    if objects == nil then
        span:finish({error = err})
        return nil, err
    end
    span:finish()

    return objects
end

local update_options_check = { version = '?number|cdata', only_if_version = '?number|cdata' }
local update_context_check = { routing_key = '?string' }
local function update(self, type_name, filter, updaters, options, context)
    forever_checks("table", "string", "table", "table", update_options_check, update_context_check)

    return call_dml_request('vshard_proxy.update', self, type_name, filter, options, context, updaters)
end

local delete_options_check = {
    version = '?number|cdata',
    only_if_version = '?number|cdata',
    secured_delete = '?boolean',
    first = '?number',
}
local function delete(self, type_name, filter, options)
    forever_checks('table', 'string', 'table', delete_options_check)

    return call_dml_request('vshard_proxy.delete', self, type_name, filter, options)
end

--[[
    type_name - name of aggregate
    filter - filter conditions
    map_fn - name of map function (aggregate -> value)
    combine_fn - name of combine function (values -> local state)
    reduce_fn - name of reduce function (local states -> state)
    options - options
    options.map_args - additional arguments for map
    options.combine_args - additional arguments for combine
    options.combine_initial_state - initial state for combine
    options.reduce_args - additional arguments for reduce
    options.reduce_initial_state - initial state for combine
    options.version - value that allows to perform versioning scan over tuples at moment "version"
    options.mode - "read" or "write"
    options.prefer_replica - if true requests will be preferably executed on replica
    options.balance - if true requests will be balanced
    options.timeout - timeout in secs

    returns - result state
]]
local map_reduce_options_check = {
    map_args = '?',
    combine_args = '?',
    combine_initial_state = '?',
    reduce_args = '?',
    reduce_initial_state = '?',
    version = '?number|cdata',
    mode = '?string',
    prefer_replica = '?boolean',
    balance = '?boolean',
    timeout = '?number',
}
local function map_reduce(self, type_name, filter, map_fn_name, combine_fn_name, reduce_fn_name, options)
    options = options or {}
    forever_checks('table', 'string', 'table', 'string', 'string', 'string', map_reduce_options_check)

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local mode = options.mode or 'read'
    local _, err = validate_request(self, type_name, mode)
    if err ~= nil then
        return nil, err
    end

    local span = tracing.start_span('vshard_proxy.map_reduce')

    local storage_opts = {
        map_args = options.map_args,
        combine_args = options.combine_args,
        combine_initial_state = options.combine_initial_state,
        version = options.version,
    }
    local args = {type_name, filter, map_fn_name, combine_fn_name, storage_opts}

    local timeout = options.timeout or cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT

    -- Pass timeout to storage
    storage_opts.timeout = timeout

    local results_on_storage, err = map_reduce_internal(self, 'vshard_proxy.map_reduce',
        type_name, filter, args, options, timeout)
    if err ~= nil then
        errors.wrap(err)
        span:finish({error = err})
        return nil, err
    end

    local sandbox = sandbox_registry.get('active')
    local reduce_fn, err = sandbox:dispatch_function(reduce_fn_name, {protected=true})
    if not reduce_fn then
        span:finish({error = err})
        return nil, err
    end

    local result_state, err = sandbox.batch_accumulate(reduce_fn,
        options.reduce_initial_state, results_on_storage, options.reduce_args)

    span:finish({error = err})
    return result_state, err
end

local call_on_storage_options_check = {
    timeout = '?number',
    mode = '?string',
    prefer_replica = '?boolean',
    balance = '?boolean',
}
local function call_on_storage(self, type_name, index_name, value, func_name, func_args, options)
    options = options or {}
    forever_checks('table', 'string', 'string', '?', 'string', '?', call_on_storage_options_check)

    if not vshard_utils.vshard_is_bootstrapped() then
        return nil, repository_error:new("Cluster isn't bootstrapped yet")
    end

    local mode = options.mode or 'read'
    local _, err = validate_request(self, type_name, mode)
    if err ~= nil then
        return nil, err
    end

    local sandbox = sandbox_registry.get('active')
    local _, err = sandbox:dispatch_function(func_name, {protected = true})
    if err ~= nil then
        return nil, err
    end

    local filter = {{ index_name, '==', value }}

    local span = tracing.start_span('vshard_proxy.call_on_storage')

    local args = {func_name, func_args}
    local timeout = options.timeout or cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT

    local results, err = map_reduce_internal(self, 'vshard_proxy.call_on_storage',
        type_name, filter, args, options, timeout)
    if err ~= nil then
        span:finish({error = err})
        return nil, err
    end

    span:finish()

    return results
end

local function push_job(_, name, args)
    forever_checks('table', 'string', 'table')
    local span = tracing.start_span('repository.push_job')

    local _, err = cartridge.rpc_call('storage', 'push_job',
        { name, args }, {leader_only = true})
    span:finish({error = err})

    if err ~= nil then
        return nil, push_job_error:new(err)
    end
    return true
end

local function validate_config(cfg)
    local types_str = cfg['types'] or ''

    local mdl, err = model.load_string(types_str)
    if mdl == nil then
        return nil, err
    end

    local _, err = model_defaults.validate_mdl_defaults(mdl)
    if err ~= nil then
        return nil, err
    end
    return true
end

local function apply_config(self, mdl, ddl, serializer, pre_calc_mdl_hash, pre_calc_ddl_hash)
    checks('table', '?table', '?table', '?table', '?string', '?string')
    local mdl_hash = pre_calc_mdl_hash ~= nil and pre_calc_mdl_hash or utils.calc_hash(mdl)
    if mdl_hash ~= self.prev_mdl_hash then
        local res, err = repository_error:pcall(set_mdl_related_config, self, mdl)
        if res == nil then
            return nil, err
        end

        self.prev_mdl_hash = mdl_hash
    end

    local ddl_hash = pre_calc_ddl_hash ~= nil and pre_calc_ddl_hash or utils.calc_hash(ddl)
    if ddl_hash ~= self.prev_ddl_hash then
        local res, err = repository_error:pcall(set_ddl_related_config, self, ddl)
        if res == nil then
            return nil, err
        end

        self.prev_ddl_hash = ddl_hash
    end

    self.serializer = serializer
end

local function new()
    local instance = {
        tenant = tenant.uid(),
        apply_config = apply_config,

        get = get,
        find = find,
        pairs = repository_pairs,
        count = count,
        put = put,
        put_batch = put_batch,
        update = update,
        delete = delete,
        map_reduce = map_reduce,
        call_on_storage = call_on_storage,
        push_job = push_job,

        aggregates = {},

        get_bucket_id_for_key = model_utils.get_bucket_id_for_key,
        get_bucket_id_for_query = get_bucket_id_for_query,
    }

    return instance
end

return {
    new = new,
    validate_config = validate_config,
}
