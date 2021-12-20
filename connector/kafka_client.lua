local module_name = 'connector.kafka_client'

local cartridge = require('cartridge')
local fiber = require('fiber')
local kafka = require('kafka')
local json = require('json')
local errors = require('errors')
local uuid = require('uuid')
local checks = require('checks')

local defaults = require('common.defaults')
local log = require('log.log').new(module_name)
local vars = require('common.vars').new(module_name)
local request_context = require('common.request_context')
local tracing = require('common.tracing')
local auth = require('common.admin.auth')
local utils = require('common.utils')

local DEFAULT_KAFKA_WORKERS_COUNT = 10
local DEFAULT_KAFKA_AUTH_TIMEOUT = 5
local JSON_FORMAT = 'json'
local PLAIN_FORMAT = 'plain'

local DEFAULT_KAFKA_CONSUMER_OPTS = {
    -- Automatically updates offset (autocommit)
    ["enable.auto.offset.store"] = "false",
    -- Which offset to take when there is no offset in a store
    -- { earliest, latest, error }
    ["auto.offset.reset"] = "earliest",
    -- Do note emit RD_KAFKA_RESP_ERR__PARTITION_EOF if the
    -- consumer reaches the end of a partition
    ["enable.partition.eof"] = "false",
}

local kafka_error = errors.new_class('kafka_error', {capture_stack = false})
local json_error = errors.new_class('json_parse_error', {capture_stack = false})
local kafka_producer_error = errors.new_class('kafka_producer_error',
    {capture_stack = false})
local kafka_consumer_error = errors.new_class('kafka_consumer_error',
    {capture_stack = false})

vars:new('routing_key')
vars:new('is_async')
-- stores kafka consumers and producers by name
vars:new('consumers')
vars:new('producers')
-- contains topics to produce on by output name
vars:new('topics')
-- workers process kafka messages
vars:new('workers')
vars:new('server')

local gen_error_callback = function(prefix, name)
    return function(err)
        log.error(kafka_error:new('%s "%s": %s', prefix, name, err))
    end
end

local function add_producer(opts)
    opts = opts or {}
    checks({
        name     = 'string',
        topic    = 'string',
        brokers  = 'table',
        options  = '?table',
        is_async = '?boolean',
    })

    if opts.options == nil then
       opts.options = {}
    end

    if opts.is_async ~= true and opts.options['enable.idempotence'] == nil then
        -- Here we enable a kind of "synchronous processing"
        -- that meets user expectations.
        -- See details https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        opts.options['enable.idempotence'] = 'true'
    end

    local producer, err = kafka.Producer.create({
        brokers = table.concat(opts.brokers, ','),
        options = opts.options or {},
        error_callback = gen_error_callback('producer', opts.name),
        default_topic_options = {
            ["partitioner"] = "murmur2_random",
        },
    })

    if not producer then
        return nil, kafka_producer_error:new(err)
    end

    vars.producers[opts.name] = producer
    vars.topics[opts.name] = opts.topic

    return true
end

local function remove_producer(name)
    local producer = vars.producers[name]
    if not producer then
        return nil, kafka_error:new('No producer with name %q exists', name)
    end

    fiber.new(function()
        local ok, err = producer:close()
        if not ok then
            local err = kafka_producer_error:new('Failed closing producer %q: %s', name, err)
            log.error('%s', err)
        end
    end)

    vars.producers[name] = nil

    return true
end

-- Simple channel pool to reduce GC pressure
local function new_channel()
    return fiber.channel(1)
end

local items = {}
local len = 0
local function get_item(constructor)
    if len == 0 then
        return constructor()
    end
    local item = items[len]
    len = len - 1
    return item
end

local function release_item(item)
    if item == nil then
        return
    end
    len = len + 1
    items[len] = item
end

local function produce(obj, output_name, is_async, format)
    local producer = vars.producers[output_name]
    if not producer then
        return nil, kafka_error:new('No producer with name "%s" exists', output_name)
    end

    local msg, err
    if format == nil or format == JSON_FORMAT then
        msg, err = kafka_error:pcall(json.encode, obj)
    elseif format == PLAIN_FORMAT then
        msg = obj
    else
        return nil, kafka_error:new("Unknown format %q", format)
    end

    if not msg then
        return nil, err
    end

    local topic = vars.topics[output_name]
    local config = {
        topic = topic,
        key = uuid.str(),
        value = msg,
    }

    local channel
    if is_async == false then
        channel = get_item(new_channel)
        config.dr_callback = function(err_msg)
            if err ~= nil then
                channel:put(err_msg)
            else
                -- Otherwise we can't distinguish "timeout error" and "no error" case
                channel:put(true)
            end
        end
    end

    local err = producer:produce_async(config)
    if err then
        release_item(channel)
        return nil, kafka_producer_error:new('Failed to produce a message: %s', err)
    end

    if channel ~= nil then
        -- Usually batch should be consumed in "queue.buffering.max.ms" (default: 5ms)
        -- so anyway once we will get some response.
        -- See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        -- For some critical cases we add vshard timeout error -
        -- if nothing happens we anyway abort this operation on client-side
        local timeout = cartridge.config_get_readonly('vshard-timeout') or defaults.VSHARD_TIMEOUT
        local value = channel:get(timeout)
        if value == nil then
            local timeout_error = box.error.last()
            return nil, kafka_producer_error:new('Kafka produce timeout error: %s', timeout_error)
        end
        release_item(channel)
        if not value then
            return kafka_producer_error:new('Kafka produce timeout error: %s', value)
        end
    end

    return true
end

local function handle_kafka_request(obj)
    local data, err = json_error:pcall(json.decode, obj.message:value())
    if not data then
        return nil, err
    end

    if obj.token_name ~= nil and not auth.authorize_with_token_name(obj.token_name) then
        return nil, kafka_error:new('Handling kafka request failed: request is not authorised')
    end

    local span = tracing.start_span('connector.handle_kafka_request')

    local rc, err = vars.server.handle_request(data, vars.routing_key, {is_async = vars.is_async})

    span:finish({error = err})

    if not rc then
        return nil, kafka_error:new('Handling kafka request failed: %s', err)
    end

    return true
end

local function check_kafka_worker_could_be_authorized(token_name, enable_logging)
    if token_name == nil then
        if not auth.is_anonymous_allowed() then
            if enable_logging then
                log.warn('Anonymous access is denied but token name is not provided for kafka workers. ' ..
                    'Retry after %ds', DEFAULT_KAFKA_AUTH_TIMEOUT)
            end
            return false
        else
            log.verbose('Kafka worker runs without token')
            return true
        end
    end

    local ok, err = auth.is_token_name_valid(token_name)
    if not ok then
        if enable_logging then
            log.warn('Kafka worker failed to authorize: %s. Retry after %ds',
                err, DEFAULT_KAFKA_AUTH_TIMEOUT)
        end
        return false
    else
        log.verbose('Kafka worker could be authorized with token %q', token_name)
        return true
    end
end

local function add_consumer(opts)
    opts = opts or {}
    checks({ name = 'string',
             brokers = 'table',
             topics = 'table',
             group = 'string|number',
             token_name = '?string',
             options = '?table',
             kafka_workers_count = 'number',
    })

    local options = utils.merge_maps(DEFAULT_KAFKA_CONSUMER_OPTS, opts.options or {})
    -- Only 1 consumer in a group will read the exact message
    options['group.id'] = tostring(opts.group)

    -- TODO: Check that brockers and topics has at least 1 element
    local consumer, err = kafka.Consumer.create({
        brokers = table.concat(opts.brokers, ','),
        options = options,
        error_callback = gen_error_callback('consumer', opts.name),
        default_topic_options = {
            ["auto.offset.reset"] = "earliest",
        },
    })

    if not consumer then
        return nil, kafka_consumer_error:new(err)
    end

    local err = consumer:subscribe(opts.topics) -- array of topics to subscribe
    if err then
        return nil, kafka_consumer_error:new('Failed to subscribe: %s', err)
    end

    local msg_channel, err = consumer:output()
    if not msg_channel then
        return nil, kafka_consumer_error:new('Consumer error: %s', err)
    end

    vars.consumers[opts.name] = consumer

    local context = not request_context.is_empty() and request_context.get() or {}
    local workers = {}
    for i = 1, opts.kafka_workers_count do
        local worker = fiber.new(function()
            request_context.init(context)
            -- There is no any sense to start without
            while #cartridge.rpc_get_candidates('runner') == 0 do
                if i == 1 then -- Don't spam into logs
                    log.warn('Kafka workers could not start without "runner" role enabled. Retry after %ds',
                        DEFAULT_KAFKA_AUTH_TIMEOUT)
                end
                fiber.sleep(DEFAULT_KAFKA_AUTH_TIMEOUT)
            end

            -- Check token is valid or authorization is disabled.
            -- If authorization is disabled it's possible to omit token name.
            -- Otherwise token is checked for existence and validity.
            -- In case of failure there will be next retry attempt after DEFAULT_KAFKA_AUTH_TIMEOUT.
            -- NB: logging is enabled only the first fiber - to avoid spam into logs.
            while not check_kafka_worker_could_be_authorized(opts.token_name, i == 1) do
                fiber.sleep(DEFAULT_KAFKA_AUTH_TIMEOUT)
            end

            while true do
                local message = msg_channel:get()
                if message == nil and msg_channel:is_closed() then
                    log.info('Consumer\'s "%s" output channel has been closed. Kafka worker #%s stopped.', opts.name, i)
                    return
                end
                local ok, err = handle_kafka_request({
                    message = message,
                    token_name = opts.token_name,
                })
                if ok then
                    local err = consumer:store_offset(message)
                    if err ~= nil then
                        log.error(kafka_error:new("Got error while committing message: %s", err))
                    end
                else
                    log.error(kafka_error:new('Handle request: %s', err))
                end
            end
        end)
        worker:name('kafka_'..opts.name..'_worker_' .. tostring(i))
        table.insert(workers, worker)
    end
    vars.workers[opts.name] = workers

    return true
end

local function setup(input)
    local ok, err = add_consumer({
        name = input.name,
        brokers = input.brokers,
        group = input.group_id,
        topics = input.topics,
        token_name = input.token_name,
        options = input.options,
        kafka_workers_count = input.kafka_workers_count or DEFAULT_KAFKA_WORKERS_COUNT,
    })
    if not ok then return nil, err end

    if input.routing_key ~= nil then
        log.info('Adding Kafka input: (%s) "%s" -> %s',
            table.concat(input.topics, ', '),
            input.group_id,
            input.routing_key)
    else
        log.info('Adding Kafka input: (%s) "%s"',
            table.concat(input.topics, ', '),
            input.group_id)
    end

    vars.routing_key = input.routing_key
    vars.is_async = input.is_async

    return true
end

local function remove_consumer(name)
    log.info('Remove Kafka consumer %q', name)

    local workers = vars.workers[name] or {}
    vars.workers[name] = nil
    -- Workers fibers should finish automatically after consumer closing.
    -- Canceling here just for double check. No need to validate an answer.
    for _, worker in pairs(workers) do
        pcall(worker.cancel, worker)
    end

    local consumer = vars.consumers[name]
    if consumer ~= nil then
        fiber.new(function()
            local ok, err = consumer:close()
            if not ok then
                err = kafka_consumer_error:new('Error closing consumer %q: %s', name, err)
                log.error('%s', err)
            end
        end)

        vars.consumers[name] = nil
    end

    return true
end

local function cleanup(name)
    vars.routing_key = nil

    return remove_consumer(name)
end

local function init()
    vars.consumers = vars.consumers or {}
    vars.producers = vars.producers or {}
    vars.topics = vars.topics or {}
    vars.workers = vars.workers or {}
    vars.server = require('connector.server')
end

return {
    init = init,
    stop = function() end,
    setup = setup,
    cleanup = cleanup,

    add_producer = add_producer,
    remove_producer = remove_producer,
    produce = produce,
}
