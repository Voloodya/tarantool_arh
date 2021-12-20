local module_name = 'common.sandbox.shared_storage'

local fiber = require('fiber')
local checks = require('checks')
local membership = require('membership')
local log = require('log.log').new(module_name)
local tenant = require('common.tenant')

local vars = require('common.vars').new(module_name)

-- Singleton for instances
vars:new('instances'
    --[namespace] = {
    --    [key] = {
    --        value = v,
    --        is_fetched = true/false,
    --        generation = 0,
    --    }
    --}
)

-- Fiber to update sandbox storage from "upstream"
vars:new('sandbox_update_worker', nil)
-- Cond variable instead of fiber.sleep to
-- get ability run update by hand
vars:new('need_to_update_condition', nil)

local function get_bucket_id(key)
    checks("?string")
    return vshard.router.bucket_id_strcrc32(key)
end

local function run_synchronization()
    if vars.need_to_update_condition ~= nil then
        vars.need_to_update_condition:signal()
    end
end

_G.sandbox_proxy = {
    set = function(namespace, key, generation)
        checks('string', 'string', 'number')
        if vars.instances[namespace] == nil then
            return nil
        end

        local shared_storage = vars.instances[namespace]
        local local_generation = shared_storage.generation

        -- This mean that we drop some changes
        if generation - local_generation > 1 then
            run_synchronization()
        end

        shared_storage.state[key].is_fetched = false
        shared_storage.state[key].generation = generation
    end,
}

local function get(self, key)
    checks('table', 'string')
    if self.state[key] == nil or not self.state[key].is_fetched then
        local namespace = self.namespace
        local bucket_id = get_bucket_id(namespace)
        -- write call because add instance to subscribers list
        local result, err = vshard.router.call(bucket_id, 'write', 'vshard_sandbox.get',
            { namespace, key, membership.myself().uri})
        if err ~= nil then
            log.error('Failed to obtain key %s for sandbox %s: %s', key, namespace, err)
            return nil, err
        end

        self.state[key] = {
            value = result[1], is_fetched = true, generation = result[2],
        }

        if result[2] - self.generation > 1 then
            run_synchronization()
        end
    end

    local value = self.state[key].value
    if value == nil then
        value = nil     -- rewrite box.NULL
    end

    return value
end

local function set(self, key, value)
    checks('table', 'string', '?')
    local namespace = self.namespace
    local bucket_id = get_bucket_id(namespace)
    local result, err = vshard.router.call(bucket_id, 'write', 'vshard_sandbox.set',
        { namespace, key, value, membership.myself().uri, bucket_id })
    if err ~= nil then
        log.error('Failed to set key %s for sandbox %s: %s', key, namespace, err)
        return nil, err
    end

    local generation = result[2]
    self.state[key] = { value = value, is_fetched = true, generation = generation }
    if generation - self.generation == 1 then
        self.generation = generation
    else
        run_synchronization()
    end

    return value
end

local function update_local_storage(instance, key_gens, max_generation)
    instance.generation = max_generation
    for _, pair in ipairs(key_gens) do
        local key = pair[1]
        local generation = pair[2]
        if instance.state[key] == nil then
            instance.state[key] = { is_fetched = false, generation = generation }
        -- Prevent overwriting of actual local tuples(key-value pairs)
        elseif instance.state[key].generation < generation then
            instance.state[key].is_fetched = false
        end
    end
end

-- Sometimes we want to wakeup this worker manually
-- when understand that lost history of some changes
local UPDATE_TIMEOUT = 5
local function update_storages()
    while true do
        vars.need_to_update_condition:wait(UPDATE_TIMEOUT)
        for namespace, instance in pairs(vars.instances) do
            local bucket_id = get_bucket_id(namespace)
            local result, err = vshard.router.call(bucket_id, 'read', 'vshard_sandbox.sync',
                { namespace, instance.generation })

            if err == nil then
                update_local_storage(vars.instances[namespace], result[1], result[2])
            else
                log.error('Update storage error: %s', err)
            end
        end
    end
end

local function new(namespace)
    checks('string')
    vars.instances = vars.instances or {}
    if vars.instances[namespace] ~= nil then
        return vars.instances[namespace]
    end

    local self = {
        namespace = namespace,
        state = {},
        generation = 0ULL,
        set = set,
        get = get,
        last_update = 0,
    }

    vars.instances[namespace] = self

    if vars.sandbox_update_worker == nil then
        vars.need_to_update_condition = fiber.cond()
        vars.sandbox_update_worker = tenant.fiber_new(update_storages)
        vars.sandbox_update_worker:name('shared_storage:sandbox_update_worker')
    end

    return self
end

return {
    new = new,
}
