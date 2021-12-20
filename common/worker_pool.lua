local fiber = require('fiber')
local log = require('log.log').new('common.pool')

-- If it will be greater membership could report a node as dead
local MAX_POOL_SIZE = 500

local function consume(self)
    local arg
    while true do
        arg = self.channel:get()
        if arg == nil then
            return
        end

        self.running = self.running + 1
        local ok, err = pcall(self.callback, unpack(arg, 1, table.maxn(arg)))
        if (not ok) and (not self.silent) then
            log.warn('Task failed: %s', err)
        end
        self.running = self.running - 1
    end
end

local function spawn_workers(self)
    local upper_bound = 2 * self.size
    if upper_bound > MAX_POOL_SIZE then
        upper_bound = MAX_POOL_SIZE
    end

    for i = self.size + 1, upper_bound do
        local f = fiber.new(consume, self, i)
        f:name(self.name .. '_' .. tostring(i))
        table.insert(self.workers, f)
    end
    self.size = 2 * self.size
end

local function process(self, ...)
    if self.running == self.size and self.size < MAX_POOL_SIZE then
        spawn_workers(self)
    end
    return self.channel:put({...})
end

local function destroy(self)
    self.channel:close()
end

local function new(size, callback, opts)
    opts = opts or {}
    local pool = {
        name = opts.name or 'worker_pool',
        size = size,
        callback = callback,
        workers = {},
        channel = fiber.channel(MAX_POOL_SIZE),
        silent = opts.silent == true,
        running = 0,

        process = process,
        destroy = destroy,
    }

    for i = 1, size do
        local f = fiber.new(consume, pool, i)
        f:name(pool.name .. '_' .. tostring(i))
        table.insert(pool.workers, f)
    end

    return pool
end

return {
    new = new,
}
