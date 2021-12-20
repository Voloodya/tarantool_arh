local log = require('log')

local M = {}

function M.connect(self)
    assert(not self.active, "Pool is active")
    assert(#self.connections == 0)

    for _=1, self.size do
        local conn, errs = self.env:connect(self.dsn, self.connect_opts)
        if errs ~= nil then
            log.warn("Can't make connection for pool %q", errs)
        else
            table.insert(self.connections, conn)
            self.queue:put(conn)
        end
    end
    self.active = true
end

function M.acquire(self, timeout)
    assert(self.active, "Pool is not active")
    return self.queue:get(timeout)
end

function M.release(self, conn)
    assert(self.active, "Pool is not active")
    assert(type(conn) == 'table')

    if not conn:is_connected() then
        log.warn("Bad connection is released back to pool")

        local i = 1
        for _, origin in ipairs(self.connections) do
            if origin == conn then
                table.remove(self.connections, i)
                break
            end
            i = i + 1
        end
        local conn, errs = self.env:connect(self.dsn, self.connect_opts)
        if errs ~= nil then
            log.warn("Can't restore connection for pool %q", errs)
        else
            table.insert(self.connections, conn)
        end
    end
    self.queue:put(conn)
end

function M.available(self)
    assert(self.active, "Pool is not active")
    return not self.queue:is_empty()
end

function M.close(self)
    assert(self.active, "Pool is not active")

    self.active = false

    while not self.queue:is_empty() do
        self.queue:get()
    end
    for _, conn in ipairs(self.connections) do
        conn:close()
    end
    self.connections = {}
end

function M.execute(self, ...)
    assert(self.active, "Pool is not active")

    local conn = self:acquire()
    local ok, res, err = pcall(conn.execute, conn, ...)
    self:release(conn)
    if ok then
        return res, err
    end
    error(res)
end

function M.tables(self, ...)
    assert(self.active, "Pool is not active")
    local conn = self:acquire()
    local ok, res, err = pcall(conn.tables, conn, ...)
    self:release(conn)
    if ok then
        return res, err
    end
    error(res)
end

function M.datasources(self, ...)
    assert(self.active, "Pool is not active")
    local ok, res, err = pcall(self.env.datasources, self.env, ...)
    if ok then
        return res, err
    end
    error(res)
end

function M.drivers(self, ...)
    assert(self.active, "Pool is not active")
    local ok, res, err = pcall(self.env.drivers, self.env, ...)
    if ok then
        return res, err
    end
    error(res)
end

for _, implementation in pairs(M) do
    if type(implementation) == 'function' then
        jit.off(implementation, true)
    end
end

return M
