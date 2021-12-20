local checks = require('checks')
local fiber = require('fiber')

local function is_lock_released(self)
    if not self.is_released then
        self.is_released = fiber.clock() - self.start_time >= self.timeout
    end
    return self.is_released
end

local function wait_lock(self)
    return self.lock:wait(self.timeout)
end

local function broadcast_and_release(self)
    self.lock:broadcast()
    self.is_released = true
end

local metatable = {
    __index = {
        released = is_lock_released,
        wait = wait_lock,
        broadcast_and_release = broadcast_and_release,
    }
}

--
-- lock_with_timeout
-- Use to create fiber lock until it is released or timeout occured.
-- Useful when fiber is created in critical section and it is not guaranteed to be finished.
--
local function new(timeout_seconds)
    checks('number')
    if timeout_seconds < 0 then
        error('Non-negative timeout expected')
    end

    local self = {
        lock = fiber.cond(),
        start_time = fiber.clock(),
        timeout = timeout_seconds,
        is_released = false,
    }

    return setmetatable(self, metatable)
end

return {
    new = new
}
