local bounded_queue = {}

function bounded_queue:is_empty()
    return self.first == 0
end

function bounded_queue:is_full()
    return self.last - self.first == -1
        or self.last - self.first + 1 == self.max_length
end

function bounded_queue:push(value)
    self.last = self.last + 1
    if self.last > self.max_length then
        self.last = 1
    end

    self.buffer[self.last] = value

    if self.first == self.last then
        self.first = self.first + 1
        if self.first > self.max_length then
            self.first = 1
        end
    elseif self.first == 0 then
        self.first = 1
    end
end

function bounded_queue:pop()
    if self:is_empty() then
        return nil
    end

    local value = self.buffer[self.first]

    if self.first == self.last then
        self.first = 0
        self.last = 0
    else
        self.first = self.first + 1
        if self.first > self.max_length then
            self.first = 1
        end
    end

    return value
end

function bounded_queue:clear()
    self.buffer = {}
    self.first = 0
    self.last = 0
end

function bounded_queue.new(max_length)
    assert(type(max_length) == 'number' and max_length > 0,
        "bounded_queue.new(): Max length of buffer must be a positive integer")

    local instance = {
        buffer = {},
        first = 0,
        last = 0,
        max_length = max_length,
        is_empty = bounded_queue.is_empty,
        is_full = bounded_queue.is_full,
        push = bounded_queue.push,
        pop = bounded_queue.pop,
        clear = bounded_queue.clear
    }

    return instance
end

return bounded_queue
