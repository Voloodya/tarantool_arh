local checks = require('checks')

local priority_queue = {}

local TOP = 1
local PRIORITY = 1
local VALUE = 2

function priority_queue:length()
    return self.cur_length
end

function priority_queue:is_empty()
    return self.cur_length == 0
end

function priority_queue:push(priority, value)
    self.cur_length = self.cur_length + 1
    self.heap[self.cur_length] = { priority, value }

    local i = self.cur_length
    local half = math.floor(i / 2)
    while half > 0 do
        if self.comparator(self.heap[i][PRIORITY], self.heap[half][PRIORITY]) then
            self.heap[i], self.heap[half] = self.heap[half], self.heap[i]
        end
        i = half
        half = math.floor(i / 2)
    end
end

function priority_queue:peek()
    local top = self.heap[TOP]
    if top == nil then
        return nil
    end
    return top[VALUE]
end

local function next_top(self, i)
    if (i * 2) + 1 > self.cur_length then
        return i * 2
    else
        if self.comparator(self.heap[i * 2][PRIORITY], self.heap[i * 2 + 1][PRIORITY]) then
            return i * 2
        else
            return i * 2 + 1
        end
    end
end

function priority_queue:pop()
    local top_value = self:peek()
    if top_value == nil then
        return nil
    end

    self.heap[TOP] = self.heap[self.cur_length]
    self.heap[self.cur_length] = nil
    self.cur_length = self.cur_length - 1

    local i = 1
    while (i * 2) <= self.cur_length do
        local j = next_top(self, i)
        if self.comparator(self.heap[j][PRIORITY], self.heap[i][PRIORITY]) then
            self.heap[i], self.heap[j] = self.heap[j], self.heap[i]
        end
        i = j
    end

    return top_value
end

function priority_queue:clear()
    self.cur_length = 0
    self.heap = {}
end

local default_comparator = function(a, b) return a < b end

function priority_queue.new(comparator)
    checks('?function')

    if comparator == nil then
        comparator = default_comparator
    end

    local instance = {
        heap = {},
        cur_length = 0,
        length = priority_queue.length,
        is_empty = priority_queue.is_empty,
        push = priority_queue.push,
        peek = priority_queue.peek,
        pop = priority_queue.pop,
        clear = priority_queue.clear,
        comparator = comparator,
    }
    return instance
end

return priority_queue
