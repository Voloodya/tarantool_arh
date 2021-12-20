local checks = require('checks')

local DEFAULT_CHUNK_SIZE = 4096

local function read(readable, from, size)
    from = from or 1
    size = size or DEFAULT_CHUNK_SIZE
    checks('string|table|cdata', 'number', 'number')
    assert(size > 0, 'read: size argument must be greater than zero')

    local chunk
    if type(readable) ~= 'string' and type(readable.read) == 'function' then
        chunk = readable:read(size) -- what if file closed
    else
        chunk = readable:sub(from, from + size - 1)
    end

    return #chunk, chunk
end

local NEWLINE_PATTERN = '[\r\n]+'

local function iter(state)
    local buf = state.buf
    local readable = state.readable
    local chunk_size = state.chunk_size or DEFAULT_CHUNK_SIZE
    local newl, nextl = buf:find(NEWLINE_PATTERN)

    -- TODO: Protect this from full file read (introduce MAX_OBJECT_SIZE)
    local eof = false
    while not eof do
        local next_line_found = type(nextl) == 'number' and nextl < #buf
        if next_line_found then break end

        local count, chunk = read(readable, state.rpos, chunk_size)

        buf = buf .. chunk
        -- -count-1 need to catch newline from previous read
        newl, nextl = buf:find(NEWLINE_PATTERN, -count-1)
        state.rpos = state.rpos + count

        eof = count < chunk_size
    end

    if buf ~= "" then
        newl = newl or #buf + 1
        nextl = (nextl or #buf) + 1
        local res = buf:sub(1, newl - 1)
        state.buf = buf:sub(nextl)
        state.count = state.count + 1
        state.offset = state.next_offset
        state.next_offset = state.next_offset + nextl - 1
        return state.count, res
    end
end

local function iterate(readable, opts) -- 2nd arg is opts
    opts = opts or {}
    checks('string|table|cdata', 'table')
    return iter, {
        count = 0,
        rpos = 1,
        offset = 0,
        next_offset = 1,
        chunk_size = opts.chunk_size,
        readable = readable,
        buf = "",
    }
end

return {
    iterate = iterate,
}
