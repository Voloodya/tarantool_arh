#!/usr/bin/env tarantool

local ffi = require("ffi")

ffi.cdef[[
int isatty(int fd);
]]

-- three standard POSIX file descriptors
local STDIN_NO = 0
local STDOUT_NO = 1
local STDERR_NO = 2

local function isatty(fd)
    return (ffi.C.isatty(fd) == 1)
end

return {
    isatty = isatty,
    STDIN_NO = STDIN_NO,
    STDOUT_NO = STDOUT_NO,
    STDERR_NO = STDERR_NO,
}
