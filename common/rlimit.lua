local bit = require("bit")
local ffi = require("ffi")

local errno = require('errno')

ffi.cdef[[
typedef uint64_t rlim_t;

typedef struct rlimit {
    rlim_t rlim_cur;  /* Soft limit */
    rlim_t rlim_max;  /* Hard limit (ceiling for rlim_cur) */
} rlimit;

int getrlimit(int resource, struct rlimit *rlim);
int setrlimit(int resource, const struct rlimit *rlim);

]]

local RLIMIT_CORE = 4
local RLIMIT_NOFILE

if ffi.os == 'Linux' then
    RLIMIT_NOFILE = 7
elseif ffi.os == 'OSX' then
    RLIMIT_NOFILE = 8
else
    error('Unsupported OS: ' .. ffi.os)
end

local RLIM_INFINITY = bit.lshift(1ULL, 63) - 1
local OPEN_MAX = 10240

local function getrlimit(resource)
    local rlimit = ffi.new("rlimit")
    local rc = ffi.C.getrlimit(resource, rlimit)
    if rc ~= 0 then
        return nil, errno.strerror()
    end

    return {rlim_cur = rlimit.rlim_cur,
            rlim_max = rlimit.rlim_max}
end

local function setrlimit(resource, limit)
    local rlimit = ffi.new('rlimit')
    rlimit.rlim_cur = limit.rlim_cur
    rlimit.rlim_max = limit.rlim_max
    local rc = ffi.C.setrlimit(resource, rlimit)
    if rc ~= 0 then
        return nil, errno.strerror()
    end

    return {rlim_cur = rlimit.rlim_cur,
            rlim_max = rlimit.rlim_max}
end

return {
    RLIM_INFINITY = RLIM_INFINITY,
    RLIMIT_CORE = RLIMIT_CORE,
    RLIMIT_NOFILE = RLIMIT_NOFILE,
    OPEN_MAX = OPEN_MAX,
    getrlimit = getrlimit,
    setrlimit = setrlimit,
}
