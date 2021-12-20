local fun = require('fun')

local OPERATORS = {
    ['=='] = function(v1, v2) return v1 == v2 end,
    ['>'] = function(v1, v2) return v1 > v2 end,
    ['<'] = function(v1, v2) return v1 < v2 end,
    ['>='] = function(v1, v2) return v1 >= v2  end,
    ['<='] = function(v1, v2) return v1 <= v2  end,
}
local SUPPORTED_OPERATORS = fun.iter(OPERATORS):map(function(op) return op end):totable()
local VERSION_PATTERN = '(%d+)%.?(%d*)%.?(%d*)'
local SCM_PATTERN = 'scm%-(%d+)'
local SEMVER_TYPE = 'semver'

local semver_mt = {
    __type = SEMVER_TYPE,
    __eq = function(v1, v2)
        return v1.major == v2.major and v1.minor == v2.minor and v1.patch == v2.patch
    end,
    __lt = function(v1, v2)
        return (v1.major < v2.major or (v1.major == v2.major and
            (v1.minor < v2.minor or (v1.minor == v2.minor and
                v1.patch < v2.patch))))
    end,
    __le = function(v1, v2)
        return v1 == v2 or v1 < v2
    end,
}

local function parse_version(ver)
    if type(ver) == 'table' and getmetatable(ver).__type == SEMVER_TYPE then
        return ver
    end

    local pattern = ver:startswith('scm-') and SCM_PATTERN or VERSION_PATTERN
    local s_major, s_minor, s_patch = ver:match(pattern)
    local major, minor, patch = tonumber(s_major), tonumber(s_minor), tonumber(s_patch)
    if not major then
        return nil, ('Cannot parse version "%s", expected "major[.minor[.patch]]" format'):format(ver)
    end

    return setmetatable({
        major = major,
        minor = minor,
        patch = patch,
    }, semver_mt)
end

local v = parse_version

local function validate_operator(op)
    return OPERATORS[op] ~= nil
end

local function validate_version(ver)
    return v(ver) ~= nil
end

local function compare(op, v1, v2)
    assert(validate_operator(op))
    local ok, comparison = pcall(OPERATORS[op], v(v1), v(v2))
    return ok and comparison
end

return {
    validate_operator = validate_operator,
    validate_version = validate_version,
    parse_version = parse_version,
    compare = compare,
    SUPPORTED_OPERATORS = SUPPORTED_OPERATORS,
}
