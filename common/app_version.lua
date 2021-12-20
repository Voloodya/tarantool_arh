local fio = require('fio')
local semver = require('common.semver')

local BIG_ENOUGH_CHUNK = 1024
local TDG_VERSION_REQUIREMENT_PATTERN = '([^%s]*)%s*([^%s]*)' -- "[operator][spaces][version]"
local VERSION_FILEPATH = fio.pathjoin(package.searchroot(), 'CURRENT_TDG_VERSION')

local current_version = 'scm-1'

local fd = fio.open(VERSION_FILEPATH, {'O_RDONLY'})
if fd then
    local s = fd:read(BIG_ENOUGH_CHUNK)
    fd:close()

    if type(s) == 'string' then
        current_version = s:strip()
    end
end

local M = {}

function M.get()
    return current_version
end

function M.check(requirement)
    local operator, version = string.match(requirement, TDG_VERSION_REQUIREMENT_PATTERN)

    if #operator == 0 or #version == 0 then
        return false, ('wrong format of tdg_version, expected "[operator][space][version]", got %s')
            :format(requirement)
    end

    if not semver.validate_operator(operator) then
        return false, ('wrong version operator, expected one of "%s", got "%s"')
            :format(table.concat(semver.SUPPORTED_OPERATORS, '", "'), operator)
    end

    if not semver.validate_version(version) then
        return false, ('invalid version, expected semantic version, got "%s"'):format(version)
    end

    if not semver.compare(operator, M.get(), version) then
        return false, ('tdg version mismatch: expected constraints "%s", current "%s"')
            :format(requirement, M.get())
    end

    return true
end

return M
