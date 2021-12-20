local checks = require('checks')
local digest = require('digest')
local mpencode = require('msgpackffi').encode

-- See https://github.com/tarantool/vshard/issues/207
-- and https://github.com/tarantool/vshard/blob/master/vshard/hash.lua
-- In order to consider 1, 1LL and 1ULL as the same values we should use msgpack encode
local function get_bucket_id_for_key(key)
    checks("?table")

    local c = digest.murmur.new()

    for _, v in ipairs(key) do
        if type(v) ~= 'string' then
            c:update(mpencode(v))
        else
            c:update(v)
        end

    end

    return digest.guava(c:result(), vshard.router.bucket_count() - 1) + 1
end

return {
    get_bucket_id_for_key = get_bucket_id_for_key,
}
