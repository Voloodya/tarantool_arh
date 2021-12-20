local checks = require('checks')
package.loaded['checks'] = nil
local forever_checks = require('checks')
package.loaded['checks'] = checks

local function get_bucket_id(namespace, key)
    forever_checks('string', 'string')
    return vshard.router.bucket_id_strcrc32({ namespace, key })
end

local function put_element(self, key, index, value)
    forever_checks('table', 'string', 'number', '?')
    if key == '' then
        return nil, 'key must be not empty'
    end
    if index < 1 then
        return nil, 'index must be greater than 0'
    end

    local bucket_id = get_bucket_id(self.namespace, key)
    return vshard.router.call(
        bucket_id, 'write', 'vshard_blob_storage.put_element',
        { self.namespace, key, index, value, bucket_id }
    )
end

local function append_element(self, key, value)
    forever_checks('table', 'string', '?')
    if key == '' then
        return nil, 'key must be not empty'
    end

    local bucket_id = get_bucket_id(self.namespace, key)
    return vshard.router.call(
        bucket_id, 'write', 'vshard_blob_storage.append_element',
        { self.namespace, key, value, bucket_id }
    )
end

local function get_element(self, key, index)
    forever_checks('table', 'string', 'number')
    if key == '' then
        return nil, 'key must be not empty'
    end
    if index < 1 then
        return nil, 'index must be greater than 0'
    end

    local bucket_id = get_bucket_id(self.namespace, key)
    return vshard.router.call(
        bucket_id, 'read', 'vshard_blob_storage.get_element',
        { self.namespace, key, index }
    )
end

local function delete_element(self, key, index)
    forever_checks('table', 'string', 'number')
    if key == '' then
        return nil, 'key must be not empty'
    end
    if index < 1 then
        return nil, 'index must be greater than 0'
    end

    local bucket_id = get_bucket_id(self.namespace, key)
    return vshard.router.call(
        bucket_id, 'write', 'vshard_blob_storage.delete_element',
        { self.namespace, key, index }
    )
end

local function elements_pairs(self, key, start)
    forever_checks('table', 'string', '?number')
    if key == '' then
        return nil, 'key must be not empty'
    end
    if start ~= nil and start < 1 then
        return nil, 'start index must be greater than 0'
    end

    local index = start ~= nil and start - 1 or 0
    return function()
        index = index + 1
        local elem = get_element(self, key, index)
        if elem == nil then
            return nil
        end
        return index, elem
    end
end

local function put(self, key, value)
    forever_checks('table', 'string', '?')
    if key == '' then
        return nil, 'key must be not empty'
    end

    local bucket_id = get_bucket_id(self.namespace, key)
    return vshard.router.call(
        bucket_id, 'write', 'vshard_blob_storage.put',
        { self.namespace, key, value, bucket_id }
    )
end

local function get(self, key)
    forever_checks('table', 'string')
    if key == '' then
        return nil, 'key must be not empty'
    end

    local bucket_id = get_bucket_id(self.namespace, key)
    return vshard.router.call(
        bucket_id, 'read', 'vshard_blob_storage.get',
        { self.namespace, key }
    )
end

local function delete(self, key)
    forever_checks('table', 'string')
    if key == '' then
        return nil, 'key must be not empty'
    end

    local bucket_id = get_bucket_id(self.namespace, key)
    return vshard.router.call(
        bucket_id, 'write', 'vshard_blob_storage.delete',
        { self.namespace, key }
    )
end

local function new(namespace)
    forever_checks('string')
    if namespace == '' then
        return nil, 'namespace name must be not empty'
    end

    return {
        namespace = namespace,

        put_element = put_element,
        append_element = append_element,
        get_element = get_element,
        delete_element = delete_element,
        pairs = elements_pairs,

        put = put,
        get = get,
        delete = delete,
    }
end

return {
    new = new,
}
