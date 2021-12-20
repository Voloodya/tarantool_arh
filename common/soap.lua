local luarapidxml = require('luarapidxml')
local strfind = string.find

---------------------------------------------------------------------
-- @param attr Table of object's attributes.
-- @return String with the value of the namespace ("xmlns") field.
---------------------------------------------------------------------
local function find_xmlns (attr)
    for a, v in pairs (attr) do
        if strfind (a, "xmlns", 1, 1) then
            return v
        end
    end
end

local function encode(data)
    return luarapidxml.encode(data)
end

-- Iterates over the children of an object.
-- It will ignore any text, so if you want all of the elements, use ipairs(obj).
-- @param obj Table (LOM format) representing the XML object.
-- @param tag String with the matching tag of the children
--	or nil to match only structured children (single strings are skipped).
-- @return Function to iterate over the children of the object
--	which returns each matching child.

local function list_children(obj, tag)
    local i = 0
    return function()
        i = i + 1
        local v = obj[i]
        while v do
            if type(v) == "table" and (not tag or v.tag == tag) then
                return v
            end
            i = i + 1
            v = obj[i]
        end
        return nil
    end
end

---------------------------------------------------------------------
-- Converts a SOAP message into Lua objects.
-- @param doc String with SOAP document.
-- @return String with namespace, String with method's name and
--	Table with SOAP elements (LuaExpat's format).
---------------------------------------------------------------------
local function decode(doc)
    local obj, err = luarapidxml.decode (doc)
    if err ~= nil then
        error(err)
    end
    local ns = obj.tag:match ("^(.-):")
    assert (obj.tag == ns..":Envelope", "Not a SOAP Envelope: "..
        tostring(obj.tag))
    local lc = list_children (obj)
    local o = lc()
    -- Skip SOAP:Header
    while o and (o.tag == ns..":Header" or o.tag == "SOAP-ENV:Header") do
        o = lc()
    end
    if o and (o.tag == ns..":Body" or o.tag == "SOAP-ENV:Body") then
        obj = list_children (o)()
    else
        error ("Couldn't find SOAP Body!")
    end

    local namespace = find_xmlns (obj.attr)
    local method = obj.tag:match ("%:([^:]*)$") or obj.tag
    local entries = {}
    for i = 1, #obj do
        entries[i] = obj[i]
    end
    return namespace, method, entries
end

return {
    decode = decode,
    encode = encode,
}
