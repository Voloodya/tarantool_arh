local ffi = require('ffi')
local lualdap = require('lualdap')


local LDAP_SASL_SIMPLE = ""
local LDAP_VERSION3 = 3
local LDAP_OPT_PROTOCOL_VERSION = 0x0011
local LDAP_OPT_DEBUG_LEVEL = 0x5001


local LDAP_SCOPE_BASE = 0x0000
local LDAP_SCOPE_ONELEVEL = 0x0001
local LDAP_SCOPE_SUBTREE = 0x0002

local LDAP_MSG_ONE = 0x00

local LDAP_RES_SEARCH_ENTRY = 0x64
local LDAP_RES_SEARCH_RESULT = 0x65
local LDAP_RES_SEARCH_REFERENCE = 0x73

-- https://git.openldap.org/openldap/openldap/-/blob/master/include/ldap.h
local LDAP_AUTH_METHOD_NOT_SUPPORTED = 0x07
local LDAP_INAPPROPRIATE_AUTH = 0x30

if not pcall(ffi.typeof, 'struct timeval') then
    if ffi.os == 'OSX' then
        ffi.cdef([[
            typedef int32_t suseconds_t;
            struct timeval {
                long        tv_sec;     /* seconds */
                suseconds_t tv_usec;    /* microseconds */
            };
        ]])
    else
        ffi.cdef([[
            struct timeval {
                long tv_sec;     /* seconds */
                long tv_usec;    /* microseconds */
            };
        ]])
    end
end

if not pcall(ffi.typeof, 'LDAP') then
    ffi.cdef([[
        typedef unsigned long ber_len_t;

        typedef struct berval {
            ber_len_t bv_len;
            char *bv_val;
        } BerValue, *BerVarray;

        struct berval *ber_bvstrdup(const char *str);
        struct berval *ber_str2bv(const char *str, unsigned long len,
                                  int duplicate, struct berval *bv);

        void ber_bvfree(struct berval *bv);

        typedef struct LDAP LDAP;
        typedef struct LDAPControl LDAPControl;
        typedef struct LDAPMessage LDAPMessage;
        typedef struct BerElement BerElement;

        LDAP *ldap_init(char *host, int port);
        int async_ldap_initialize(LDAP **ldp, const char *uri);

        int async_ldap_sasl_bind_s(LDAP *ld, const char *dn, const char *mechanism,
                             struct berval *cred, LDAPControl *sctrls[],
                             LDAPControl *cctrls[], struct berval **servercredp);

        int async_ldap_start_tls_s(LDAP *ld, LDAPControl **serverctrls, LDAPControl **clientctrls);

        int ldap_unbind_ext(LDAP *ld, LDAPControl *sctrls[],
                            LDAPControl *cctrls[]);

        int ldap_set_option(LDAP *ld, int option, const void *invalue);

        char *ldap_err2string( int err );

        LDAPMessage *ldap_first_message( LDAP *ld, LDAPMessage *result );
        LDAPMessage *ldap_first_entry( LDAP *ld, LDAPMessage *result );

        LDAPMessage *ldap_first_reference( LDAP *ld, LDAPMessage *result );

        char *ldap_first_attribute(LDAP *ld, LDAPMessage *entry, BerElement **berptr );
        char *ldap_next_attribute(LDAP *ld, LDAPMessage *entry, BerElement *ber );

        char *ldap_get_dn( LDAP *ld, LDAPMessage *entry );

        struct berval **ldap_get_values_len(LDAP *ld, LDAPMessage *entry, char *attr);
        int ldap_count_values_len(struct berval **vals);

        int ldap_msgtype( LDAPMessage *msg );
]])
end

local function ldap_err2string(err)
    return ffi.string(ffi.C.ldap_err2string(err))
end

local function ldap_close(ld)
    if ld == nil or ld[0] == ffi.cast('LDAP*', 0) then
        return true
    end

    ffi.C.ldap_unbind_ext(ld[0], ffi.cast('LDAPControl**', 0),
                          ffi.cast('LDAPControl**', 0))
    ld[0] = ffi.cast('LDAP*', 0)

    return true
end

local function ldap_set_version(ld, version)
    if ld == nil or ld[0] == ffi.cast('LDAP*', 0) then
        return nil, "ldap_set_version(): ldap structure not initialized"
    end

    local ver = ffi.new("int[1]")
    ver[0] = version

    local err = ffi.C.ldap_set_option(
        ld[0],
        LDAP_OPT_PROTOCOL_VERSION,
        ffi.cast('void*', ver))

    if err ~= 0 then
        return nil, 'Error setting LDAP version: ' .. tostring(err)
    end

    return true
end

local function ldap_set_debug_level(ld, level)
    if ld == nil or ld[0] == ffi.cast('LDAP*', 0) then
        return nil, "ldap_set_debug_level(): ldap structure not initialized"
    end

    local lvl = ffi.new("int[1]")
    lvl[0] = level

    local err = ffi.C.ldap_set_option(
        ld[0],
        LDAP_OPT_DEBUG_LEVEL,
        ffi.cast('void*', lvl))

    if err ~= 0 then
        return nil, 'Error setting LDAP debug level'
    end

    return true
end

local function ldap_initialize(host)
    if not string.startswith(host, 'ldap://') then
        host = 'ldap://' .. host
    end

    local ld = ffi.new("LDAP*[1]")
    ld[0] = ffi.cast('LDAP*', 0)

    ffi.gc(ld, ldap_close)

    local err = lualdap.ldap_initialize(ld, host)

    if err ~= 0 then
        return nil, ldap_err2string(err)
    end

    return ld
end

local function ber_bvstrdup(str)
   local berval = ffi.C.ber_str2bv(str, 0, 1, ffi.cast('struct berval*', 0))
   ffi.gc(berval, ffi.C.ber_bvfree)

   return berval
end

local function ldap_simple_bind(ld, who, password)
    if ld == nil or ld[0] == ffi.cast('LDAP*', 0) then
        return nil, "ldap_simple_bind(): ldap structure not initialized"
    end

    return lualdap.ldap_simple_bind_s(ld[0], who, password)
end

local function ldap_sasl_bind(ld, who, password)
    if ld == nil or ld[0] == ffi.cast('LDAP*', 0) then
        return nil, "ldap_sasl_bind(): ldap structure not initialized"
    end

    local cred = ber_bvstrdup(password)

    local err = lualdap.ldap_sasl_bind_s(ld[0], who, LDAP_SASL_SIMPLE,
                                         cred, ffi.cast('LDAPControl**', 0),
                                         ffi.cast('LDAPControl**', 0),
                                         ffi.cast('struct berval**', 0))

    if err == LDAP_INAPPROPRIATE_AUTH or err == LDAP_AUTH_METHOD_NOT_SUPPORTED then
       err = ldap_simple_bind(ld, who, password)
    end

    if err ~= 0 then
        return nil, ldap_err2string(err)
    end

    return true
end

local function ldap_start_tls(ld)
    if ld == nil or ld[0] == ffi.cast('LDAP*', 0) then
        return nil, "ldap_start_tls(): ldap structure not initialized"
    end

    local err = lualdap.ldap_start_tls_s(ld[0],
                                         ffi.cast('LDAPControl**', 0),
                                         ffi.cast('LDAPControl**', 0))

    if err ~= 0 then
        return nil, ldap_err2string(err)
    end

    return true
end

local function ldap_open_simple(host, user, password, use_tls)
    if use_tls == nil then use_tls = false end

    local ld, err = ldap_initialize(host)

    if ld == nil then
        return nil, err
    end

    local res, err = ldap_set_debug_level(ld, 7)

    if res == nil then
        return nil, err
    end

    local res, err = ldap_set_version(ld, LDAP_VERSION3)

    if res == nil then
        return nil, err
    end

    if use_tls then
        local res, err = ldap_start_tls(ld)

        if res ~= nil then
            return nil, err
        end
    end

    local res, err = ldap_sasl_bind(ld, user, password)
    if res == nil then
        return nil, err
    end

    return ld
end

local function scope_name_to_id(scope_name)
    if scope_name == 'subtree' then
        return LDAP_SCOPE_SUBTREE
    elseif scope_name == 'onelevel' then
        return LDAP_SCOPE_ONELEVEL
    elseif scope_name == 'base' then
        return LDAP_SCOPE_BASE
    end

    return nil
end

local function with_default(option, default)
    if option ~= nil then
        return option
    end

    return default
end

local function number_to_timeval(num)
    if num == nil then
       return ffi.cast('struct timeval*', 0)
    end

    local tv = ffi.new('struct timeval[1]')
    local tv_sec = math.floor(num)
    tv[0].tv_sec = tv_sec
    tv[0].tv_usec = math.floor(1000000 * (num - tv_sec))

    return tv
end

local function ldap_get_attributes(ld, entry)
    local ber = ffi.new("BerElement*[1]")
    ber[0] = ffi.cast('BerElement*', 0)

    local attr = ffi.C.ldap_first_attribute(ld[0], entry, ber);

    local res = {}

    while attr ~= ffi.cast('char*', 0) do
        local attr_str = ffi.string(attr)

        local values = ffi.C.ldap_get_values_len(ld[0], entry, attr)
        local values_len = ffi.C.ldap_count_values_len(values)

        if values_len == 0 then
            res[attr_str] = true
        elseif values_len == 1 then
            local value = ffi.string(values[0].bv_val, values[0].bv_len)
            res[attr_str] = value
        else
            res[attr_str] = {}

            for i = 0, values_len - 1 do
                local value = ffi.string(values[i].bv_val, values[i].bv_len)
                table.insert(res[attr_str], value)
            end
        end

        attr = ffi.C.ldap_next_attribute(ld[0], entry, ber[0])
    end

    return res
end

local function ldap_search_next_message(ld, msgid, timeout_tv)
    local res = ffi.new("LDAPMessage*[1]")
    res[0] = ffi.cast('LDAPMessage*', 0)

    local rc = lualdap.ldap_result(ld, msgid[0], LDAP_MSG_ONE, timeout_tv[0], res)

    if rc == 0 then
        return nil, "ldap_search_next_message(): timeout exceeded"
    elseif rc == -1 then
        return nil, "ldap_search_next_message(): result error"
    elseif rc == LDAP_RES_SEARCH_RESULT then
        return nil
    else
        local message = ffi.C.ldap_first_message(ld[0], res[0])
        local msg_type = ffi.C.ldap_msgtype(message)

        if msg_type == LDAP_RES_SEARCH_ENTRY then
            local entry = ffi.C.ldap_first_entry(ld[0], message)
            local dn = ffi.string(ffi.C.ldap_get_dn(ld[0], entry))

            local attrs = ldap_get_attributes(ld, entry)

            return {dn=dn, attrs=attrs}
        elseif msg_type == LDAP_RES_SEARCH_REFERENCE then
            local ref = ffi.C.ldap_first_reference(ld[0], message)
            local dn = ffi.string(ffi.C.ldap_get_dn(ld[0], ref))

            return {dn=dn}
        elseif msg_type == LDAP_RES_SEARCH_RESULT then
            return nil
        end
    end

    return nil
end

local function ldap_search(ld, options)
    if ld == nil or ld[0] == ffi.cast('LDAP*', 0) then
        return nil, "ldap_search(): ldap structure not initialized"
    end

    if options == nil or type(options) ~= 'table' then
        return nil, "ldap_search(): options must be a table"
    end

    if options.base == nil then
        return nil, "ldap_search(): options.base argument is mandatory"
    end

    if options.scope ~= nil and scope_name_to_id(options.scope) == nil then
        return nil, string.format("ldap_search(): incorrect options.scope '%s'", options.scope)
    end

    if options.timeout ~= nil and type(options.timeout) ~= "number" then
        return nil, "ldap_search(): options.timeout should be a number"
    end

    local attrsonly = with_default(options.attrsonly, false)
    local base = options.base
    local filter = with_default(options.filter, "(objectclass=*)")
    local scope = scope_name_to_id(with_default(options.scope, "subtree"))
    local sizelimit = options.sizelimit
    local timeout = options.timeout
    local attrs = table.copy(with_default(options.attrs, {}))
    local msgid = ffi.new("int[1]")

    table.insert(attrs, ffi.cast('char*', 0))
    local cattrs = ffi.new('const char*[?]', #attrs, attrs)

    local timeout_tv = number_to_timeval(timeout)

    local err = lualdap.ldap_search_ext(ld[0],
                                        base,
                                        scope,
                                        filter,
                                        cattrs,
                                        attrsonly and 1 or 0,
                                        ffi.cast('LDAPControl**', 0),
                                        ffi.cast('LDAPControl**', 0),
                                        timeout_tv[0],
                                        sizelimit,
                                        msgid)

    if err ~= 0 then
        return nil, ldap_err2string(err)
    end

    local function next_message()
        if ld[0] == ffi.cast('LDAP*', 0) then
            return nil, "ldap structure not initialized"
        end

        local res, err = ldap_search_next_message(ld, msgid, timeout_tv)

        return res, err
    end

    return next_message
end

return {open=ldap_open_simple,
        close=ldap_close,
        search=ldap_search}
