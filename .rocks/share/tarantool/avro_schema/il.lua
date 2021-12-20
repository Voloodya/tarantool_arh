local ffi            = require('ffi')
local json           = require('json').new()
local msgpack        = require('msgpack')
local json_encode    = json and json.encode
local msgpack_decode = msgpack and msgpack.decode
local ffi_new        = ffi.new
local format, rep    = string.format, string.rep
local insert, remove = table.insert, table.remove
local concat         = table.concat
local max            = math.max

json.cfg{encode_use_tostring = true}

local loaded, opcode = pcall(ffi_new, 'struct schema_il_Opcode')

if not loaded then
    ffi.cdef([[
    struct schema_il_Opcode {
        struct {
            uint16_t op;
            union {
                uint16_t scale;
                uint16_t step;
                uint16_t k;
            };
        };
        union {
            uint32_t ripv;
            uint32_t offset;
            uint32_t name;
            uint32_t len;
        };
        union {
            struct {
                uint32_t ipv;
                 int32_t ipo;
            };
            int32_t  ci;
            int64_t  cl;
            double   cd;
        };

        // block
        // ripv ipv ipo




        /* rt.err_type depends on these values */






    };

    struct schema_il_V {
        union {
            uint64_t     raw;
            struct {
                uint32_t gen    :30;
                uint32_t islocal:1;
                uint32_t isdead :1;
                 int32_t inc;
            };
        };
    };
    ]])
    opcode = ffi_new('struct schema_il_Opcode')
end

local op2str = {
    [0xc0   ] = 'CALLFUNC   ',   [0xc1   ] = 'DECLFUNC   ',
    [0xc2    ] = 'IBRANCH    ',   [0xc3    ] = 'SBRANCH    ',
    [0xc4      ] = 'IFSET      ',   [0xc5      ] = 'IFNUL      ',
    [0xc6  ] = 'INTSWITCH  ',   [0xc7  ] = 'STRSWITCH  ',
    [0xc8 ] = 'OBJFOREACH ',   [0xc9       ] = 'MOVE       ',
    [0xca       ] = 'SKIP       ',   [0xcb      ] = 'PSKIP      ',
    [0xcc   ] = 'PUTBOOLC   ',   [0xcd    ] = 'PUTINTC    ',
    [0xce   ] = 'PUTLONGC   ',   [0xcf  ] = 'PUTFLOATC  ',
    [0xd0 ] = 'PUTDOUBLEC ',   [0xd1    ] = 'PUTSTRC    ',
    [0xd2    ] = 'PUTBINC    ',   [0xd3  ] = 'PUTARRAYC  ',
    [0xd4    ] = 'PUTMAPC    ',   [0xd5      ] = 'PUTXC      ',
    [0xd6   ] = 'PUTINTKC   ',   [0xd7  ] = 'PUTDUMMYC  ',
    [0xd8    ] = 'PUTNULC    ',   [0xd9    ] = 'PUTBOOL    ',
    [0xda     ] = 'PUTINT     ',   [0xdb    ] = 'PUTLONG    ',
    [0xdc   ] = 'PUTFLOAT   ',   [0xdd  ] = 'PUTDOUBLE  ',
    [0xde     ] = 'PUTSTR     ',   [0xdf     ] = 'PUTBIN     ',
    [0xe0   ] = 'PUTARRAY   ',   [0xe1     ] = 'PUTMAP     ',
    [0xe2] = 'PUTINT2LONG',   [0xe3 ] = 'PUTINT2FLT ',
    [0xe4 ] = 'PUTINT2DBL ',   [0xe5] = 'PUTLONG2FLT',
    [0xe6] = 'PUTLONG2DBL',   [0xe7 ] = 'PUTFLT2DBL ',
    [0xe8 ] = 'PUTSTR2BIN ',   [0xe9 ] = 'PUTBIN2STR ',
    [0xea ] = 'PUTENUMI2S ',   [0xeb ] = 'PUTENUMS2I ',
    [0xec     ] = 'ISBOOL     ',   [0xed      ] = 'ISINT      ',
    [0xf0     ] = 'ISLONG     ',   [0xee    ] = 'ISFLOAT    ',
    [0xef   ] = 'ISDOUBLE   ',   [0xf1      ] = 'ISSTR      ',
    [0xf2      ] = 'ISBIN      ',   [0xf3    ] = 'ISARRAY    ',
    [0xf4      ] = 'ISMAP      ',   [0xf5      ] = 'ISNUL      ',
    [0xf6 ] = 'ISNULORMAP ',   [0xf7      ] = 'LENIS      ',
    [0xf8      ] = 'ISSET      ',   [0xf9   ] = 'ISNOTSET   ',
    [0xfa   ] = 'BEGINVAR   ',   [0xfb     ] = 'ENDVAR     ',
    [0xfc  ] = 'CHECKOBUF  ',   [0xfd  ] = 'ERRVALUEV  ',
    [0xfe      ] = 'ERROR      ',
}

local function opcode_new(op)
    local o = ffi_new('struct schema_il_Opcode')
    if op then o.op = op end
    return o
end

local function opcode_ctor_ipv(op)
    return function(ipv)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.ipv = ipv
        return o
    end
end

local function opcode_ctor_ipv_ipo(op)
    return function(ipv, ipo)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.ipv = ipv; o.ipo = ipo
        return o
    end
end

local function opcode_ctor_ripv_ipv_ipo(op)
    return function(ripv, ipv, ipo)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.ripv = ripv or 0xffffffff
        o.ipv = ipv; o.ipo = ipo
        return o
    end
end

local function opcode_ctor_offset_ipv_ipo(op)
    return function(offset, ipv, ipo)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.offset = offset; o.ipv = ipv; o.ipo = ipo
        return o
    end
end

local function opcode_ctor_offset_ci(op)
    return function(offset, ci)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.offset = offset; o.ci = ci
        return o
    end
end

local il_methods = {
    declfunc = function(name, ipv)
        local o = opcode_new(0xc1)
        o.name = name; o.ipv = ipv
        return o
    end,
    ibranch = function(ci)
        local o = opcode_new(0xc2)
        o.ci = ci
        return o
    end,
    ifset       = opcode_ctor_ipv         (0xc4),
    ifnul       = opcode_ctor_ipv_ipo     (0xc5),
    intswitch   = opcode_ctor_ipv_ipo     (0xc6),
    strswitch   = opcode_ctor_ipv_ipo     (0xc7),
    objforeach  = opcode_ctor_ripv_ipv_ipo(0xc8),
    ----------------------------------------------------------------
    move        = opcode_ctor_ripv_ipv_ipo(0xc9),
    skip        = opcode_ctor_ripv_ipv_ipo(0xca),
    pskip       = opcode_ctor_ripv_ipv_ipo(0xcb),
    ----------------------------------------------------------------
    putboolc    = function(offset, cb)
        local o = opcode_new(0xcc)
        o.offset = offset; o.ci = cb and 1 or 0
        return o
    end,
    putintc     = opcode_ctor_offset_ci(0xcd),
    putintkc    = opcode_ctor_offset_ci(0xd6),
    putlongc = function(offset, cl)
        local o = opcode_new(0xce)
        o.offset = offset; o.cl = cl
        return o
    end,
    putfloatc   = function(offset, cf)
        local o = opcode_new(0xcf)
        o.offset = offset; o.cd = cf
        return o
    end,
    putdoublec  = function(offset, cd)
        local o = opcode_new(0xd0)
        o.offset = offset; o.cd = cd
        return o
    end,
    putarrayc   = opcode_ctor_offset_ci(0xd3),
    putmapc     = opcode_ctor_offset_ci(0xd4),
    putnulc = function(offset)
        local o = opcode_new(0xd8); o.offset = offset
        return o
    end,
    putdummyc = function(offset)
        local o = opcode_new(0xd7); o.offset = offset
        return o
    end,
    ----------------------------------------------------------------
    putbool     = opcode_ctor_offset_ipv_ipo(0xd9),
    putint      = opcode_ctor_offset_ipv_ipo(0xda),
    putlong     = opcode_ctor_offset_ipv_ipo(0xdb),
    putfloat    = opcode_ctor_offset_ipv_ipo(0xdc),
    putdouble   = opcode_ctor_offset_ipv_ipo(0xdd),
    putstr      = opcode_ctor_offset_ipv_ipo(0xde),
    putbin      = opcode_ctor_offset_ipv_ipo(0xdf),
    putarray    = opcode_ctor_offset_ipv_ipo(0xe0),
    putmap      = opcode_ctor_offset_ipv_ipo(0xe1),
    putint2long = opcode_ctor_offset_ipv_ipo(0xe2),
    putint2flt  = opcode_ctor_offset_ipv_ipo(0xe3),
    putint2dbl  = opcode_ctor_offset_ipv_ipo(0xe4),
    putlong2flt = opcode_ctor_offset_ipv_ipo(0xe5),
    putlong2dbl = opcode_ctor_offset_ipv_ipo(0xe6),
    putflt2dbl  = opcode_ctor_offset_ipv_ipo(0xe7),
    putstr2bin  = opcode_ctor_offset_ipv_ipo(0xe8),
    putbin2str  = opcode_ctor_offset_ipv_ipo(0xe9),
    ----------------------------------------------------------------
    isbool      = opcode_ctor_ipv_ipo(0xec),
    isint       = opcode_ctor_ipv_ipo(0xed),
    islong      = opcode_ctor_ipv_ipo(0xf0),
    isfloat     = opcode_ctor_ipv_ipo(0xee),
    isdouble    = opcode_ctor_ipv_ipo(0xef),
    isstr       = opcode_ctor_ipv_ipo(0xf1),
    isbin       = opcode_ctor_ipv_ipo(0xf2),
    isarray     = opcode_ctor_ipv_ipo(0xf3),
    ismap       = opcode_ctor_ipv_ipo(0xf4),
    isnul       = opcode_ctor_ipv_ipo(0xf5),
    isnulormap  = opcode_ctor_ipv_ipo(0xf6),
    ----------------------------------------------------------------
    lenis = function(ipv, ipo, len)
        local o = opcode_new(0xf7)
        o.ipv = ipv; o.ipo = ipo; o.len = len
        return o
    end,
    ----------------------------------------------------------------
    isnotset    = opcode_ctor_ipv(0xf9),
    beginvar    = opcode_ctor_ipv(0xfa),
    endvar      = opcode_ctor_ipv(0xfb),
    ----------------------------------------------------------------
    checkobuf = function(offset, ipv, ipo, scale)
        local o = opcode_new(0xfc)
        o.offset = offset; o.ipv = ipv or 0xffffffff
        o.ipo = ipo or 0; o.scale = scale or 1
        return o
    end,
    errvaluev  = opcode_ctor_ipv_ipo(0xfd)
    ----------------------------------------------------------------
    -- callfunc, sbranch, putstrc, putbinc, putxc and isset
    -- are instance methods
}

-- visualize register
local function rvis(reg, inc)
    if reg == 0xffffffff then
        return '_'
    else
        return format(inc and inc ~= 0 and '$%d%+d' or '$%d', reg, inc)
    end
end

-- visualize constant
local function cvis(o, extra, decode)
    if extra then
        local c = extra[o]
        if c then
            local ok, res = true, c
            if decode then
                ok, res = pcall(decode, c)
            end
            if ok then
                ok, res = pcall(json_encode, res)
            end
            if ok then
                return res
            end
        end
    end
    return format('#%p', o)
end

-- visualize opcode
local function opcode_vis(o, extra)
    local opname = op2str[o.op]
    if o.op == 0xc0 then
        return format('%s %s,\t%s,\tFUNC<%s>,\t%d', opname,
                      rvis(o.ripv), rvis(o.ipv, o.ipo), extra[o], o.k)
    elseif o.op == 0xc1 then
        return format('%s %d,\t%s', opname, o.name, rvis(o.ipv))
    elseif o.op == 0xc2 then
        return format('%s %d', opname, o.ci)
    elseif o.op == 0xc3 then
        return format('%s %s', opname, cvis(o, extra))
    elseif o.op == 0xc4 or (
           o.op >= 0xf9 and o.op <= 0xfb) then
        return format('%s %s', opname, rvis(o.ipv))
    elseif (o.op >= 0xc5 and o.op <= 0xc7) or
           (o.op >= 0xec and o.op <= 0xf6) or
           o.op == 0xfd then
        return format('%s [%s]', opname, rvis(o.ipv, o.ipo))
    elseif o.op == 0xc8 then
        return format('%s %s,\t[%s],\t%d', opname, rvis(o.ripv), rvis(o.ipv, o.ipo), o.step)
    elseif o.op == 0xc9 then
        if o.ripv == 0xffffffff then return 'NOP' end
        return format('%s %s,\t%s', opname, rvis(o.ripv), rvis(o.ipv, o.ipo))
    elseif o.op == 0xf8 then
        return format('%s %s,\t%s,\t%s', opname, rvis(o.ripv), rvis(o.ipv, o.ipo), cvis(o, extra))
    elseif o.op == 0xca or o.op == 0xcb then
        return format('%s %s,\t[%s]', opname, rvis(o.ripv), rvis(o.ipv, o.ipo))
    elseif o.op == 0xcc or o.op == 0xcd or o.op == 0xd6 or
           o.op == 0xd3 or o.op == 0xd4 then
        return format('%s [%s],\t%d', opname, rvis(0, o.offset), o.ci)
    elseif o.op == 0xce then
        return format('%s [%s],\t%s', opname, rvis(0, o.offset), o.cl)
    elseif o.op == 0xcf or o.op == 0xd0 then
        return format('%s [%s],\t%f', opname, rvis(0, o.offset), o.cd)
    elseif o.op == 0xd8 then
        return format('%s [%s]', opname, rvis(0, o.offset))
    elseif o.op == 0xd1 or o.op == 0xd2 then
        return format('%s [%s],\t%s', opname, rvis(0, o.offset), cvis(o, extra))
    elseif o.op == 0xd5 then
        return format('%s [%s],\t%s', opname, rvis(0, o.offset), cvis(o, extra, msgpack_decode))
    elseif o.op >= 0xd9 and o.op <= 0xe9 then
        return format('%s [%s],\t[%s]', opname, rvis(0, o.offset), rvis(o.ipv, o.ipo))
    elseif o.op == 0xea or o.op == 0xeb then
        return format('%s [%s],\t[%s],\t%s', opname,
                      rvis(0, o.offset), rvis(o.ipv, o.ipo), cvis(o, extra))
    elseif o.op == 0xf7 then
        return format('%s [%s],\t%d', opname, rvis(o.ipv, o.ipo), o.len)
    elseif o.op == 0xfc then
        return format('%s %s,\t[%s],\t%d', opname, rvis(0, o.offset), rvis(o.ipv, o.ipo), o.scale)
    else
        return format('<opcode: %d>', o.op)
    end
end

-- visualize IL code
local il_vis_helper
il_vis_helper = function(res, il, indentcache, level, object)
    local object_t = type(object)
    if object_t == 'table' then
        local start = 1
        if level ~= 0 or type(object[1]) ~= 'table' then
            il_vis_helper(res, il, indentcache, level, object[1])
            start = 2
            level = level + 1
        end
        for i = start, #object do
            il_vis_helper(res, il, indentcache, level, object[i])
        end
    elseif object_t ~= 'nil' then
        local indent = indentcache[level]
        if not indent then
            indent = '\n'..rep('  ', level)
            indentcache[level] = indent
        end
        insert(res, indent)
        insert(res, il.opcode_vis(object))
    end
end
local function il_vis(il, root)
    local res = {}
    il_vis_helper(res, il, {}, 0, root)
    if res[1] == '\n' then res[1] = '' end
    insert(res, '\n')
    return concat(res)
end

-- === Basic optimization engine in less than 450 LOC. ===
--
-- Elide some MOVE $reg, $reg+offset instructions.
-- Combine COBs (CHECKOBUFs) and hoist them out of loops.
--
-- We track variable state in a 'scope' data structure.
-- A scope is a dict keyed by a variable name. Scopes are chained
-- via 'parent' key. The lookup starts with the outter-most scope
-- and walks the chain until the entry is found.
--
-- The root scope is created upon entering DECLFUNC. When optimiser
-- encounters a nested block, it adds another scope on top of the chain
-- and invokes itself recursively. Once the block is done, the child
-- scope is removed.
--
-- When a variable value is updated in a program (e.g. assignment),
-- the updated entry is created in the outter-most scope, shadowing
-- any entry in parent scopes. Once a block is complete, the outermost
-- scope has all variable changes captured. These changes are merged
-- with the parent scope.
--
-- Variable state is <gen, inc, islocal, isdead> tuple. Variables are
-- scoped lexically; if a variable is marked local it is skipped when
-- merging with the parent scope. Codegen may explicitly shorten variable
-- lifetime with a ENDVAR instruction. Once a variable is dead its value
-- is no longer relevant. Valid code MUST never reference a dead variable.
--
-- Note: ENDVAR affects the code following the instruction (in a depth-first
--       tree walk order.) E.g. if the first branch in a conditional
--       declares a variable dead, it affects subsequent branches as well.
--
-- Concerning gen and inc.
--
-- Gen is a generation counter. If a variable X is updated
-- in such a way that new(X) = old(X) + C, the instruction is elided
-- and C is added to inc while gen is unchanged. Those we call 'dirty'
-- variables. If a variable is updated in a different fashion, including
-- but not limited to copying another variable's value, i.e. X = Y, we
-- declare that a new value is unrelated to the old one. Gen is assigned
-- a new unique id, and inc is set to 0.
--
-- Surprisingly, this simple framework is powerful enough to determine
-- if a loop proceeds in fixed increments (capture variable state
-- at the begining of the loop body and at the end and compare gen-s.)

local function vlookup(scope, vid)
    if not scope then
        assert(false, format('$%d not found', vid)) -- assume correct code
    end
    local res = scope[vid]
    if res then
        if res.isdead == 1 then
            assert(false, format('$%d is dead', vid)) -- assume correct code
        end
        return res
    end
    return vlookup(scope.parent, vid)
end

local function vcreate(scope, vid)
    local v = scope[vid]
    if not v then
        v = ffi_new('struct schema_il_V')
        scope[vid] = v
    end
    return v
end

-- 'execute' an instruction and update scope
local function vexecute(il, scope, o, res)
    assert(type(o)=='cdata')
    if o.op == 0xc9 and o.ripv == o.ipv then
       local vinfo = vlookup(scope, o.ipv)
       local vnewinfo = vcreate(scope, o.ipv)
       vnewinfo.gen = vinfo.gen
       vnewinfo.inc = vinfo.inc + o.ipo
       return
    end
    if o.op == 0xfa then
        local vinfo = vcreate(scope, o.ipv)
        vinfo.islocal = 1
        insert(res, o)
        return
    end
    if o.op == 0xfb then
        local vinfo = vcreate(scope, o.ipv)
        vinfo.isdead = 1
        insert(res, o)
        return
    end
    local fixipo = 0
    if (o.op == 0xc0 or
        o.op >= 0xc5 and o.op <= 0xcb or
        o.op >= 0xd9 and o.op <= 0xf8 or
        o.op == 0xfc or o.op == 0xfd) and
       o.ipv ~= 0xffffffff then

        local vinfo = vlookup(scope, o.ipv)
        fixipo = vinfo.inc
    end
    local fixoffset = 0
    if o.op >= 0xcc and o.op <= 0xeb or
       o.op == 0xfc then

        local vinfo = vlookup(scope, 0)
        fixoffset = vinfo.inc
    end
    -- spill $0
    local v0info
    if o.op == 0xc0 then
        v0info = vlookup(scope, 0)
        if v0info.inc ~= 0 then
            insert(res, il.move(0, 0, v0info.inc))
        end
    end
    -- apply fixes
    o.ipo = o.ipo + fixipo
    o.offset = o.offset + fixoffset
    insert(res, o)
    if (o.op == 0xc0 or
        o.op >= 0xc8 and o.op <= 0xcb) and
       o.ripv ~= 0xffffffff then
        local vinfo = vcreate(scope, o.ripv)
        vinfo.gen = il.id()
        vinfo.inc = 0
    end
    -- adjust $0 after func call
    if o.op == 0xc0 then
        local new_v0info = vcreate(scope, 0)
        local inc = il._wpo_info[il.get_extra(o)]
        if inc then -- this function adds a const value to $0
            inc = inc + v0info.inc
            new_v0info.gen = v0info.gen
            new_v0info.inc = inc
            insert(res, il.move(0, 0, -inc))
        else
            new_v0info.gen = il.id()
            new_v0info.inc = 0
        end
    end
end

-- Merge branches (conditional or switch);
-- if branches had diverged irt $r, spill $r
-- (e.g. one did $1 = $1 + 2 while another didn't touch $1 at all)
-- Note: bscopes[1]/bblocks[1] are unused, indices start with 2.
local function vmergebranches(il, bscopes, bblocks)
    local parent = bscopes[2].parent
    local skip = { parent = true } -- vids to skip: dead or diverged
    local maybediverged = {}
    local diverged = {}
    local counters = {} -- how many branches have it, if < #total then diverged
    local nscopes = #bscopes
    for i = 2, nscopes do
        for vid, vinfo in pairs(bscopes[i]) do
            local check_it = not skip[vid] and vinfo.islocal == 0
            if check_it and vinfo.raw ~= vlookup(parent, vid).raw then
                local vother = maybediverged[vid]
                counters[vid] = (counters[vid] or 1) + 1
                if vinfo.isdead == 1 then
                    skip[vid] = true
                    diverged[vid] = nil
                    maybediverged[vid] = nil
                    vcreate(parent, vid).isdead = 1
                elseif not vother then
                    maybediverged[vid] = vinfo
                elseif vother.raw ~= vinfo.raw then
                    skip[vid] = true
                    maybediverged[vid] = nil
                    diverged[vid] = true
                end
            end
        end
    end
    for vid, vinfo in pairs(maybediverged) do
        if counters[vid] ~= nscopes then
            diverged[vid] = true
        else
            local vpinfo = vcreate(parent, vid)
            vpinfo.gen = vinfo.gen
            vpinfo.inc = vinfo.inc
        end
    end
    for vid, _ in pairs(diverged) do
        local vinfo_parent = vlookup(parent, vid)
        for i = 2, nscopes do
            local vinfo = bscopes[i][vid] or vinfo_parent
            if vinfo.inc ~= 0 then
                local bblock = bblocks[i]
                if not bblock then -- restore missing branch
                    bblock = { il.ibranch(bblocks[2][1].ci == 0) }
                    bblocks[i] = bblock
                end
                insert(bblock, il.move(vid, vid, vinfo.inc))
            end
        end
        vinfo_parent = vcreate(parent, vid)
        vinfo_parent.gen = il.id()
        vinfo_parent.inc = 0
    end
    return diverged
end

-- Spill 'dirty' variables at the end of a loop body.
local function vmergeloop(il, lscope, lblock)
    local parent = lscope.parent
    for vid, vinfo in pairs(lscope) do
        if vinfo ~= parent and vinfo.islocal == 0 and vinfo.isdead == 0 then
            local vinfo_parent = vlookup(parent, vid)
            local delta_inc = vinfo.inc - vinfo_parent.inc
            if delta_inc ~= 0 then
                insert(lblock, il.move(vid, vid, delta_inc))
            end
        end
    end
end

-- Whether or not it is valid to move a COB across the specified
-- code range. If start is non-nil block[start] is assumed to be
-- another COB we are attempting to merge with.
local vcobmotionvalid
vcobmotionvalid = function(block, cob, start, stop)
    if cob.ipv == 0xffffffff then
        return true
    else
        local t = block[start]
        if t and t.ipv ~= 0xffffffff then
            return false -- they won't merge
        end
    end
    for i = start or 1, stop or #block do
        local o = block[i]
        if type(o) == 'table' then
            if not vcobmotionvalid(o, cob) then
                return false
            end
        elseif o.op == 0xf4 or o.op == 0xf3 then
            -- we lack alias analysis; it's unsafe to move past ANY typecheck
            return false
        elseif o.op >= 0xc8 and o.op <= 0xcb and
               o.ripv == cob.ipv then
            -- it modifies the variable
            return false
        end
    end
    return true
end

-- Merge 2 COBs, update a.
local function vcobmerge(a, b)
    if a.offset < b.offset then
        a.offset = b.offset
    end
    if b.ipv ~= 0xffffffff then
        assert(a.ipv == 0xffffffff)
        a.ipv = b.ipv
        a.ipo = b.ipo
        a.scale = b.scale
    end
end

-- Here be dragons.
local voptimizeblock
voptimizeblock = function(il, scope, block, res)
    -- COB hoisting state
    local block_0gen = vlookup(scope, 0).gen -- $0 at block start
    local first_cob_pos, first_cob_0gen -- first COB (if any), will attempt
                                    -- to pop it into the parent
    local cob_pos, cob_0gen         -- 'active' COB, when we encounter
                                    -- another COB, we attempt to merge

    for i = 2, #block do -- foreach item in the current block, excl. head
        local new_cob_pos, new_cob_0gen, new_cob_0gen_hack
        local o = block[i]
        if type(o) == 'cdata' then -- Opcode
            vexecute(il, scope, o, res)
            if o.op == 0xfc then
                new_cob_pos = #res; new_cob_0gen = vlookup(scope, 0).gen
            end
        else
            local head = o[1]
            if head.op >= 0xc4 and head.op <= 0xc7 then
                -- branchy things: conditions and switches
                local bscopes = {0}
                local bblocks = {}
                vexecute(il, scope, head, bblocks)
                local cobhoistable, cobmaxoffset = 0, 0
                for j = 2, #o do
                    local branch = o[j]
                    local bscope = { parent = scope }
                    local bblock = { branch[1] }
                    voptimizeblock(il, bscope, branch, bblock)
                    bscopes[j] = bscope
                    bblocks[j] = bblock
                    -- update hoistable COBs counter
                    local cob = bblock[0]
                    if cob and cob.ipv == 0xffffffff then
                        cobhoistable = cobhoistable + 1
                        cobmaxoffset = max(cobmaxoffset, cob.offset)
                    end
                end
                -- a condition has 2 branches, though empty ones are omitted;
                -- if it's the case temporary restore the second branch
                -- for the vmergebranches() to consider this execution path
                if #o == 2 and (head.op == 0xc4 or
                                head.op == 0xc5) then
                    bscopes[3] = { parent = scope }
                end
                -- hoist COBs but only if at least half of the branches will
                -- benefit
                if cobhoistable >= #bblocks/2 then
                    for j = 2, #bblocks do
                        local bblock = bblocks[j]
                        local pos = bblock[-1]
                        if pos and bblock[0].ipv == 0xffffffff then
                            remove(bblock, pos)
                        end
                    end
                    insert(res, il.checkobuf(cobmaxoffset))
                    new_cob_pos, new_cob_0gen = #res, vlookup(scope, 0).gen
                end
                -- finally, merge branches
                vmergebranches(il, bscopes, bblocks)
                insert(res, bblocks)
            elseif head.op == 0xc8 then
                -- loops
                local lscope = { parent = scope }
                local lblock = {}
                vexecute(il, scope, head, lblock)
                local loop_var = head.ripv
                voptimizeblock(il, lscope, o, lblock)
                if lscope[loop_var] ~= nil -- loop_var may be optimized away
                        and scope[loop_var].gen == lscope[loop_var].gen then
                    -- loop variable incremented in fixed steps
                    head.step = lscope[loop_var].inc
                    lscope[loop_var] = nil
                end
                -- hoist COB out of loop
                local v0info = vlookup(scope, 0)
                local loop_v0info = vlookup(lscope, 0)
                local new_cob = lblock[0]
                if v0info.gen == loop_v0info.gen and new_cob then
                    remove(lblock, lblock[-1])
                    local step = loop_v0info.inc - v0info.inc
                    if step == 0 then
                        insert(res, new_cob)
                    else
                        insert(res, il.checkobuf(v0info.inc, head.ipv,
                                                 head.ipo, step))
                    end
                    new_cob_pos, new_cob_0gen = #res, v0info.gen
                end
                if v0info.raw ~= loop_v0info.raw then
                    new_cob_0gen_hack = il.id() -- ex: record( array, int )
                    local new_v0info = vcreate(scope, 0)
                    new_v0info.gen = new_cob_0gen_hack
                    new_v0info.inc = v0info.inc
                end
                -- finally, merge loop
                vmergeloop(il, lscope, lblock)
                insert(res, lblock)
            else
                assert(false)
            end
        end
        -- push COB up
        if new_cob_pos then
            if not cob_pos or cob_0gen ~= new_cob_0gen or
                not vcobmotionvalid(res, res[new_cob_pos], cob_pos) then
                -- no active COB or merge imposible: activate new COB
                cob_pos = new_cob_pos
                cob_0gen = new_cob_0gen_hack or new_cob_0gen
                if not first_cob_pos then
                    first_cob_pos, first_cob_0gen = cob_pos, new_cob_0gen
                end
            else
                -- update active COB and drop the new one
                local new_cob = res[new_cob_pos]
                remove(res, new_cob_pos)
                vcobmerge(new_cob, res[cob_pos])
                res[cob_pos] = new_cob
                cob_0gen = new_cob_0gen_hack or cob_0gen
            end
        end
    end
    -- Add missing ENDVARs.
    for vid, vinfo in pairs(scope) do
        if vid ~= 'parent' and vinfo.islocal == 1 and vinfo.isdead == 0 then
            insert(res, il.endvar(vid))
        end
    end
    -- Attempt to pop the very first COB into the parent.
    if first_cob_pos and first_cob_0gen == block_0gen and
       vcobmotionvalid(res, res[first_cob_pos], nil, first_cob_pos) then
        -- There was a COB and the code motion was valid.
        -- Create a copy of the COB with adjusted offset and save it in res[0].
        -- If the parent decides to accept, it has to remove a now redundant
        -- COB at firstcobpos.
        local o = res[first_cob_pos]
        res[0] = il.checkobuf(o.offset, o.ipv, o.ipo, o.scale)
        res[-1] = first_cob_pos
    end
end

local function voptimizefunc(il, func)
    local head, scope = func[1], {}
    local res = { head }
    local r0 = vcreate(scope, 0)
    local r1 = vcreate(scope, head.ipv)
    voptimizeblock(il, scope, func, res)
    if r0.gen == 0 then
        il._wpo_info[head.name] = r0.inc
    end
    if r0.inc ~= 0 then
        insert(res, il.move(0, 0, r0.inc))
    end
    if r1.inc ~= 0 then
        insert(res, il.move(head.ipv, head.ipv, r1.inc))
    end
    return res
end

local function voptimize(il, code)
    local res = {}
    -- simple form of whole program optimization:
    -- start with leaf functions, record $0 update pattern
    il._wpo_info = {}
    for i = #code,1,-1 do
        res[i] = voptimizefunc(il, code[i])
    end
    return res
end

local function il_create()

    local extra = {}
    local id = 10 -- low ids are reserved

    local il
    il = setmetatable({
        callfunc = function(ripv, ipv, ipo, func, k)
            local o = opcode_new(0xc0)
            o.ripv = ripv or 0xffffffff; o.ipv = ipv; o.ipo = ipo;
            o.k = k or 0
            extra[o] = func
            return o
        end,
        ----------------------------------------------------------------
        sbranch = function(cs)
            local o = opcode_new(0xc3)
            extra[o] = cs
            return o
        end,
        putstrc = function(offset, cs)
            local o = opcode_new(0xd1)
            o.offset = offset; extra[o] = cs
            return o
        end,
        putbinc = function(offset, cb)
            local o = opcode_new(0xd2)
            o.offset = offset; extra[o] = cb
            return o
        end,
        putxc = function(offset, cx)
            local o = opcode_new(0xd5)
            o.offset = offset; extra[o] = cx
            return o
        end,
        putenums2i = function(offset, ipv, ipo, tab)
            local o = opcode_new(0xeb)
            o.offset = offset; o.ipv = ipv; o.ipo = ipo
            extra[o] = tab
            return o
        end,
        putenumi2s = function(offset, ipv, ipo, tab)
            local o = opcode_new(0xea)
            o.offset = offset; o.ipv = ipv; o.ipo = ipo
            extra[o] = tab
            return o
        end,
        isset = function(ripv, ipv, ipo, cs)
            local o = opcode_new(0xf8)
            o.ripv = ripv; o.ipv = ipv
            o.ipo = ipo; extra[o] = cs
            return o
        end,
        error = function(cs)
            local o = opcode_new(0xfe)
            extra[o] = cs
            return o
        end,
    ----------------------------------------------------------------
        id = function(n) local res = id; id = id + (n or 1); return res end,
        get_extra = function(o)
            return extra[o]
        end,
        vis = function(code) return il_vis(il, code) end,
        opcode_vis = function(o)
            return opcode_vis(o, extra)
        end,
        optimize = function(code) return voptimize(il, code) end,
    }, { __index = il_methods })
    return il
end

return {
    il_create = il_create
}
