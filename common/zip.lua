local zip = require('zip')
local zlib = require('zlib')
local fio = require('fio')
local bit = require('bit')
local errno = require('errno')
local utils = require('common.utils')

local function unzip_dir(src, dst)
    local zipfile, err = zip.open(src)
    if err ~= nil then
        return nil, err
    end

    local files = {}
    for file_data in zipfile:files() do
        if file_data.filename:endswith('/') == false then
            table.insert(files, file_data.filename)
        end
    end

    for _, filename in ipairs(files) do
        local file, err = zipfile:open(filename)
        if err ~= nil then
            zipfile:close()
            return nil, err
        end

        local file_content, err = file:read('*a')
        file:close()

        if err ~= nil then
            zipfile:close()
            return nil, err
        end

        local file_dst_path = fio.pathjoin(dst, filename)
        local dirname = fio.dirname(file_dst_path)
        local _, err = fio.mktree(dirname)
        if err ~= nil and errno() ~= errno.EEXIST then
            zipfile:close()
            return nil, err
        end

        local file_dst, err = fio.open(file_dst_path, {'O_CREAT', 'O_WRONLY'}, tonumber('644', 8))
        if err ~= nil then
            zipfile:close()
            return nil, err
        end

        local _, err = file_dst:write(file_content)
        file_dst:close()
        if err ~= nil then
            zipfile:close()
            return nil, err
        end
    end
    zipfile:close()
end

local function shl(n, m)
    return n * 2 ^ m
end

local function mode_to_windowbits(mode)
    if mode == "gzip" then
        return 31
    elseif mode == "zlib" then
        return 0
    elseif mode == "raw" then
        return -15
    end
end

local function zlib_compress(data, mode)
    return zlib.deflate(6, mode_to_windowbits(mode))(data, "finish")
end

local function zlib_crc32(data)
    return zlib.crc32()(data)
end

local function number_to_lestring(number, nbytes)
    local out = {}
    for _ = 1, nbytes do
        local byte = number % 256
        table.insert(out, string.char(byte))
        number = (number - byte) / 256
    end
    return table.concat(out)
end

local LOCAL_FILE_HEADER_SIGNATURE = number_to_lestring(0x04034b50, 4)
local CENTRAL_DIRECTORY_SIGNATURE = number_to_lestring(0x02014b50, 4)
local END_OF_CENTRAL_DIR_SIGNATURE = number_to_lestring(0x06054b50, 4)

-- Bits 00-04: day
-- Bits 05-08: month
-- Bits 09-15: years from 1980
local function get_date(mtime)
    local parsed = utils.parse_unix_time(mtime * 1e9)
    local result = 0
    result = bit.bor(result, parsed.day)
    result = bit.bor(result, bit.lshift(parsed.month, 5))
    result = bit.bor(result, bit.lshift(parsed.year - 1980, 9))
    return result
end

--- Begin a new file to be stored inside the zipfile.
-- @param self handle of the zipfile being written.
-- @param filename filename of the file to be added to the zipfile.
-- @return true if succeeded, nil in case of failure.
local function zipwriter_open_new_file_in_zip(self, filename)
    if self.in_open_file then
        self:close_file_in_zip()
        return nil
    end

    local stat = self.ziphandle:stat()
    local lfh = {}
    self.local_file_header = lfh
    -- https://users.cs.jmu.edu/buchhofp/forensics/formats/pkzip.html
    -- Bits 00-04: seconds divided by 2
    -- Bits 05-10: minute
    -- Bits 11-15: hour
    lfh.last_mod_file_time = 0
    lfh.last_mod_file_date = get_date(stat.mtime)
    lfh.file_name_length = #filename
    lfh.extra_field_length = 0
    lfh.file_name = filename:gsub("\\", "/")
    lfh.external_attr = shl(493, 16)
    self.in_open_file = true
    return true
end

--- Write data to the file currently being stored in the zipfile.
local function zipwriter_write_file_in_zip(self, data)
    if not self.in_open_file then
        return nil
    end
    local lfh = self.local_file_header
    local compressed = zlib_compress(data, "raw")
    lfh.crc32 = zlib_crc32(data)
    lfh.compressed_size = #compressed
    lfh.uncompressed_size = #data
    self.data = compressed
    return true
end

--- Complete the writing of a file stored in the zipfile.
local function zipwriter_close_file_in_zip(self)
    local zh = self.ziphandle

    if not self.in_open_file then
        return nil
    end

    -- Local file header
    local lfh = self.local_file_header
    lfh.offset = zh:seek(0, 'SEEK_CUR')
    zh:write(LOCAL_FILE_HEADER_SIGNATURE)
    zh:write(number_to_lestring(20, 2)) -- version needed to extract: 2.0
    zh:write(number_to_lestring(4, 2)) -- general purpose bit flag
    zh:write(number_to_lestring(8, 2)) -- compression method: deflate
    zh:write(number_to_lestring(lfh.last_mod_file_time, 2))
    zh:write(number_to_lestring(lfh.last_mod_file_date, 2))
    zh:write(number_to_lestring(lfh.crc32, 4))
    zh:write(number_to_lestring(lfh.compressed_size, 4))
    zh:write(number_to_lestring(lfh.uncompressed_size, 4))
    zh:write(number_to_lestring(lfh.file_name_length, 2))
    zh:write(number_to_lestring(lfh.extra_field_length, 2))
    zh:write(lfh.file_name)

    -- File data
    zh:write(self.data)

    table.insert(self.files, lfh)
    self.in_open_file = false

    return true
end

local function zipwriter_add(self, file, filename)
    local _, err = self:open_new_file_in_zip(filename)
    if err ~= nil then
        return nil, err
    end

    local abspath = fio.abspath(file)
    local fin, err = fio.open(abspath, {'O_RDONLY'})
    if err ~= nil then
        return nil, err
    end

    local data, err = fin:read()
    fin:close()
    if err ~= nil then
        return nil, err
    end

    local _, err = self:write_file_in_zip(data)
    if err ~= nil then
        return nil, err
    end

    _, err = self:close_file_in_zip()
    if err ~= nil then
        return nil, err
    end
end

--- Complete the writing of the zipfile.
-- @param self handle of the zipfile being written.
-- @return true if succeeded, nil in case of failure.
local function zipwriter_close(self)
    local zh = self.ziphandle
    local central_directory_offset = zh:seek(0, 'SEEK_CUR')

    local size_of_central_directory = 0
    -- Central directory structure
    for _, lfh in ipairs(self.files) do
        zh:write(CENTRAL_DIRECTORY_SIGNATURE) -- signature
        zh:write(number_to_lestring(3, 2)) -- version made by: UNIX
        zh:write(number_to_lestring(20, 2)) -- version needed to extract: 2.0
        zh:write(number_to_lestring(0, 2)) -- general purpose bit flag
        zh:write(number_to_lestring(8, 2)) -- compression method: deflate
        zh:write(number_to_lestring(lfh.last_mod_file_time, 2))
        zh:write(number_to_lestring(lfh.last_mod_file_date, 2))
        zh:write(number_to_lestring(lfh.crc32, 4))
        zh:write(number_to_lestring(lfh.compressed_size, 4))
        zh:write(number_to_lestring(lfh.uncompressed_size, 4))
        zh:write(number_to_lestring(lfh.file_name_length, 2))
        zh:write(number_to_lestring(lfh.extra_field_length, 2))
        zh:write(number_to_lestring(0, 2)) -- file comment length
        zh:write(number_to_lestring(0, 2)) -- disk number start
        zh:write(number_to_lestring(0, 2)) -- internal file attributes
        zh:write(number_to_lestring(lfh.external_attr, 4)) -- external file attributes
        zh:write(number_to_lestring(lfh.offset, 4)) -- relative offset of local header
        zh:write(lfh.file_name)
        size_of_central_directory = size_of_central_directory + 46 + lfh.file_name_length
    end

    -- End of central directory record
    zh:write(END_OF_CENTRAL_DIR_SIGNATURE) -- signature
    zh:write(number_to_lestring(0, 2)) -- number of this disk
    zh:write(number_to_lestring(0, 2)) -- number of disk with start of central directory
    zh:write(number_to_lestring(#self.files, 2)) -- total number of entries in the central dir on this disk
    zh:write(number_to_lestring(#self.files, 2)) -- total number of entries in the central dir
    zh:write(number_to_lestring(size_of_central_directory, 4))
    zh:write(number_to_lestring(central_directory_offset, 4))
    zh:write(number_to_lestring(0, 2)) -- zip file comment length
    zh:close()

    return true
end

--- Return a zip handle open for writing.
-- @param name filename of the zipfile to be created.
-- @return a zip handle, or nil in case of error.
local function new_zipwriter(name)
    local fh, err = fio.open(fio.abspath(name), {'O_CREAT', 'O_WRONLY'}, tonumber('644', 8))
    if err ~= nil then
        return nil, err
    end

    local zw = {
        ziphandle = fh,
        in_open_file = false,
        files = {},
        add = zipwriter_add,
        close = zipwriter_close,
        open_new_file_in_zip = zipwriter_open_new_file_in_zip,
        write_file_in_zip = zipwriter_write_file_in_zip,
        close_file_in_zip = zipwriter_close_file_in_zip,
    }

    return zw
end

local function walk(path, basepath, callback, blacklist)
    blacklist = blacklist or {}
    if fio.path.is_dir(path) then
        local file_list, err = fio.listdir(path)
        if err ~= nil then
            return nil, err
        end
        for _, file in ipairs(file_list) do
            local fullpath = fio.pathjoin(path, file)
            local relpath = fio.pathjoin(basepath, file)
            local _, err = walk(fullpath, relpath, callback, blacklist)
            if err ~= nil then
                return nil, err
            end
        end
    else
        if blacklist[path] ~= true then
            local err = callback(path, basepath)
            if err ~= nil then
                return nil, err
            end
        end
    end
end

--- Compress files in a .zip archive.
--- See https://github.com/luarocks/luarocks/blob/3a356aa01db8587f7598512ca30d8a8ec8f113d1/src/luarocks/tools/zip.lua
local function zip_dir(zipfile, src)
    local zw, err = new_zipwriter(zipfile)
    if err ~= nil then
        return nil, err
    end

    local callback = function(path, relpath)
        local ok, err = zw:add(path, relpath)
        if not ok then
            return err
        end
    end

    local self_abspath = fio.abspath(zipfile)
    local blacklist = {
        [self_abspath] = true,
    }

    local _, err = walk(src, '', callback, blacklist)
    if err ~= nil then
        return nil, err
    end
    zw:close()
end

return {
    unzip = unzip_dir,
    zip = zip_dir,
}
