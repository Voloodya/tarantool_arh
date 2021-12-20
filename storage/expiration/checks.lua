local fio = require('fio')
local uuid = require('uuid')

local function is_dir_writable(path)
    if not path or path == '' then
        return {
            is_writeable = false,
            message = 'Path is empty',
        }
    end

    path = fio.abspath(path)
    if fio.path.exists(path) then
        if not fio.path.is_dir(path) then
            return {
                is_writeable = false,
                message = path .. ' is not a directory',
            }
        end

        local test_file_path = fio.pathjoin(path, uuid.str())
        local fh, err = fio.open(test_file_path, 'O_CREAT')
        if not fh then
            return {
                is_writeable = false,
                message = err,
            }
        end
        fh:close()
        os.remove(test_file_path)
        return { is_writeable = true }
    else
        local parent_path = fio.dirname(path)
        if parent_path == path then
            return {
                is_writeable = false,
                message = 'Directory ' .. path ..' does not exists',
            }
        end
        return is_dir_writable(parent_path)
    end
end

return {
    is_dir_writable = is_dir_writable,
}
