local function is_success_status_code(code)
    return 200 <= code and code <= 205
end

return {
    is_success_status_code = is_success_status_code,
}
