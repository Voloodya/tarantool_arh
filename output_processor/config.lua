local checks = require('checks')

local errors = require('errors')
local config_error = errors.new_class('Invalid output_processor config')

local config_checks = require('common.config_checks').new(config_error)

local output_config = require('connector.config.output')

local utils = require('common.utils')

local function check_output_processor_output(conf, field, output)
    config_checks:check_luatype(field .. '.output', output, 'string')
    config_checks:assert(
        output_config.output_exists(conf, output),
        '%s.output %q does not exist into connector.output', field, output)
end

local function validate(conf)
    checks('table')

    local cfg = conf['output_processor']

    if not cfg then
        return true
    end

    config_checks:check_luatype('output_processor', cfg, 'table')

    for key, properties in pairs(cfg) do
        local field = string.format('output_processor[%q]', key)
        config_checks:check_luatype(field, properties, 'table')

        config_checks:check_luatype(field, properties, 'table')

        -- Record in queue will be expired after "expiration_timeout" seconds.
        -- Even there were no attempts to send it.
        local expiration_timeout = properties['expiration_timeout']
        if expiration_timeout ~= nil then
            config_checks:check_luatype(field .. '.expiration_timeout', expiration_timeout, 'number')
            config_checks:assert(expiration_timeout > 0,
                field .. '.expiration_timeout expected to be greater than 0')
        end

        -- Option to enable/disable synchronous mode
        config_checks:check_optional_luatype(field .. '.is_async', properties['is_async'], 'boolean')

        -- Timeout for synchronous mode to retry processing
        local sync_retry_timeout = properties['sync_retry_timeout']
        if sync_retry_timeout ~= nil then
            config_checks:assert(properties['is_async'] == false,
                field .. '.sync_retry_timeout makes sense only with is_async=false')
            config_checks:check_luatype(field .. '.sync_retry_timeout', sync_retry_timeout, 'number')
            config_checks:assert(sync_retry_timeout > 0, field .. '.sync_retry_timeout expected to be greater than 0')
        end

        local sync_failed_attempt_count_threshold = properties['sync_failed_attempt_count_threshold']
        if sync_failed_attempt_count_threshold ~= nil then
            config_checks:assert(properties['is_async'] == false,
                field .. '.sync_failed_attempt_count_threshold makes sense only with is_async=false')
            config_checks:check_luatype(field .. '.sync_failed_attempt_count_threshold',
                sync_failed_attempt_count_threshold, 'number')
            config_checks:assert(sync_failed_attempt_count_threshold > 0,
                field .. '.sync_failed_attempt_count_threshold expected to be greater than 0')
        end

        -- How to store data in output processing queue:
        --   * copy - store full tuple. Consume a lot of memory but will be replicated even tuple is deleted
        --       or expired.
        --   * reference - store only primary key. Object won't replicated if for some reasons
        --       it was removed from storage.
        local storage_queue_mode = properties['store_strategy']
        if storage_queue_mode ~= nil then
            config_checks:check_luatype(field .. '.store_strategy', storage_queue_mode, 'string')
            local allowed_values = {['copy'] = true, ['reference'] = true}
            assert(allowed_values[storage_queue_mode] ~= nil,
                field .. '.store_strategy expected to be "copy" or "reference"')
        end

        local handlers = properties['handlers']
        config_checks:check_luatype(field .. '.handlers', handlers, 'table')
        config_checks:assert(utils.is_array(handlers), field .. '.handlers expected to be an array')
        config_checks:assert(#handlers > 0, field .. '.handlers expected to have at least one element')

        config_checks:check_table_keys(field, properties,
                {
                    'store_strategy', 'handlers', 'expiration_timeout', -- General options
                    'is_async', 'sync_retry_timeout', 'sync_failed_attempt_count_threshold', -- Sync mode options
                })

        for i, handler in ipairs(handlers) do
            local handler_name = string.format('%s.handlers[%d]', field, i)

            config_checks:check_luatype(handler_name, handler, 'table')
            config_checks:check_luatype(handler_name .. '.function', handler['function'], 'string')
            local outputs = handler['outputs']
            config_checks:check_luatype(handler_name .. '.outputs', outputs, 'table')
            config_checks:assert(utils.is_array(outputs), handler_name .. '.outputs expected to be an array')
            config_checks:assert(#outputs > 0,
                handler_name .. '.outputs expected to have at least one element')

            for _, output in ipairs(outputs) do
                check_output_processor_output(conf, handler_name, output)
            end

            config_checks:check_table_keys(handler_name, handler, {'function', 'outputs'})
        end
    end

    return true
end

return {
    validate = function(cfg)
        return config_error:pcall(validate, cfg)
    end
}
