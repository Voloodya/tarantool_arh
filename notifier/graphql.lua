local graphql = require('common.graphql')
local types = require('graphql.types')
local tenant = require('common.tenant')

local function get_users(_, _)
    local notifier = tenant.get_cfg('notifier') or {}
    return notifier.users or {}
end

local function set_user(_, args)
    local notifier, err = tenant.get_cfg_deepcopy('notifier')
    if err ~= nil then
        return nil, err
    end

    if notifier == nil then
        notifier = {}
    end

    if notifier.users == nil then
        notifier.users = {}
    end

    local user = nil
    for _, u in ipairs(notifier.users) do
        if u.id == args.id then
            user = u
            break
        end
    end

    if user == nil then
        user = {}
        table.insert(notifier.users, user)
    end

    user.id = args.id
    user.name = args.name
    user.addr = args.addr

    local _, err = tenant.patch_config({notifier = notifier})
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function delete_user(_, args)
    local notifier, err = tenant.get_cfg_deepcopy('notifier')
    if err ~= nil then
        return nil, err
    end

    for i, user in ipairs(notifier.users) do
        if user.id == args.id then
            table.remove(notifier.users, i)
            local _, err = tenant.patch_config({notifier = notifier})
            if err ~= nil then
                return nil, err
            end
            return 'ok'
        end
    end

    return 'ok'
end

local function get_mail_server(_, _)
    local notifier = tenant.get_cfg('notifier') or {}
    return {notifier.mail_server}
end

local function set_mail_server(_, args)
    local notifier, err = tenant.get_cfg_deepcopy('notifier')
    if err ~= nil then
        return nil, err
    end

    if notifier == nil then
        notifier = {}
    end

    notifier.mail_server = {
        url = args.url,
        from = args.from,
        username = args.username,
        password = args.password,
        timeout = args.timeout,
        skip_verify_host = args.skip_verify_host,
    }

    local _, err = tenant.patch_config({notifier = notifier})
    if err ~= nil then
        return nil, err
    end

    return 'ok'
end

local function init()
    types.object {
        name = 'User_list',
        description = 'A list of users',
        fields = {
            id = types.string.nonNull,
            name = types.string.nonNull,
            addr = types.string.nonNull
        },
        schema = 'admin',
    }

    graphql.add_callback(
        {schema='admin',
         name='notifier_get_users',
         callback='notifier.graphql.get_users',
         kind=types.list('User_list')})

    graphql.add_mutation(
        {schema='admin',
         name='notifier_upsert_user',
         callback='notifier.graphql.set_user',
         kind=types.string.nonNull,
         args={
             id = types.string.nonNull,
             name = types.string.nonNull,
             addr = types.string.nonNull
    }})

    graphql.add_mutation(
        {schema='admin',
         name='notifier_delete_user',
         callback='notifier.graphql.delete_user',
         kind=types.string.nonNull,
         args={
             id = types.string.nonNull
    }})

    types.object {
        name = 'Mail_server',
        description = 'A settings of mail server',
        fields = {
            url = types.string.nonNull,
            from = types.string.nonNull,
            username = types.string.nonNull,
            password = types.string.nonNull,
            timeout = types.int,
            skip_verify_host = types.boolean,
        },
        schema = 'admin',
    }

    graphql.add_callback(
        {schema='admin',
         name='get_mail_server',
         callback='notifier.graphql.get_mail_server',
         kind=types.list('Mail_server')})

    graphql.add_mutation(
        {schema='admin',
         name='set_mail_server',
         callback='notifier.graphql.set_mail_server',
         kind=types.string.nonNull,
         args={
             url = types.string.nonNull,
             from = types.string.nonNull,
             username = types.string.nonNull,
             password = types.string.nonNull,
             timeout = types.int.nonNull,
             skip_verify_host = types.boolean,
    }})
end

return {
    get_users = get_users,
    set_user = set_user,
    delete_user = delete_user,
    get_mail_server = get_mail_server,
    set_mail_server = set_mail_server,
    init = init
}
