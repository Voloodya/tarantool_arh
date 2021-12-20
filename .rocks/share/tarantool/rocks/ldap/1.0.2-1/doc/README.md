# LDAP client library for tarantool

This library allows you to authenticate in a LDAP server and perform searches.


## Usage example

First, download [glauth](https://github.com/glauth/glauth), a simple
Go-based LDAP server using the following command:

```bash
./download_glauth.sh
```

Then run `glauth`:

```bash
./glauth -c glauth_test.cfg
```

Then run the following tarantool script in a separate terminal

```lua
#!/usr/bin/env tarantool

local ldap = require('ldap')
local yaml = require('yaml')

local user = "cn=johndoe,ou=superheros,dc=glauth,dc=com"
local password = "dogood"

local ld = assert(ldap.open("localhost:3893", user, password))

local iter = assert(ldap.search(ld,
    {base="dc=glauth,dc=com",
     scope="subtree",
     sizelimit=10,
     filter="(objectclass=*)"}))

for entry in iter do
    print(yaml.encode(entry))
end
```

## Usage ldap for authorization in the web interface

See [this](https://www.tarantool.io/en/enterprise_doc/dev/#implementing-ldap-authorization-in-the-web-interface) doc page.
