# ODBC ffi connector for Tarantool

Based on unixODBC

## Examples

### Use a single connection

```lua
local odbc = require('odbc')
local yaml = require('yaml')

local env, err = odbc.create_env()
local conn, err = env:connect("DSN=odbc_test")

local result, err = conn:execute("SELECT 1 as a, 2 as b")
print(yaml.encode(result))

conn:close()
```

### Use ODBC transactions

```lua
local odbc = require('odbc')
local yaml = require('yaml')

local env, err = odbc.create_env()
local conn, err = env:connect("DSN=odbc_test")
conn:execute("CREATE TABLE t(id INT, value TEXT)")

conn:set_autocommit(false)
conn:execute("INSERT INTO t VALUES (1, 'one')")
conn:execute("INSERT INTO t VALUES (2, 'two')")
local result, err = conn:execute("SELECT * FROM t")
print(yaml.encode(result))

conn:commit()
conn:close()
```

# Building

``` bash
tarantoolctl rocks install ldoc  --server=https://tarantool.github.io/LDoc/
tarantoolctl rocks STATIC_BUILD=ON make
tarantoolctl rocks pack odbc
```

- result
``` bash
odbc-scm-1.<platform>.rock
```

# Testing

``` bash
export PG="DRIVER=psqlodbcw.so;DATABASE=<DBNAME>;UID=<USER>;PWD=<PASSWORD>;SERVER=<HOST>;PORT=<PORT>;"
export SYBASE="DRIVER=libtdsodbc.so;DATABASE=<DBNAME>;UID=<USER>;PWD=<PASSWORD>;SERVER=<HOST>;PORT=<PORT>;"

tarantoolctl rocks install luatest

.rocks/bin/luatest
```

# Building

# centos:7 docker based static build

``` bash
docker build -t odbc -f Dockerfile.staticbuild .
docker create --name odbc -it --rm odbc
docker cp odbc:/odbc/odbc-scm-1.linux-x86_64.rock .
docker rm -f odbc
```

- result is `odbc-scm-1.linux-x86_64.rock`

# macos (or linux)

``` bash
brew install unixodbc
# or sudo yum install unixODBC
tarantoolctl rocks STATIC_BUILD=ON make
tarantoolctl rocks pack odbc scm-1
```

# release from tag

``` bash
export TAG=`git tag`
tarantoolctl rocks install ldoc  --server=https://tarantool.github.io/LDoc/

tarantoolctl rocks new_version --tag $TAG
tarantoolctl rocks STATIC_BUILD=ON make odbc-$TAG-1.rockspec
tarantoolctl rocks pack odbc $TAG
```

# LIMITATIONS

- When no type marshaling case binding params have to be explicitly casted in sql expression, e.g.
  ```lua
  conn:execute([[ insert into <table> values (cast(? as json) ) ]], { json:encode({a=1, b=2}) })
  ```

# macosx testing

- objc corefoundation (unixodbc dep) does not like fork/exec
  - https://blog.phusion.nl/2017/10/13/why-ruby-app-servers-break-on-macos-high-sierra-and-what-can-be-done-about-it/
  - http://sealiesoftware.com/blog/archive/2017/6/5/Objective-C_and_fork_in_macOS_1013.html

    ```tarantool
    require('luatest.sandboxed_runner').run({'-c', 'test/09-prepare_test.lua'})
    ```

# Benchmarks

- This project
``` bash
export DSN="DRIVER=psqlodbcw.so;DATABASE=<DBNAME>;UID=<USER>;PWD=<PASSWORD>;SERVER=<HOST>;PORT=<PORT>;"
time tarantool test/bench/simple.lua
```

- Other
``` bash
export DSN="DRIVER=psqlodbcw.so;DATABASE=<DBNAME>;UID=<USER>;PWD=<PASSWORD>;SERVER=<HOST>;PORT=<PORT>;"
time MODULENAME='odbc' tarantool test/bench/simple.lua
```
