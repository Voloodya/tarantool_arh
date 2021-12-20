package = "ldap"
version = "1.0.2-1"
source = {
   url = "git+ssh://git@gitlab.com:tarantool/enterprise/ldap.git",
   tag = "1.0.2",
   branch = "master"
}
dependencies = {
   "lua >= 5.1"
}
external_dependencies = {
   TARANTOOL = {
      header = "tarantool/module.h"
   }
}
build = {
   type = "cmake",
   variables = {
      CMAKE_BUILD_TYPE = "RelWithDebInfo",
      TARANTOOL_DIR = "$(TARANTOOL_DIR)",
      TARANTOOL_INSTALL_LIBDIR = "$(LIBDIR)",
      TARANTOOL_INSTALL_LUADIR = "$(LUADIR)"
   }
}
