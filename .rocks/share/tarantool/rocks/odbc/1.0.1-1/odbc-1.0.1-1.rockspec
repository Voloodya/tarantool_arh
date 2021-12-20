package = "odbc"
version = "1.0.1-1"
source = {
   url = "https://github.com/tarantool/odbc.git",
   tag = "1.0.1",
   branch = "master"
}
description = {
   summary = "ODBC FFI connector",
   detailed = "ODBC Luajit FFI connector",
   homepage = "tarantool.io",
   license = "BSD2"
}
external_dependencies = {
   TARANTOOL = {
      header = "tarantool/module.h"
   }
}
build = {
   type = "cmake",
   variables = {
      STATIC_BUILD = "$(STATIC_BUILD)",
      TARANTOOL_DIR = "$(TARANTOOL_DIR)",
      TARANTOOL_INSTALL_BINDIR = "$(BINDIR)",
      TARANTOOL_INSTALL_LIBDIR = "$(LIBDIR)",
      TARANTOOL_INSTALL_LUADIR = "$(LUADIR)",
      version = "scm-1"
   }
}
