package = "luarapidxml"
version = "2.0.2-1"
source = {
   url = "git+https://github.com/tarantool/luarapidxml.git",
   tag = "2.0.2",
   branch = "master"
}
description = {
   summary = "Fast XML parsing module for Tarantool",
   homepage = "https://github.com/tarantool/luarapidxml",
   license = "BSD2",
   maintainer = "Yaroslav Dynnikov <yaroslav.dynnikov@gmail.com>"
}
dependencies = {
   "tarantool",
   "lua == 5.1"
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
