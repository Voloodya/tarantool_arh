package = "watchdog"
version = "1.1.1-1"
source = {
   url = "git://github.com/tarantool/watchdog.git",
   tag = "1.1.1",
   branch = "master"
}
description = {
   summary = "Simple watchdog module for Tarantool",
   homepage = "https://github.com/tarantool/watchdog",
   license = "BSD2",
   maintainer = "Yaroslav Dynnikov <yaroslav.dynnikov@gmail.com>"
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
