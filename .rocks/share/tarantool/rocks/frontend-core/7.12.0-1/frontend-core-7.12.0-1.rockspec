package = "frontend-core"
version = "7.12.0-1"
source = {
   url = "git+https://github.com/tarantool/frontend-core.git",
   tag = "7.12.0",
}
dependencies = {
   "lua >= 5.1"
}
build = {
   type = "make",
   install = {
      lua = {
         ["frontend-core"] = "frontend-core.lua"
      }
   },
   install_variables = {
      INST_LUADIR = "$(LUADIR)"
   }
}
