#!/bin/bash

TDG_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

awk '{sub(/dev_mode = true/,"dev_mode = false")}1' ${TDG_DIR}/env.lua > temp.lua &&
mv temp.lua ${TDG_DIR}/env.lua &&
echo 'Switched to production mode'
