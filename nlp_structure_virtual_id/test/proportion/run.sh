#!/bin/bash -e
# 变量`baseDirForScriptSelf`是当前脚本所在的路径
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh
bash ${baseDirForScriptSelf}/1_insert_data.sh nlp_dev


