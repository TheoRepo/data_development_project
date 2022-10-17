#!/bin/bash -e
# 变量`baseDirForScriptSelf`是当前脚本所在的路径
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

bash ${baseDirForScriptSelf}/0_create_table.sh nlp_dev tdl_qianyu_insur_claim_list_tmp
bash ${baseDirForScriptSelf}/0_create_table.sh nlp_dev tdl_qianyu_insur_unclaim_list_tmp
bash ${baseDirForScriptSelf}/0_create_table.sh nlp_dev tdl_qianyu_insur_list_20220829
bash ${baseDirForScriptSelf}/1_insert_table.sh
bash ${baseDirForScriptSelf}/2_insert_table.sh 90 360
