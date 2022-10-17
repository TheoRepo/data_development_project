#!/bin/bash

source /etc/profile
source ~/.bash_profile

rss_path=$1

sql_part="
insert overwrite table nlp_dev.virtual_id_verification_20220720
select msg from preprocess.ds_txt_final_sample where 
(app_name regexp '支付宝|抖音火山版|今日头条|BOSS直聘|抖音|番茄小说|京东|快手|连信|TT语音'
or 
suspected_app_name regexp '支付宝|抖音火山版|今日头条|BOSS直聘|抖音|番茄小说|京东|快手|连信|TT语音')
distribute by rand() sort by rand() limit 10000;
"

cd /home/${rss_path}/ && bash rss.sh "data_explorer" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 数据写入完成