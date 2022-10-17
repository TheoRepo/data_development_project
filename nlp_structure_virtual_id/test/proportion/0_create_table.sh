#!/bin/bash
source /etc/profile
source ~/.bash_profile

sql_part="
drop table if exists nlp_dev.virtual_id_verification_20220720;
create table if not exists nlp_dev.virtual_id_verification_20220720
(
    msg String COMMENT '短文本'
)COMMENT '虚拟ID占比核验数据' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
"

beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 建表完成