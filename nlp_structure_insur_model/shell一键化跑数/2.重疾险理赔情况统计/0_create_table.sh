#!/bin/bash
source /etc/profile
source ~/.bash_profile

new_database_name=$1
new_table_name=$2

sql_part="
drop table if exists ${new_database_name}.${new_table_name};
create table ${new_database_name}.${new_table_name} 
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT '手机号映射id',
    start_time String COMMENT '投保日期yyyy-MM-dd格式',
    end_time String COMMENT '理赔日期yyyy-MM-dd格式',
    claim_status String COMMENT '理赔状态',
    insur_institute String COMMENT '机构名称'
)COMMENT '目标数据抽样' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc; 
"

# beeline的命令行
# -u参数，连接hive
# -e参数，sql代码
beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "$sql_part"

# shell脚本的if判断逻辑
# 如果代码运行失败，输出："sql 运行失败！！！！！！"
if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 建表完成