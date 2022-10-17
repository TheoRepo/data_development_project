#!/usr/bin/bash

list="BOSS直聘id 支付宝id 抖音火山版id 抖音id 今日头条id 京东id 快手id 连信id TT语音id 番茄小说id"
for i in $list
do
    `beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --showHeader=false --outputformat=tsv2 -e "select msg,virtual_id_type,virtual_id from nlp_dev.virtual_id_txt_ner_v5 where msg!= '【连信】TA在手机联系人中给你做了备注,正在使用' and virtual_id_type = '$i' distribute by rand() sort by rand() limit 1000">>virtualid_20220623_tmp.txt`
done
