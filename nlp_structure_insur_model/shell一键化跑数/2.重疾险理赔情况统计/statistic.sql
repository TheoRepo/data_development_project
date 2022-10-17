-- 老方式：表现期为720天，投保事件日期为观察点
-- 统计
-- 1. 成功投保重疾险种
-- 2. 投保两年内发生理赔
-- 的总人数：
select count(mobile_id),count(distinct mobile_id) 
from nlp_dev.tdl_qianyu_insur_list_20220829 
where claim_status='1';


-- 统计
-- 1. 成功投保重疾险种
-- 2. 投保180天内发生理赔
-- 的总人数：
select count(mobile_id),count(distinct mobile_id) 
from nlp_dev.tdl_qianyu_insur_list_20220829 
where end_time <= date_add(start_time,180)
and start_time < end_time 
and claim_status='1';


-- 统计
-- 1. 成功投保重疾险种
-- 2. 投保90天内发生理赔
-- 的总人数：
select count(mobile_id),count(distinct mobile_id) 
from nlp_dev.tdl_qianyu_insur_list_20220829 
where end_time <= date_add(start_time,90) 
and start_time < end_time 
and claim_status='1';


-- 数据分布
-- 机构
select count(distinct mobile_id), insur_institute
from 
(
    select * 
    from nlp_dev.tdl_qianyu_insur_list_20220829 
    where claim_status='1'
) a
group by insur_institute
;

-- 导出txt格式的数据
beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --showHeader=false --outputformat=tsv2 -e "select count(distinct mobile_id), insur_institute from (select * from nlp_dev.tdl_qianyu_insur_list_20220829 where claim_status='1') a group by insur_institute">>老方式_机构分布.txt


-- 时间
select count(distinct mobile_id), substr(start_time,1,7)
from 
(
    select * 
    from nlp_dev.tdl_qianyu_insur_list_20220829 
    where claim_status='1'
) a
group by substr(start_time,1,7)
;

-- 导出txt格式的数据
beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --showHeader=false --outputformat=tsv2 -e "select count(distinct mobile_id), substr(start_time,1,7) from (select * from nlp_dev.tdl_qianyu_insur_list_20220829 where claim_status='1') a group by substr(start_time,1,7)">>老方式_时间分布.txt


-- 新方式：表现期为360，投保后90天为观察点
-- 统计
-- 1. 成功投保重疾险种
-- 2. 投保后90天为观察点，表现期为360天内发生理赔
-- 的总人数：
select count(mobile_id),count(distinct mobile_id) 
from nlp_dev.tdl_qianyu_insur_list_20220829 
-- 逻辑判断：行为在观察点之后
where end_time > DATE_ADD(start_time, 90) 
-- 业务逻辑：360天表现期
and end_time <= date_add(date_add(start_time,90),360) 
and claim_status='1';


-- 统计
-- 1. 成功投保重疾险种
-- 2. 投保后90天为观察点，表现期为180天内发生理赔
-- 的总人数：
select count(mobile_id),count(distinct mobile_id) 
from nlp_dev.tdl_qianyu_insur_list_20220829 
-- 逻辑判断：行为在观察点之后
where end_time > DATE_ADD(start_time, 90) 
-- 业务逻辑：180天表现期
and end_time <= date_add(date_add(start_time,90),180) 
and claim_status='1';


-- 统计
-- 1. 成功投保重疾险种
-- 2. 投保后90天为观察点，表现期为90天内发生理赔
-- 的总人数：
select count(mobile_id),count(distinct mobile_id) 
from nlp_dev.tdl_qianyu_insur_list_20220829 
-- 逻辑判断：行为在观察点之后
where end_time > DATE_ADD(start_time, 90) 
-- 业务逻辑：90天表现期
and end_time <= date_add(date_add(start_time,90),90) 
and claim_status='1';


-- 数据分布
-- 机构
select count(distinct mobile_id), insur_institute
from 
(
    select * 
    from nlp_dev.tdl_qianyu_insur_list_20220829 
    where end_time > DATE_ADD(start_time, 90)
    and end_time <= date_add(date_add(start_time,90),360) 
    and claim_status='1'
) a
group by insur_institute
;

-- 导出txt格式的数据
beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --showHeader=false --outputformat=tsv2 -e "select count(distinct mobile_id), insur_institute from (select * from nlp_dev.tdl_qianyu_insur_list_20220829 where end_time > DATE_ADD(start_time, 90) and end_time <= date_add(date_add(start_time,90),360) and claim_status='1') a group by insur_institute">>新方式_机构分布.txt


-- 时间
select count(distinct mobile_id), substr(start_time,1,7)
from 
(
    select * 
    from nlp_dev.tdl_qianyu_insur_list_20220829 
    where end_time > DATE_ADD(start_time, 90)
    and end_time <= date_add(date_add(start_time,90),360) 
    and claim_status='1'
) a
group by substr(start_time,1,7)
;

-- 导出txt格式的数据
beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --showHeader=false --outputformat=tsv2 -e "select count(distinct mobile_id), substr(start_time,1,7) from (select * from nlp_dev.tdl_qianyu_insur_list_20220829 where end_time > DATE_ADD(start_time, 90) and end_time <= date_add(date_add(start_time,90),360) and claim_status='1') a group by substr(start_time,1,7)">>新方式_时间分布.txt


-- 原则：结果表有两年的观察期（表现期），通过修改筛选条件，就统计一年观察期，180天，90天，30天观察期的数据
-- 长的，涵盖，短的


-- 统计
-- 1. 成功投保重疾险种
-- 2. 在两年的表现期内，没有理赔行为，没有退保行为，没有保险过期记录
-- 3. 在两年的表现期外，发生理赔行为，或者有退保行为，或者有保险过期记录
-- 的总人数：
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_qianyu_insur_list_20220829 where claim_status='0';
