# 保险数据开发流程化


## 项目运行命令
```bash
nohup sh run.sh >data_development.log 2>&1 &
```

## 项目依赖的数据域

|  表名   | 明细  |
|  ----  | ----  |
| dwb.insurance_txt_apply  | 投保 (只有投保有保险起止日期) |
| dwb.insurance_txt_claim  | 理赔 (无保险起止日期，有理赔出险日期)  |
| dwb.insurance_txt_n4pay  | 缴费 (无保险起止日期) |
| dwb.insurance_txt_expire  | 过期（保险合同正常到期且未续保,有过期日期，无保险起止日期） |
| dwb.insurance_txt_surrender	  | 退保 (有退保日期，无保险起止日期) |
| nlp_dev.insurance_txt_apply_white_list | 投保（带有msg字段和保险机构名称）|

## 项目结果表(nlp_dev.tdl_qianyu_insur_list_20220829)

|     | row_key | mobile_id  | start_time | end_time | claim_status | insur_institute |
|  ----  | ---- | ---- | ---- | ---- | ---- | ---- |
| 字段说明  | 唯一编码 | 手机号映射id | 投保事件时间 | 理赔事件时间 | 理赔状态 | 保险机构名称 |
| 数据样例  | 8f7e99398d02d32360d692923df9d1f084727507tluedhhn | 1000503109 | 2019-10-14 | 2020-12-20 | 1 | 友邦保险有限公司上海分公司 |
| 计算逻辑  | 取自nlp_dev.insurance_txt_apply_white_list表row_key字段 | 取自nlp_dev.insurance_txt_apply_white_list表mobile_id字段 | 取自nlp_dev.insurance_txt_apply_white_list表the_date字段 | 取自dwb.insurance_txt_claim表the_date字段 | 1：从投保后90天开始计算，在未来360天内，发生过期、退保、理赔（包括临界点360天）<br>0：至少拥有一年的表现期;从投保后，在未来360天内，没有发生过，过期、退保、理赔<br>0：从投保后90天开始计算，在未来360天后，发生过期、退保、理赔（不包括临界点360天） | 取自nlp_dev.insurance_txt_apply_white_list表insur_institute字段 |


## 脚本运行成功判断指标
0_create_table.sh运行成功返回的日志！！！
```text
ls: cannot access /usr/local/spark-2.4.3-bin-hadoop2.7/lib/spark-assembly-*.jar: No such file or directory
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
scan complete in 1ms
Connecting to jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
22/08/29 19:18:15 [main]: INFO jdbc.HiveConnection: Connected to manager02-fcy.hadoop.dztech.com:10000
Connected to: Apache Hive (version 1.1.0-cdh5.15.2)
Driver: Hive JDBC (version 1.1.0-cdh5.15.2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
INFO  : Compiling command(queryId=hive_20220829191818_b3f04dcd-b523-4521-b065-96719055e42a): drop table if exists nlp_dev.tdl_qianyu_insur_list_20220829
INFO  : Semantic Analysis Completed
INFO  : Returning Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hive_20220829191818_b3f04dcd-b523-4521-b065-96719055e42a); Time taken: 0.349 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20220829191818_b3f04dcd-b523-4521-b065-96719055e42a): drop table if exists nlp_dev.tdl_qianyu_insur_list_20220829
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20220829191818_b3f04dcd-b523-4521-b065-96719055e42a); Time taken: 20.075 seconds
INFO  : OK
No rows affected (20.436 seconds)
INFO  : Compiling command(queryId=hive_20220829191818_e9dc65c8-d9a1-4eb8-a736-1b2f66880f09): create table nlp_dev.tdl_qianyu_insur_list_20220829 
(
    mobile_id String COMMENT '手机号',
    start_time String COMMENT '投保日期',
    end_time String COMMENT '理赔日期',
    claim_status String COMMENT '理赔状态'
)COMMENT '目标数据抽样' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orc
INFO  : Semantic Analysis Completed
INFO  : Returning Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hive_20220829191818_e9dc65c8-d9a1-4eb8-a736-1b2f66880f09); Time taken: 0.121 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20220829191818_e9dc65c8-d9a1-4eb8-a736-1b2f66880f09): create table nlp_dev.tdl_qianyu_insur_list_20220829 
(
    mobile_id String COMMENT '手机号',
    start_time String COMMENT '投保日期',
    end_time String COMMENT '理赔日期',
    claim_status String COMMENT '理赔状态'
)COMMENT '目标数据抽样' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orc
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20220829191818_e9dc65c8-d9a1-4eb8-a736-1b2f66880f09); Time taken: 5.34 seconds
INFO  : OK
No rows affected (5.466 seconds)
Beeline version 1.1.0-cdh5.15.2 by Apache Hive
Closing: 0: jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
建表完成
```


1_insert_table.sh运行成功返回的日志！！！
```text
2022-08-29 19:29:54 | [INFO] | spark-sql execute success,sql:

insert overwrite table nlp_dev.tdl_qianyu_insur_claim_list_tmp 
SELECT
    a.mobile_id,
    a.the_date as start_time,
    b.t as end_time,
    CASE
        WHEN  b.mobile_id IS NOT NULL THEN '1'
        WHEN  b.mobile_id IS NULL THEN '0'
    END AS claim_status
FROM
    (-- 从投保表中全表筛选出重疾险种名单，保留时间分区字段用于判断两年的存续条件
        SELECT
            mobile_id,
            the_date
        FROM
            nlp_dev.insurance_txt_apply_white_list
        WHERE
            -- the_date REGEXP '2019|2020|2021'
            -- 从数据源nlp_dev.insurance_txt_apply_white_list取有两年表现期的原始数据
            the_date<=date_sub('2021-12-05',720)
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
    -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            MIN(the_date) AS t       --同一个人可能有多个理赔记录，取最近的分区时间作为理赔时间
        FROM
            dwb.insurance_txt_claim
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
        GROUP BY
            mobile_id
    ) b ON a.mobile_id = b.mobile_id
    ;


insert overwrite table nlp_dev.tdl_qianyu_insur_unclaim_list_tmp
SELECT
    a.mobile_id,
    a.the_date AS start_time,
    b.t AS end_time,
    CASE
        WHEN b.mobile_id IS NOT NULL THEN '1'
        WHEN b.mobile_id IS NULL THEN '0'
    END AS claim_status
FROM
    (
        -- 从投保表中全表筛选出重疾险种名单，保留时间分区字段用于判断两年的存续条件
        SELECT
            mobile_id,
            the_date
        FROM
            nlp_dev.insurance_txt_apply_white_list
        WHERE
            -- the_date REGEXP '2019|2020|2021'
            -- 从数据源nlp_dev.insurance_txt_apply_white_list取有两年表现期的原始数据
            the_date<=date_sub('2021-12-05',720)
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
        -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            MIN(the_date) t
        FROM
            (
                SELECT
                    mobile_id,
                    the_date --同一个人可能有多个过期记录，取最近的分区时间作为过期时间
                FROM
                    dwb.insurance_txt_expire
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    the_date --同一个人可能有多个退保记录，取最近的分区时间作为退保时间
                FROM
                    dwb.insurance_txt_surrender
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    the_date --同一个人可能有多个理赔记录，取最近的分区时间作为理赔时间
                FROM
                    dwb.insurance_txt_claim
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
            ) tmp
        GROUP BY
            mobile_id
    ) b ON a.mobile_id = b.mobile_id
    ;


insert overwrite table nlp_dev.tdl_qianyu_insur_list_20220829
SELECT * from
    (
    select 
        a.mobile_id,
        a.start_time,
        a.end_time,
        a.claim_status
    from 
        nlp_dev.tdl_qianyu_insur_claim_list_tmp a
    where a.claim_status = '1'
    UNION ALL
    select 
        b.mobile_id,
        b.start_time,
        b.end_time,
        b.claim_status
    from 
        nlp_dev.tdl_qianyu_insur_unclaim_list_tmp b
    where b.claim_status = '0'
    ) tab
    ;

数据写入完成
```