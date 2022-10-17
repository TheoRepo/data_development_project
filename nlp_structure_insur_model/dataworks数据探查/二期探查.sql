--*************************************************************************************************************--
保险建模二期数据探查：

一期数据探查结果统计：
（1）统计投保重疾险种的总事件数222736744，总人数116237449（证明1人多次存在）
（2）统计投保重疾险种且合同满2年的总事件数47858，总人数43603（证明1人多次存在）
PS：总人数显然较少(effective_date和end_date空值率较高)，需要以保单生效日期为观察点，由于缴费、退保、到期表中无effective_date，故无法补充从其它表数据
（3）统计投保重疾险种且发生理赔的总事件数346231，总人数323259
PS：报案时间和事件时间一致，出险时间在事件时间，且相近；且从探查过程而知，出险时间空值率较高
（4）统计投保重疾险种且发生理赔（且理赔时间不为空值）的总事件数329，总人数155
PS：只有155人理赔时间能够确认,即有99.952%不能确定出险时间（出险时间字段deal_date空值率较高所致）
（5）统计投保重疾险种且合同期（无两年内的时间限制）内发生理赔的总人数106，未发生理赔总人数43497

需求变更：
（由于一期合同满两年的限定条件下，无法提取到目标数据，以下是修改后的数据域的限定条件）
i.   我们关注的是客户的进入保险开始后2年内的观察情况，这个进入保险的时间是可以通过投保时间时间来反推的
ii.  同时关注的不是保险合同的截止时间，而是保险的状态是否持续2年，从投保开始2年内没有被核保拒绝、退保、到期等行为，即可认为满足存续条件（也可从正面的缴费记录等来推断）
iii. 出险时间并不需要非常的严格，事件时间可以代表理赔发生时间
iiii.过期、退保时间等均采用对应的事件时间替代
--*************************************************************************************************************--
注：
以下数据探查：先探查定义为‘1’的目标人数，其次再探查定义为‘0’的目标数据；
对目标数据探查时：分别从四个粒度探查目标数据的数据量情况，即mobile_id粒度、app_name粒度、product_name粒度、insur_contract_no粒度


# 查询目标数据域总人数
1.-- 统计成功投保重疾险种的总人数：
select count(mobile_id),count(distinct mobile_id) from dwb.insurance_txt_apply where the_date regexp '2019|2020|2021' and product_name REGEXP '重疾|重大疾病';
+------------+------------+--+
|    _c0     |    _c1     |
+------------+------------+--+
| 222736744  | 116237449  |
+------------+------------+--+

# 对定义为‘1’的数据探查如下：
# 先从较粗的粒度统计：即mobile_id粒度
2.-- 两年内发生理赔的人员名单
drop table nlp_dev.tdl_af_insur_list_tmp_v2;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_v2 AS
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
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
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

-- 查询两年存续时间内发生理赔的名单
select * from nlp_dev.tdl_af_insur_list_tmp_v2 where end_time <= date_add(start_time,720) and claim_status='1';

-- 统计两年存续时间内发生理赔的人员去重数量
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_v2 where end_time <= date_add(start_time,720) and claim_status='1';
**********
279638  103839
**********

分析：
1. 279638-103839=175,799，即存在一个人买有多种重疾险，且存在理赔行为。
--******************************************************************************************************************************
（以下附加部分的探索结论：175799条重复不一定是一个人买有较多重疾险种，大概率是一个人同一个保险产品的投保行为记录在多个时间分区）
--******************************************************************************************************************************

2. 103839条样本是以默认一个人在2019-07-01至2021-06-20期间（720天）发生的理赔产品，就是在2019-07-01至2021-12-05期间的投保产品。
（
    理论上：
        1.存在一个人买几份重疾险的情况；
        2.存在某个人在2019-07-01至2021-06-20期间（720天）发生的理赔行为对应的理赔产品是在2019-07-01之前投的重疾险种；
        3.投保表和理赔表中的合同编号字段空值率较高，且同一个人同一个产品合同对应的产品名称也不一致，无法更好地定位同一个人同一个产品的投保和理赔行为
）以上三条总结均为通过下面的探查所知。

注：（以下附加部分的探索结论：175799条重复不一定是一个人买有较多重疾险种，大概率是一个人同一个保险产品的投保行为记录在多个时间分区）


--********************************************************************************************************************************
"""
附加：探究一个人买有多种重疾险具体细节
-- 所有发生理赔的名单
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_v2 where claim_status='1';
**********
280225  103950
**********

分析：103950-103839=111，即有111人在两年后发生的理赔

-- 找出这111人发生理赔的时间加以确认
select  * from nlp_dev.tdl_af_insur_list_tmp_v2 where end_time >= date_add(start_time,720) and claim_status='1' order by mobile_id,start_time;
select mobile_id,count(mobile_id) from nlp_dev.tdl_af_insur_list_tmp_v2 where end_time >= date_add(start_time,720) and claim_status='1' group by mobile_id order by count(mobile_id) desc;
-- 找出重复次数较多的人（两年后发生的理赔的人中）
mobile_id	count(mobile_id)
872123292   7
707021580   7
72290473    6

-- 部分结果样例：
mobile_id	 start_time	  end_time	 claim_status
872123292   2019-08-23  2021-08-18  1
872123292   2019-08-23  2021-08-18  1
872123292   2019-08-24  2021-08-18  1
872123292   2019-08-25  2021-08-18  1
872123292   2019-08-25  2021-08-18  1
872123292   2019-08-22  2021-08-18  1
872123292   2019-08-24  2021-08-18  1
522153588   2019-07-22  2021-09-18  1
53811898    2019-08-31  2021-12-03  1
53811898    2019-11-27  2021-12-03  1
分析：以上可知他们最近一次发生理赔的时间的确在投保时间的两年后。

"""
以下：分别从投保表和理赔表分析872123292人投保和理赔情况（主要探查：872123292该人员有那么多投保记录的具体细节）
投保表：
select * from dwb.insurance_txt_apply where the_date REGEXP '2019-08-22|2019-08-23|2019-08-24|2019-08-25' and mobile_id ='872123292'
 +---------------------------------------------------+------------+----------------------+-----------+---------------------+-----------------+--------------+-------------------+-----------+-----------------+-------------------+--------------------+---------------+-------------------+--------------------+----------------------+----------------------+----------------+----------+-----------------+-----------+---------+-------+-------------+-------+--------------------+--------------------+-------------+-------------------------+--+
|                      row_key                      | mobile_id  |      event_time      | app_name  | suspected_app_name  |  main_call_no   | class_label  | class_label_prob  |  insurer  | applicant_name  | applicant_gender  | applicant_id_card  | insured_name  |   product_name    | insur_contract_no  | insur_contract_no_h  | insur_contract_no_t  | policy_period  | premium  | effective_date  | end_date  | car_no  |  vin  | apply_date  |  url  | insurcatelevelone  | insurcateleveltwo  |  the_date   |         file_no         |
+---------------------------------------------------+------------+----------------------+-----------+---------------------+-----------------+--------------+-------------------+-----------+-----------------+-------------------+--------------------+---------------+-------------------+--------------------+----------------------+----------------------+----------------+----------+-----------------+-----------+---------+-------+-------------+-------+--------------------+--------------------+-------------+-------------------------+--+
| 5f157eb33131e9359d95b554e90b64892da3047fpvdwjaab  | 872123292  | 2019-08-24 09:30:55  | 复星联合健康保险  | 未识别                 | 10657120637777  | 保险_投保        | 1.0               | 复星联合健康保险  | 毛立飞             | 女                 | NULL               | NULL          | 复星联合妈咪保贝少儿重大疾病保险  | W87190000061720    | NULL                 | NULL                 | NULL           | NULL     | NULL            | NULL      | NULL    | NULL  | NULL        | NULL  | 健康险    | 重疾险    | 2019-08-24  | merge_20190824_7195_L0  |
| cf014f5a2e268b6baf0903af59a2e2ad378a1f7bzonmrcwj  | 872123292  | 2019-08-22 18:16:50  | 复星联合健康保险  | 未识别                 | 10657120637777  | 保险_投保        | 1.0               | 复星联合健康保险  | 毛立飞             | 女                 | NULL               | NULL          | 复星联合少儿重大疾病保险          | W87190000061720    | NULL                 | NULL                 | NULL           | 1351.90  | 2019-08-23      | NULL      | NULL    | NULL  | NULL        | NULL  | 健康险    | 重疾险    | 2019-08-22  | merge_20190822_2076_L0  |
| 5543d7cda9730df1e67c70650ab72b119e16f661quudxvis  | 872123292  | 2019-08-22 18:33:26  | 复星联合健康      | 复星联合健康           | 10657120637777  | 保险_投保        | 0.9767            | 复星联合健康保险  | NULL               | NULL               | NULL               | NULL          | NULL                              | NULL               | NULL                 | NULL                 | NULL           | NULL     | NULL            | NULL      | NULL    | NULL  | NULL        | NULL  | 其他      | 其他      | 2019-08-22  | merge_20190822_2076_L0  |
| 3ed3b68f0262dd4ba48b830da03faab4cbbc54fetpfeunww  | 872123292  | 2019-08-23 09:31:15  | 复星联合健康保险  | 未识别                 | 10657120637777  | 保险_投保        | 1.0               | 复星联合健康保险  | 毛立飞             | 女                 | NULL               | NULL          | 复星联合妈咪保贝少儿重大疾病保险  | W87190000061727    | NULL                 | NULL                 | NULL           | NULL     | NULL            | NULL      | NULL    | NULL  | NULL        | NULL  | 健康险    | 重疾险    | 2019-08-23  | merge_20190823_4213_L0  |
| 741f3987c1c22a219deebc771a2d554c08ef4d4fygmhtwus  | 872123292  | 2019-08-25 09:30:44  | 复星联合健康      | 复星联合健康           | 10657120637777  | 保险_投保        | 1.0               | 复星联合健康保险  | 毛立飞             | 女                 | NULL               | NULL          | 复星联合妈咪保贝少儿重大疾病保险  | W87190000061727    | NULL                 | NULL                 | NULL           | NULL     | NULL            | NULL      | NULL    | NULL  | NULL        | NULL  | 健康险    | 重疾险    | 2019-08-25  | merge_20190825_6492_L0  |
| 3939ccfdb536f2e193441b2ebba5438f3c43f851bryctumd  | 872123292  | 2019-08-25 09:30:43  | 复星联合健康保险  | 未识别                 | 10657120637777  | 保险_投保        | 1.0               | 复星联合健康保险  | 毛立飞             | 女                 | NULL               | NULL          | 复星联合妈咪保贝少儿重大疾病保险  | W87190000061720    | NULL                 | NULL                 | NULL           | NULL     | NULL            | NULL      | NULL    | NULL  | NULL        | NULL  | 健康险    | 重疾险    | 2019-08-25  | merge_20190825_6492_L0  |
| 4eea8c7d797985c9aa2a0a754099698d0d74cb6fcxdcyyan  | 872123292  | 2019-08-23 09:31:11  | 复星联合健康      | 复星联合健康           | 10657120637777  | 保险_投保        | 1.0               | 复星联合健康保险  | 毛立飞             | 女                 | NULL               | NULL          | 复星联合妈咪保贝少儿重大疾病保险  | W87190000061720    | NULL                 | NULL                 | NULL           | NULL     | NULL            | NULL      | NULL    | NULL  | NULL        | NULL  | 健康险    | 重疾险    | 2019-08-23  | merge_20190823_4213_L0  |
| 82bee9d96e36c9eff2cdd3c28eb84e0d5e9eeaf4gyhnavbn  | 872123292  | 2019-08-24 09:30:56  | 复星联合健康      | 复星联合健康           | 10657120637777  | 保险_投保        | 1.0               | 复星联合健康保险  | 毛立飞             | 女                 | NULL               | NULL          | 复星联合妈咪保贝少儿重大疾病保险  | W87190000061727    | NULL                 | NULL                 | NULL           | NULL     | NULL            | NULL      | NULL    | NULL  | NULL        | NULL  | 健康险    | 重疾险    | 2019-08-24  | merge_20190824_7195_L0  |
+---------------------------------------------------+------------+----------------------+-----------+---------------------+-----------------+--------------+-------------------+-----------+-----------------+-------------------+--------------------+---------------+-------------------+--------------------+----------------------+----------------------+----------------+----------+-----------------+-----------+---------+-------+-------------+-------+--------------------+--------------------+-------------+-------------------------+--+
分析：以上得知：
1.同一个人同一天可能有多个保险产品（以合同号区分）投保记录
2.同一个人的同一个保险产品（以合同号区分）可能出现在相互邻近的多个时间分区内

--**************************************************************************************************************
结论：175799条重复不一定是一个人买有较多重疾险种，大概率是一个人同一个保险产品的投保行为记录在多个时间分区
--**************************************************************************************************************

理赔表：
select * from dwb.insurance_txt_claim where the_date REGEXP '2021-08' and mobile_id ='872123292'
+---------------------------------------------------+------------+----------------------+-----------+---------------------+------------------+--------------+-------------------+----------+----------------+------------------+---------------+---------------+--------------------+----------+------------+------------+------------+---------+------------+--------------+------------+-------------+---------------+-----------+-------+--------------------+--------------------+-------------+-------------------------+--+
|                      row_key                      | mobile_id  |      event_time      | app_name  | suspected_app_name  |   main_call_no   | class_label  | class_label_prob  | insurer  | reporter_name  | reporter_gender  | insured_name  | product_name  | insur_contract_no  | case_no  | case_no_h  | case_no_t  | claim_amt  | car_no  | car_model  | report_date  | deal_date  | agent_name  | agent_mobile  | loss_amt  |  url  | insurcatelevelone  | insurcateleveltwo  |  the_date   |         file_no         |
+---------------------------------------------------+------------+----------------------+-----------+---------------------+------------------+--------------+-------------------+----------+----------------+------------------+---------------+---------------+--------------------+----------+------------+------------+------------+---------+------------+--------------+------------+-------------+---------------+-----------+-------+--------------------+--------------------+-------------+-------------------------+--+
| d5773aeb640276255939d1133d081832b60e00fdsctlrlua  | 872123292  | 2021-08-18 17:13:37  | 梧桐树    | 梧桐树              | 106930973054039  | 保险_定损理赔      | 0.9466            | NULL     | NULL           | NULL             | NULL          | 贝少儿重大疾病保险     | NULL               | NULL     | NULL       | NULL       | NULL       | NULL    | NULL       | NULL         | NULL       | NULL        | NULL          | NULL      | NULL  | 健康险                | 重疾险                | 2021-08-18  | merge_20210818_1111_L0  |
+---------------------------------------------------+------------+----------------------+-----------+---------------------+------------------+--------------+-------------------+----------+----------------+------------------+---------------+---------------+--------------------+----------+------------+------------+------------+---------+------------+--------------+------------+-------------+---------------+-----------+-------+--------------------+--------------------+-------------+-------------------------+--+
分析：理赔表和投保表中的product_name可能表示同一个产品，但是字面值不一致，同时观察到insurance_txt_claim.product_name有可能是insurance_txt_apply.product_name的字面值截断

select product_name,insur_contract_no from dwb.insurance_txt_claim where the_date REGEXP '2021-08-01' and product_name REGEXP '重疾|重大疾病' limit 500;
product_name	         insur_contract_no
重大疾病(疾病类)理       #NULL
产品众安重疾险           #NULL
重大疾病个人综合保险     #NULL
产品众安重疾险           #NULL

分析：
要想分析product_name粒度的理赔人群情况，只能使用product_name(insurance_txt_claim.product_name有可能是insurance_txt_apply.product_name的字面值截断)；insur_contract_no空值较多；insurcateleveltwo只是保险类型分类，粒度比product_name粗

-- 为了更好地定位同一个人同一个产品，进行...
以下探查：测试投保表和理赔表的product_name和insur_contract_no的特点
CREATE TABLE nlp_dev.tdl_afeng_insur_product AS
SELECT
    *
FROM
    (
        SELECT
            a.mobile_id,
            a.product_name AS pro_1,
            a.insur_contract_no as insur_contract_no_1,
            b.product_name AS pro_2,
            b.insur_contract_no as insur_contract_no_2,
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
                    product_name,
                    insur_contract_no,
                    the_date
                FROM
                    dwb.insurance_txt_apply
                WHERE
                    the_date REGEXP '2019'
                    AND product_name REGEXP '重疾|重大疾病'
            ) a
            LEFT JOIN (
                -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
                SELECT
                    mobile_id,
                    product_name,
                    insur_contract_no,
                    MIN(the_date) AS t --同一个人可能有多个理赔记录，取最近的分区时间作为理赔时间
                FROM
                    dwb.insurance_txt_claim
                WHERE
                    the_date REGEXP '2020'
                    AND product_name REGEXP '重疾|重大疾病'
                GROUP BY
                    mobile_id,
                    product_name,
                    insur_contract_no
            ) b ON a.mobile_id = b.mobile_id
    ) a
WHERE
    claim_status = '1'


select * from nlp_dev.tdl_afeng_insur_product where insur_contract_no_2 = insur_contract_no_1
result:
+------------+---------------+------------------------+---------+------------------------+-------------+-------------+---------------+--+
| mobile_id  |     pro_1     |  insur_contract_no_1   |  pro_2  |  insur_contract_no_2   | start_time  |  end_time   | claim_status  |
+------------+---------------+------------------------+---------+------------------------+-------------+-------------+---------------+--+
| 289487657  | 上海医保账户重大疾病保险  | PC0200A221805986       | 重大疾病保险  | PC0200A221805986       | 2019-08-26  | 2020-07-20  | 1
| 496794114  | 上海医保账户重大疾病保险  | PC0200A231637455       | 重大疾病保险  | PC0200A231637455       | 2019-11-22  | 2020-10-16  | 1
| 102943796  | 上海医保账户重大疾病保险  | PC0200A227951891       | 重大疾病保险  | PC0200A227951891       | 2019-10-11  | 2020-09-05  | 1
| 306443647  | 上海医保账户重大疾病保险  | PC0200A228975296       | 重大疾病保险  | PC0200A228975296       | 2019-10-21  | 2020-09-13  | 1
| 368590783  | 上海医保账户重大疾病保险  | PC0200A224330823       | 重大疾病保险  | PC0200A224330823       | 2019-09-06  | 2020-07-26  | 1
| 517693247  | 重大疾病保险              | 828042019220197000008  | 重大疾病保险  | 828042019220197000008  | 2019-12-04  | 2020-01-15  | 1
| 31657564   | 重大疾病保险              | 828042019220197000008  | 重大疾病保险  | 828042019220197000008  | 2019-12-27  | 2020-01-09  | 1
| 8794848    | 重疾安心保基础版          | 828042019520221000005  | 重大疾病保险  | 828042019520221000005  | 2019-07-05  | 2020-07-07  | 1
| 8794848    | 70种重大疾病保险基础版    | 828042019520221000005  | 重大疾病保险  | 828042019520221000005  | 2019-07-05  | 2020-07-07  | 1
| 554864998  | 上海医保账户重大疾病保险  | PC0200A224054313       | 重大疾病保险  | PC0200A224054313       | 2019-09-05  | 2020-07-30  | 1
+------------+---------------+------------------------+---------+------------------------+-------------+-------------+---------------+--+
select count(distinct mobile_id) from nlp_dev.tdl_afeng_insur_product
*******
7395
*******
分析：
以上是测试2019年投保（重疾险种），并在2020年理赔的人数结果：
1. 仅仅10条数据满足同一个人同一个保险合同，原因：合同编号字段空值率较高
2. 7395-10=7385条中有很多不确定是否为同一个人同一个保险产品的理赔，原因：合同编号字段空值率较高
3. 同一个人同一个合同编号在投保表和理赔表中的product_name字面值描述有差异。

--********************************************************************************************************************************************

# product_name粒度
-- 创建包含产品字段的临时表
drop table nlp_dev.tdl_af_insur_list_tmp_product_v2;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_product_v2 AS
SELECT
    a.mobile_id,
    a.product_name,
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
            product_name,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
    -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            product_name,
            MIN(the_date) AS t       --同一个人可能有多个理赔记录，取最近的分区时间作为理赔时间
        FROM
            dwb.insurance_txt_claim
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
        GROUP BY
            mobile_id,product_name
    ) b ON a.mobile_id = b.mobile_id AND a.product_name = b.product_name

-- 统计product_name粒度下投保两年内发生理赔的人员去重数
select claim_status,count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_product_v2 where end_time <= date_add(start_time,720) group by claim_status;
claim_status	    count(DISTINCT mobile_id)
1                   1775
结论：由于投保表和理赔表中同一个人同一个产品合同对应的产品名称不一致，故统计出来的数据较少

# insur_contract_no粒度
-- 以合同编号粒度统计两年内发生理赔的人群去重数
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_contract_no_v2 AS
SELECT
    a.mobile_id,
    a.insur_contract_no AS insur_contract_no_1,
    b.insur_contract_no AS insur_contract_no_2,
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
            insur_contract_no,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
        -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            insur_contract_no,
            MIN(the_date) AS t --无论同一个人同一款保险产品是否可能有多个理赔记录，取最近的分区时间作为理赔时间
        FROM
            dwb.insurance_txt_claim
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
        GROUP BY
            mobile_id,
            insur_contract_no
    ) b ON a.mobile_id = b.mobile_id
    AND a.insur_contract_no = b.insur_contract_no

-- 统计insur_contract_no粒度下投保两年内发生理赔的人员去重数
select claim_status,count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_contract_no_v2 where end_time <= date_add(start_time,720) group by claim_status;
claim_status	    count(DISTINCT mobile_id)
1                   174
结论：由于投保表和理赔表中的合同编号字段空值率较高，故统计出来的数据较少


# app_name粒度
-- 以平台粒度统计两年内发生理赔的人群去重数
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_app_name AS
SELECT
    a.mobile_id,
    a.app_name AS app_name_1,
    b.app_name AS app_name_2,
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
            CASE
                WHEN app_name = '未识别' THEN suspected_app_name
                ELSE app_name
            END AS app_name,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
            AND suspected_app_name != '未识别'
    ) a
    LEFT JOIN (
        -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            app_name,
            MIN(the_date) AS t --无论同一个人同一款保险产品是否可能有多个理赔记录，取最近的分区时间作为理赔时间
        FROM
            dwb.insurance_txt_claim
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
            AND app_name != '未识别'
        GROUP BY
            mobile_id,
            app_name
    ) b ON a.mobile_id = b.mobile_id
    AND a.app_name = b.app_name

-- 统计app_name粒度下投保两年内发生理赔的人员去重数
select claim_status,count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_app_name where end_time <= date_add(start_time,720) group by claim_status;
claim_status	    count(DISTINCT mobile_id)
1                   10787
*********************************************************************************************************************************************************************

综上所述：
不同粒度统计下两年内发生理赔的 去重人员 数量，即定义为1的数量：
1. mobile_id粒度：103839人
2. app_name粒度：10787人
2. product_name粒度：1775人
3. insur_contract_no粒度：174人


-------------------------------------------------------------------------------------------------------------------------------------------------------------------

# 对定义为‘0’的数据探查如下：
结合过期、退保、理赔表进行反向探查：即，只要没在过期、退保、理赔表中出现的投保人员（重疾险种），就默认为 '0'的分类中。
1. mobile_id粒度
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 AS
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
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
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

****************************************************************************
对于没有两年表现期的数据踢掉，即2019-07-01至2019-12-16期间投保且未发生过过期、退保、理赔的去重人数：
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 where start_time>='2019-07-01' and start_time<=date_sub('2021-12-05',720) and claim_status='0';
15299485    10011355

-- 2019-07-01至2019-12-16期间投保的人员中，在投保两年后发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 where start_time>='2019-07-01' and start_time<=date_sub('2021-12-05',720) and end_time >= date_add(start_time,720);
127993  93259

-- 确认在投保两年后发生过期、退保、理赔的人员中确实都是在2019-07-01至2019-12-16期间投保的
select * from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 where start_time>='2019-07-01' and start_time<=date_sub('2021-12-05',720) and end_time >= date_add(start_time,720);


剔除没有两年表现期的数据后，定义为0的数据：
select 93259+10011355=10,104,614

****************************************************************************


-- 统计投保总人数（重疾险种）
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2;
222736744       116237449

#（目标0的数据）#
-- 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 where claim_status='0';
176997422   99491539

分析： 99491539/116237449=85.6%,反向探查:只要没在过期、退保、理赔表中出现的投保人员（重疾险种），就默认为 '0'的分类中。

-- 统计时间内（2.5年，即无两年的时间限定）发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 where claim_status='1';
45739322    16745910
-- 统计两年内发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 where end_time <= date_add(start_time,720);
45613642    16710739

#（目标0的数据）#
-- 统计两年后发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_mobile_id_v2 where end_time >= date_add(start_time,720);
127993      93259

分析： 16710739+93259-16745910=58088，由于end_time都是取最近的记录，start_time（投保时间）存在同一个人在不同的时间分区内有多个投保记录（可能是同一产品，可能是不同的产品），即这58088人在不同的时间分区内有多个投保记录
-- 统计非1非0的人数
非1非0的去重人数≈两年内发生过期、退保、理赔的去重人数-两年内发生理赔的去重人数   （近似等于的原因：此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多）
select 16710739-103839=                 16606900
select 116237449-103839-99491539-93259= 16548812 != 16606900  >>> 存在一个人购买多个重疾产品，可能两年内发生过期或退保的那个产品是2019-07-01之前投的保，此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多


******************************************************
2. app_name粒度
drop table nlp_dev.tdl_af_insur_list_tmp_app_name_v2;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_app_name_v2 AS
SELECT
    a.mobile_id,
    a.app_name AS app_name_1,
    b.app_name AS app_name_2,
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
            CASE
                WHEN app_name = '未识别' THEN suspected_app_name
                ELSE app_name
            END AS app_name,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
        -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            app_name,
            MIN(the_date) t
        FROM
            (
                SELECT
                    mobile_id,
                    app_name,
                    the_date --同一个人可能有多个过期记录，取最近的分区时间作为过期时间
                FROM
                    dwb.insurance_txt_expire
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    the_date --同一个人可能有多个退保记录，取最近的分区时间作为退保时间
                FROM
                    dwb.insurance_txt_surrender
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    the_date --同一个人可能有多个理赔记录，取最近的分区时间作为理赔时间
                FROM
                    dwb.insurance_txt_claim
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
            ) tmp --where app_name != '未识别'
        GROUP BY
            mobile_id,
            app_name
    ) b ON a.mobile_id = b.mobile_id
    AND a.app_name = b.app_name



#（目标0的数据）#
-- 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_app_name_v2 where claim_status='0';
197705519   108166797
分析：select 108166797/116237449=93.06%,反向探查:只要没在过期、退保、理赔表中出现的投保人员（重疾险种），就默认为 '0'的分类中。

-- 统计时间内（2.5年，即无两年的时间限定）发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_app_name_v2 where claim_status='1';
25031225    12021124
-- 统计两年内发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_app_name_v2 where end_time <= date_add(start_time,720);
25005091    12010705

#（目标0的数据）#
-- 统计两年后发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_app_name_v2 where end_time >= date_add(start_time,720);
26573   19679
分析：select 12010705+19679-12021124=9260,由于end_time都是取最近的记录，start_time（投保时间）存在同一个人在不同的时间分区内有多个投保记录（可能是同一产品，可能是不同的产品），即这9260人在不同的时间分区内有多个投保记录

-- 统计非1非0的人数
非1非0的去重人数≈两年内发生过期、退保、理赔的去重人数-两年内发生理赔的去重人数   （近似等于的原因：此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多）
select 12021124-10787=                 12010337
select 116237449-10787-108166797-19679=8040186 != 12010337  >>> 存在一个人购买多个重疾产品，可能两年内发生过期或退保的那个产品是2019-07-01之前投的保，（此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多）
******************************************************

3. product_name粒度
drop table nlp_dev.tdl_af_insur_list_tmp_product_name_v2;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_product_name_v2 AS
SELECT
    a.mobile_id,
    a.product_name AS product_name_1,
    b.product_name AS product_name_2,
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
            product_name,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
        -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            product_name,
            MIN(the_date) t
        FROM
            (
                SELECT
                    mobile_id,
                    product_name,
                    the_date --同一个人可能有多个过期记录，取最近的分区时间作为过期时间
                FROM
                    dwb.insurance_txt_expire
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    product_name,
                    the_date --同一个人可能有多个退保记录，取最近的分区时间作为退保时间
                FROM
                    dwb.insurance_txt_surrender
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    product_name,
                    the_date --同一个人可能有多个理赔记录，取最近的分区时间作为理赔时间
                FROM
                    dwb.insurance_txt_claim
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
            ) tmp
        GROUP BY
            mobile_id,
            product_name
    ) b ON a.mobile_id = b.mobile_id
    AND a.product_name = b.product_name


#（目标0的数据）#
-- 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_product_name_v2 where claim_status='0';
199337308   108545205

分析：108545205/116237449=93.4%,反向探查:只要没在过期、退保、理赔表中出现的投保人员（重疾险种），就默认为 '0'的分类中。

-- 统计时间内（2.5年，即无两年的时间限定）发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_product_name_v2 where claim_status='1';
23399436    11891246
-- 统计两年内发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_product_name_v2 where end_time <= date_add(start_time,720);
23386080    11884799

#（目标0的数据）#
-- 统计两年后发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_product_name_v2 where end_time >= date_add(start_time,720);
13422   10413

-- 统计非1非0的人数
非1非0的去重人数≈两年内发生过期、退保、理赔的去重人数-两年内发生理赔的去重人数   （近似等于的原因：此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多）
select 11884799-1775=                  11883024
select 116237449-1775-108545205-10413= 7680056 != 11883024  >>> 存在一个人购买多个重疾产品，可能两年内发生过期或退保的那个产品是2019-07-01之前投的保，此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多
******************************************************

4. insur_contract_no粒度
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_insur_contract_no_v2 AS
SELECT
    a.mobile_id,
    a.insur_contract_no AS insur_contract_no_1,
    b.insur_contract_no AS insur_contract_no_2,
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
            insur_contract_no,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
        -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            insur_contract_no,
            MIN(the_date) t
        FROM
            (
                SELECT
                    mobile_id,
                    insur_contract_no,
                    the_date --同一个人可能有多个过期记录，取最近的分区时间作为过期时间
                FROM
                    dwb.insurance_txt_expire
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    insur_contract_no,
                    the_date --同一个人可能有多个退保记录，取最近的分区时间作为退保时间
                FROM
                    dwb.insurance_txt_surrender
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
                UNION ALL
                SELECT
                    mobile_id,
                    insur_contract_no,
                    the_date --同一个人可能有多个理赔记录，取最近的分区时间作为理赔时间
                FROM
                    dwb.insurance_txt_claim
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND product_name REGEXP '重疾|重大疾病'
            ) tmp
        GROUP BY
            mobile_id,
            insur_contract_no
    ) b ON a.mobile_id = b.mobile_id
    AND a.insur_contract_no = b.insur_contract_no




#（目标0的数据）#
-- 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_insur_contract_no_v2 where claim_status='0';
222110611   116139954

分析： 116139954/116237449=99.916%,反向探查:只要没在过期、退保、理赔表中出现的投保人员（重疾险种），就默认为 '0'的分类中。

-- 统计时间内（2.5年，即无两年的时间限定）发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_insur_contract_no_v2 where claim_status='1';
626133  352363
-- 统计两年内发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_insur_contract_no_v2 where end_time <= date_add(start_time,720);
623891  351824

#（目标0的数据）#
-- 统计两年后发生过期、退保、理赔的去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_insur_contract_no_v2 where end_time >= date_add(start_time,720);
2286    1331

分析：351824+1331-352363=792,由于end_time都是取最近的记录，start_time（投保时间）存在同一个人在不同的时间分区内有多个投保记录（可能是同一产品，可能是不同的产品），即这792人在不同的时间分区内有多个投保记录

-- 统计非1非0的人数
非1非0的去重人数≈两年内发生过期、退保、理赔的去重人数-两年内发生理赔的去重人数   （近似等于的原因：此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多）
select 351824-174=                   351650
select 116237449-174-116139954-1331= 95990 != 351650  >>> 存在一个人购买多个重疾产品，可能两年内发生过期或退保的那个产品是2019-07-01之前投的保，此部分由于产品名称不一致和合同编号空值较多的缘故，无法区分，默认都是2019-07-01之后投的保，故可能导致这个部分的数据偏多



综上所述：
2019-07-01至2021-12-05期间重疾险种投保总人数：116237449人，不同粒度下，定义为0的人数统计如下：
1. mobile_id粒度
## 定义为1的人数统计：
 投保人数中，两年内发生理赔的去重人数： select  103839/116237449=0.0893%

## 定义为0的人数统计：
 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数：99491539/116237449=85.6%
统计两年后发生过期、退保、理赔的去重人数：93259
定义为0的去重人数占总投保人数的比值：select (99491539+93259)/116237449=85.7%

--***--
思考：为什么0.0893%+85.7%<100%?    (100%-(0.0893%+85.7%)=14.2%,这14.2%包含哪些情况？)
答：投保的两年内发生过期、退保，未发生理赔的人群既不属于1，也不属于0

注：其它粒度的情形类似。
--***--
2. app_name粒度
## 定义为1的人数统计：
 投保人数中，两年内发生理赔的去重人数： select  10787/116237449=0.00928%

## 定义为0的人数统计：
 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数：  89698842/116237449=77.169%
统计两年后发生过期、退保、理赔的去重人数：12494
定义为0的去重人数占总投保人数的比值： select (89698842+12494)/116237449=77.179%

3. product_name粒度
## 定义为1的人数统计：
 投保人数中，两年内发生理赔的去重人数：  1775/116237449=0.0015%

## 定义为0的人数统计：
 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数：  108545205/116237449=93.38%
统计两年后发生过期、退保、理赔的去重人数：10413
定义为0的去重人数占总投保人数的比值：select (108545205+10413)/116237449=93.39%

4. insur_contract_no粒度
## 定义为1的人数统计：
 投保人数中，两年内发生理赔的去重人数：  174/116237449=0.00015%

## 定义为0的人数统计：
 投保人数中，统计时间内（2.5年，即无两年的时间限定）未发生过期、退保、理赔的去重人数 116139954/116237449=99.916%
统计两年后发生过期、退保、理赔的去重人数：1331
定义为0的去重人数占总投保人数的比值：select (116139954+1331)/116237449=99.917%

至此：
保险建模总体统计已完成，整理后excel结果详见：《朴道大箴保险样本抽取需求20220808.xlsx》文件中结果部分；
以下从保险缴费情况的正面推测，仅仅是观察一下正面推测的情况
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
以下从缴费记录的正面推测：
弊端：由于保险产品缴费一般按缴纳，为了确认客户的存续状态，需要去最大的缴费时间作为end_time，这样的话对这种场景不准确（若某个客户有两个产品的，投保时间一远一近，若远的那个产品缴费时间和近的产品投保时间满足两年的存续条件，则被计算在内，实际上可能不满足）
-- 统计全表（无两年的时间限制条件）中缴费去重人数：
select count(mobile_id),count(distinct mobile_id) from dwb.insurance_txt_n4pay where the_date regexp '2019|2020|2021' and product_name REGEXP '重疾|重大疾病';
183635050       41673686

-- 统计投保人数中，缴费人数占比：
DROP TABLE nlp_dev.tdl_af_insur_list_tmp_n4pay_v2;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_n4pay_v2 AS
SELECT
    a.mobile_id,
    a.the_date as start_time,
    b.t as end_time,
    CASE
        WHEN  b.mobile_id IS NOT NULL THEN '1'
        WHEN  b.mobile_id IS NULL THEN '0'
    END AS status
FROM
    (-- 从投保表中全表筛选出重疾险种名单，保留时间分区字段用于判断两年的存续条件
        SELECT
            mobile_id,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
    ) a
    LEFT JOIN (
    -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断两年的存续条件
        SELECT
            mobile_id,
            MAX(the_date) AS t       --同一个人可能有多个缴费记录，取最远的分区时间作为缴费时间（因为保险产品一般都是按月缴费）
        FROM
            dwb.insurance_txt_n4pay
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
        GROUP BY
            mobile_id
    ) b ON a.mobile_id = b.mobile_id

-- 统计投保记录中的缴费去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_n4pay_v2 where status='1';
47346174    16871491

-- 统计投保记录中两年外发生的缴费去重人数
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_n4pay_v2 where status='1' and end_time >= date_add(start_time,720) ;
2695990     1474726

分析： 1474726/16871491=8.74%



