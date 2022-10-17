--*************************************************************************************************************--
保险建模一期数据探查：

需求简介：
1、需求背景
      同金融信贷场景的建模样本一样，在保险场景也有的类似的建模样本需求
2、背景补充
（1）保险概况流程（具体子类型产品可能还需要继续深化）
    被营销->投保->核保->缴费->理赔
（2）名词解释
    i.   观察点：一般为投保时间或者核保时间，前后有一点偏差也能够接受
    ii.  观察期：观察点之前一段固定时间，一般用于限定X特征的情况，此项目暂时不用考虑
    iii. 表现期：观察点之后的一段固定时间，用于界定好坏样本（还有灰样本），正常来说只有看完了表现期全部的情况后，才能对样本进行界定
3、需求详情
（1）目标样本：重疾险种在互联网渠道投保时未来两年内出险以及未出险的样本。
（2）目标Y定义：以保单生效日期为观察点，表现期定为2年，发生核保理赔定义1，未发生理赔的定义为0，剔除保险合同未满2年的样本
（3）交付情况：待探索后确认
--*************************************************************************************************************--
-- 保险建模一期数据探查过程如下：
1.保险合同的起止日期字段探查：
经探查知：只有投保表中有保险合同起止日期，且空值率较高
涉及的数据域：
dwb.insurance_txt_apply       --投保 (只有投保有保险起止日期)
dwb.insurance_txt_claim       --理赔 (无保险起止日期，有理赔出险日期)
dwb.insurance_txt_n4pay       --缴费 (无保险起止日期)
dwb.insurance_txt_surrender   --退保 (有退保日期，无保险起止日期)
dwb.insurance_txt_expire      --过期（保险合同正常到期且未续保,有过期日期，无保险起止日期）

2. 查询重疾险种及其识别
SELECT
    distinct
    product_name,
    insurcateleveltwo
FROM
    dwb.insurance_txt_apply
WHERE
    the_date = '2021-12-01'
    AND effective_date IS NOT NULL
    AND end_date IS NOT NULL
LIMIT
    1000;

result:(展示5个样例：)
product_name	         insurcateleveltwo
百万安心疗(重疾升级版)尊享版  重疾险
重疾安心保(2021版)方案四    重疾险
百万安心疗(重疾升级版)优选版  重疾险
个人重大疾病保险            重疾险

总结：可使用 product_name REGEXP '重疾|重大疾病' 对重疾险种加以限定

3. 统计成功投保重疾险种的总人数：（数据总集）
select count(mobile_id),count(distinct mobile_id) from dwb.insurance_txt_apply where the_date regexp '2019|2020|2021' and product_name REGEXP '重疾|重大疾病';
+------------+------------+--+
|    _c0     |    _c1     |
+------------+------------+--+
| 222736744  | 116237449  |
+------------+------------+--+

4. 统计成功投保重疾险种且合同满2年的总人数：
SELECT
    COUNT(mobile_id),
    COUNT(DISTINCT mobile_id)
FROM
    dwb.insurance_txt_apply
WHERE
    the_date REGEXP '2019|2020|2021'
    AND product_name REGEXP '重疾|重大疾病'
    AND datediff(end_date, effective_date) > 720;

result:
----------
47858
43603
-----------
结果分析：
1.47858>43603,故存在一个人买两份及以上的重疾险种，以去重人数为主（只要有一个重疾险种两年内出险，就能定义为1）；
2.总人数显然较少(effective_date和end_date空值率较高)，且缴费、退保、到期表中无effective_date，故无法从其它表补充数据


5. 统计成功投保重疾险种且发生理赔的总人数：
select count(mobile_id),count(distinct mobile_id) from dwb.insurance_txt_claim where the_date regexp '2019|2020|2021' and product_name REGEXP '重疾|重大疾病' ;
result:
-------------------------
count(mobile_id)    count(DISTINCT mobile_id)
346231              323259
---------------------------


6. 探查出险时间、报案时间和事件时间（发信时间）的联系
select mobile_id,event_time,report_date,deal_date from dwb.insurance_txt_claim where  the_date regexp '2019' and deal_date is not null and report_date is not null and product_name REGEXP '重疾|重大疾病';
+-------------+----------------------+--------------+-------------+--+
|  mobile_id  |      event_time      | report_date  |  deal_date  |
+-------------+----------------------+--------------+-------------+--+
| 340471712   | 2019-08-08 08:59:55  | 2019-08-08   | 2019-07-29  |
| 227312823   | 2019-08-12 16:18:17  | 2019-08-12   | 2019-08-01  |
| 227312823   | 2019-08-03 09:52:45  | 2019-08-03   | 2019-07-25  |
| 227312823   | 2019-08-03 18:59:15  | 2019-08-03   | 2019-07-31  |
| 227312823   | 2019-08-27 11:51:14  | 2019-08-27   | 2019-08-27  |
| 227312823   | 2019-08-28 10:25:35  | 2019-08-28   | 2019-06-05  |
+-------------+----------------------+--------------+-------------+--+
结论：报案时间和事件时间一致，出险时间先于事件时间，且相近；且从探查过程而知，出险时间空值率较高,故后续可使用事件时间代替出险时间。

7. 统计成功投保重疾险种且发生理赔（且理赔时间不为空值）的总人数：
select count(mobile_id),count(distinct mobile_id) from dwb.insurance_txt_claim where the_date regexp '2019|2020|2021' and product_name REGEXP '重疾|重大疾病' and deal_date IS NOT NULL ;
-----------------------------------
count(mobile_id)
count(DISTINCT mobile_id)
329
155
------------------------------------
155/323259=0.048%;                1-0.048%=99.952%
结论：只有155人理赔时间能够确认,即有99.952%不能确定出险时间（出险时间字段deal_date空值率较高所致）

8.  统计成功投保重疾险种且合同期（满两年）内发生理赔的总人数（包括两年多才发生理赔的人员）：
drop table nlp_dev.tdl_af_insur_list;
CREATE TABLE nlp_dev.tdl_af_insur_list AS
SELECT
    a.mobile_id,
    CASE
        WHEN a.mobile_id IS NOT NULL
        AND b.mobile_id IS NOT NULL THEN '1'
        WHEN a.mobile_id IS NOT NULL
        AND b.mobile_id IS NULL THEN '0'
    END AS claim_status
FROM
    (
        SELECT
            mobile_id
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
            AND datediff(end_date, effective_date) > 720 --过滤掉合同期限不满足两年或无法确定合同期限的部分(end_date或者effective_date为空)
        GROUP BY
            mobile_id
    ) a
    LEFT JOIN (
        SELECT
            mobile_id
        FROM
            dwb.insurance_txt_claim
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
        GROUP BY
            mobile_id
    ) b ON a.mobile_id = b.mobile_id

-- 统计发生理赔和未发生理赔的人数（无两年内的时间限制）
select claim_status,count(mobile_id) from nlp_dev.tdl_af_insur_list group by claim_status;
-------------------
claim_status	    count(mobile_id)
0   43497
1   106
----------------------
总名单数：43178+106=43603
无两年时间限制的情况下：
发生理赔的占比：106/43603=0.24%
未发生理赔的占比：43497/43603=99.76%

-- 验证106人的出险时间
select a.* from
(select mobile_id,event_time,report_date,deal_date from dwb.insurance_txt_claim where  the_date regexp '2019|2020|2021' and product_name REGEXP '重疾|重大疾病')a
join
(select * from nlp_dev.tdl_af_insur_list where claim_status='1')b
on a.mobile_id = b.mobile_id
------------------------------
result:(仅列出部分样例)
60719748	2021-09-24 22:53:41	NULL	NULL
60719748	2021-09-24 22:47:03	NULL	NULL
160180241	2021-01-22 14:36:40	NULL	NULL
205039128	2021-01-22 14:23:11	NULL	NULL
480551581	2021-01-22 14:28:18	NULL	NULL
302536890	2020-12-18 18:00:07	NULL	NULL
425758210	2020-12-19 15:51:39	NULL	NULL
546316324	2021-01-01 11:51:20	NULL	NULL
163322010	2021-01-01 11:27:38	NULL	NULL
290036851	2020-02-19 16:53:17	NULL	NULL

结论：这106人的出险时间均为空值（承上启下：故加上两年内的时间限制后，符合条件的数据为0）

9. 两年内发生理赔的人员名单（合同期限满两年）
-- 创建中间表用于验证数据
drop table nlp_dev.tdl_af_insur_list_tmp;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp AS
SELECT
    a.mobile_id,
    a.effective_date,
    a.end_date,
    b.t,
    CASE
        WHEN a.mobile_id IS NOT NULL
        AND b.mobile_id IS NOT NULL THEN '1'
        WHEN a.mobile_id IS NOT NULL
        AND b.mobile_id IS NULL THEN '0'
    END AS claim_status
FROM
    (
        SELECT
            mobile_id,
            effective_date,
            end_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病'
            AND datediff(end_date, effective_date) > 720
    ) a
    LEFT JOIN (
        SELECT
            mobile_id,
            MIN(deal_date) AS t --取最近的出险时间，使用max()和min()可以实现多条为一条
        FROM
            dwb.insurance_txt_claim
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND deal_date IS NOT NULL
            AND product_name REGEXP '重疾|重大疾病'
        GROUP BY
            mobile_id
    ) b ON a.mobile_id = b.mobile_id

----------------------
WHERE        --两年内发生理赔的时间限制条件
    c.t >= c.effective_date
    AND c.t < DATE_ADD(c.effective_date, 720)
----------------------

-- 查询中间表结果
select * from nlp_dev.tdl_af_insur_list_tmp  where claim_status='1'; --0条数据
select claim_status,count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp group by claim_status;
--------------------
0   43603
------------------

-- 查询结果
select count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp group by claim_status;
----------
47858
43603
-----------

以下探查：放宽 保险合同满两年的时间限制，即探查：在满足合同期限的生效日期不为空，但截止日期为空的情况，找出两年表现期内发生理赔的人员
-- 统计从保单生效期开始,两年内出现理赔的人员名单（合同期限的生效日期不为空，但截止日期为空的情况）
drop table nlp_dev.tdl_af_insur_list_03;
CREATE TABLE nlp_dev.tdl_af_insur_list_03 AS
SELECT
    c.mobile_id,
    c.effective_date,
    c.end_date,
    c.t,
    c.claim_status
FROM
    (
        SELECT
            a.mobile_id,
            a.effective_date,
            a.end_date,
            b.t,
            CASE
                WHEN a.mobile_id IS NOT NULL
                AND b.mobile_id IS NOT NULL THEN '1'
                WHEN a.mobile_id IS NOT NULL
                AND b.mobile_id IS NULL THEN '0'
            END AS claim_status
        FROM
            (
                SELECT
                    mobile_id,
                    effective_date,
                    end_date
                FROM
                    dwb.insurance_txt_apply
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND end_date is null
                    AND effective_date is not null --只要合同生效日期不为空即可
                    AND product_name REGEXP '重疾|重大疾病'
            ) a
            LEFT JOIN (
                SELECT
                    mobile_id,
                    MIN(deal_date) AS t --取最近的出险时间，使用max()和min()可以实现多条为一条
                FROM
                    dwb.insurance_txt_claim
                WHERE
                    the_date REGEXP '2019|2020|2021'
                    AND deal_date is not null
                    AND product_name REGEXP '重疾|重大疾病'
                GROUP BY
                    mobile_id
            ) b on a.mobile_id=b.mobile_id
    ) c
WHERE    --过滤出两年内发生理赔的人员名单
    c.t >= c.effective_date
    AND c.t < DATE_ADD(c.effective_date, 720)

-- 查询
select * from nlp_dev.tdl_af_insur_list_03;
第二种情况的样例：（全部数据，就仅仅只有6条满足的数据）
+---------------------+-----------------+-----------+-------------+---------------+--+
|      mobile_id      | effective_date  | end_date  |    time     | claim_status  |
+---------------------+-----------------+-----------+-------------+---------------+--+
| 1887677124          | 2020-12-22      | NULL      | 2021-05-18  | 1             |
| 262084900348508430  | 2021-02-01      | NULL      | 2021-02-01  | 1             |
| 1456671638          | 2020-03-15      | NULL      | 2021-11-18  | 1             |
| 103244912           | 2021-01-21      | NULL      | 2021-01-23  | 1             |
| 125215355           | 2020-07-03      | NULL      | 2020-09-21  | 1             |
| 1006684927          | 2020-08-14      | NULL      | 2021-01-11  | 1             |
+---------------------+-----------------+-----------+-------------+---------------+--+

-- 查询结果
select claim_status,count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_03 group by claim_status;
+---------------+------+--+
claim_status	count(DISTINCT mobile_id)
1   6
+---------------+------+--+

综上所述，保险建模一期数据探查结果如下：
1.以保单生效日期为观察点，表现期定为2年，发生核保理赔定义1，未发生理赔的定义为0，剔除保险合同未满2年的样本：>>> 没有符合的数据；                            结果表：nlp_dev.tdl_af_insur_list_tmp
2.如果表现期定为2.5年，则合同未满2年且两年内发生理赔核验的样本数为                                 >>>  106（注：这106个人对应的出险时间均为null）     结果表：nlp_dev.tdl_af_insur_list
3.以保单生效日期不为空，保险失效期为空，但在保单生效期的两年内发生理赔核验的样本数：                    >>>  6                                       结果表：nlp_dev.tdl_af_insur_list_03

