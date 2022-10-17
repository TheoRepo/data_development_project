-- 对定义为‘1’的数据探查如下：
-- 先从较粗的粒度统计：即mobile_id粒度
-- 保险理赔清单
drop table if exists nlp_dev.tdl_qianyu_insur_claim_list_tmp;
CREATE TABLE nlp_dev.tdl_qianyu_insur_claim_list_tmp AS
SELECT
    a.row_key,
    a.mobile_id,
    a.the_date as start_time,
    b.t as end_time,
    CASE
        WHEN  b.mobile_id IS NOT NULL THEN '1'
        WHEN  b.mobile_id IS NULL THEN '0'
    END AS claim_status,
    a.insur_institute
FROM
    (-- 从投保表中全表筛选出重疾险种名单，保留时间分区字段用于判断两年的存续条件
        SELECT
            row_key,
            mobile_id,
            the_date,
            insur_institute,
        FROM
            nlp_dev.insurance_txt_apply_white_list
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


-- 对定义为‘0’的数据探查如下：
-- 结合过期、退保、理赔表进行反向探查：即，只要没在过期、退保、理赔表中出现的投保人员（重疾险种），就默认为 '0'的分类中。
-- mobile_id粒度
drop table if exists nlp_dev.tdl_qianyu_insur_unclaim_list_tmp;
CREATE TABLE nlp_dev.tdl_qianyu_insur_unclaim_list_tmp AS
SELECT
    a.row_key,
    a.mobile_id,
    a.the_date AS start_time,
    b.t AS end_time,
    CASE
        WHEN b.mobile_id IS NOT NULL THEN '1'
        WHEN b.mobile_id IS NULL THEN '0'
    END AS claim_status,
    a.insur_institute
FROM
    (
        -- 从投保表中全表筛选出重疾险种名单，保留时间分区字段用于判断两年的存续条件
        SELECT
            row_key,
            mobile_id,
            the_date,
            insur_institute
        FROM
            nlp_dev.insurance_txt_apply_white_list
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



-- 按照观察期选取结果数据
CREATE TABLE nlp_dev.tdl_qianyu_insur_list_20220829 AS
SELECT 
    row_key,
    mobile_id,
    start_time,
    end_time,
    claim_status,
    insur_institute
from 
    (
    select 
        a.row_key,
        a.mobile_id,
        a.start_time,
        a.end_time,
        a.claim_status,
        insur_institute
    from 
        nlp_dev.tdl_qianyu_insur_claim_list_tmp a
    where a.claim_status = '1'
    -- 在两年的表现期（观察期）内，发生理赔行为，的人员名单
    and a.start_time>='2019-07-01' and date_add(a.start_time,0)>=date_sub(a.end_time,720)
    -- 逻辑核验: 行为一定发生在观察点以后
    and date_add(a.start_time,0) < a.end_time
    UNION ALL
    select 
        b.row_key,
        b.mobile_id,
        b.start_time,
        b.end_time,
        b.claim_status,
        insur_institute
    from 
        nlp_dev.tdl_qianyu_insur_unclaim_list_tmp b
    where b.claim_status = '0'
    -- 在两年的表现期（观察期）内，没有理赔行为，没有退保行为，没有保险过期记录，的人员名单
    and b.start_time>='2019-07-01' and start_time<=date_sub('2021-12-05',720)
    UNION ALL
    select 
        c.row_key,
        c.mobile_id,
        c.start_time,
        c.end_time,
        '0'  as claim_status,
        insur_institute
    from 
        nlp_dev.tdl_qianyu_insur_unclaim_list_tmp c
    where c.claim_status = '1'
    -- 在两年的表现期（观察期）外，发生理赔行为，退保行为，保险过期记录，的人员名单
    and c.start_time>='2019-07-01' and date_add(c.start_time,0)<date_sub(c.end_time,720) -- (不包括临界点，因为1的数据包含了临界点)
    -- 逻辑核验: 行为一定发生在观察点以后
    and date_add(c.start_time,0) < c.end_time
    ) tab
    ;