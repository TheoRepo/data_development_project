#!/bin/bash
source /etc/profile
source ~/.bash_profile


sql_part="
-- 对定义为‘1’的数据探查如下：
insert overwrite table nlp_dev.tdl_qianyu_insur_claim_list_tmp
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


-- 对定义为‘0’的数据探查如下：
insert overwrite table nlp_dev.tdl_qianyu_insur_unclaim_list_tmp
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
    ;
"

bash submit.sh "data_development" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo "数据写入完成"