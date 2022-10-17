#!/bin/bash
source /etc/profile
source ~/.bash_profile

observed_point=$1
observed_period=$2

sql_part="
-- 将两张过程表的整合成一张结果表
insert overwrite table nlp_dev.tdl_qianyu_insur_list_20220829
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
    and a.start_time>='2019-07-01' and date_add(a.start_time,${observed_point})>=date_sub(a.end_time,${observed_period})
    -- 逻辑核验: 行为一定发生在观察点以后
    and date_add(a.start_time,${observed_point}) < a.end_time
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
    and b.start_time>='2019-07-01' and start_time<=date_sub('2021-12-05',${observed_period})
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
    and c.start_time>='2019-07-01' and date_add(c.start_time,${observed_point})<date_sub(c.end_time,${observed_period}) -- (不包括临界点，因为1的数据包含了临界点)
    -- 逻辑核验: 行为一定发生在观察点以后
    and date_add(c.start_time,${observed_point}) < c.end_time
    ) tab
    ;
"

bash submit.sh "data_development" "nlp_dev" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo "数据写入完成"