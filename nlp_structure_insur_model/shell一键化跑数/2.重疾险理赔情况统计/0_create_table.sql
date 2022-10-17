drop table if exists nlp_dev.tdl_qianyu_insur_claim_list_tmp
CREATE TABLE nlp_dev.tdl_qianyu_insur_claim_list_tmp
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT '手机号映射id',
    start_time String COMMENT '投保日期yyyy-MM-dd格式',
    end_time String COMMENT '理赔日期yyyy-MM-dd格式',
    claim_status String COMMENT '理赔状态',
    insur_institute String COMMENT '机构名称'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc; 


drop table if exists nlp_dev.tdl_qianyu_insur_unclaim_list_tmp
CREATE TABLE nlp_dev.tdl_qianyu_insur_unclaim_list_tmp
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT '手机号映射id',
    start_time String COMMENT '投保日期yyyy-MM-dd格式',
    end_time String COMMENT '理赔日期yyyy-MM-dd格式',
    claim_status String COMMENT '理赔状态',
    insur_institute String COMMENT '机构名称'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc; 


drop table if exists nlp_dev.tdl_qianyu_insur_list_20220829
CREATE TABLE nlp_dev.tdl_qianyu_insur_list_20220829
(
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT '手机号映射id',
    start_time String COMMENT '投保日期yyyy-MM-dd格式',
    end_time String COMMENT '理赔日期yyyy-MM-dd格式',
    claim_status String COMMENT '理赔状态',
    insur_institute String COMMENT '机构名称'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc; 