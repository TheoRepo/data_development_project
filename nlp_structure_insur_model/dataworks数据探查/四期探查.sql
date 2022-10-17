
-- 优化需求: 机构分布的统计结果中，不需要投保渠道：比如水滴保险、轻松保，只要投保机构，比如中国人寿
-- 措施: 以银保监会公布的保险公司名单为标准，从中提取关键词碰撞msg字段得到机构名称字段，通过关键词和机构全称的映射关系补全机构名称字段
-- 代码逻辑: 
-- 1. 使用投保表dwb.insurance_txt_apply，和dwb.insurance_txt_claim理赔表发生碰撞，获得重疾险的保险产品的理赔人员名单，记作中间表1
-- 2. 使用中间表,以row_key作为关键词，去碰撞nlp_online.insurance_txt_apply，获得msg字段，记作中间表2
-- 3. 以银保监会公布的保险公司名单中提取关键词，将msg字段和关键词匹配，得到保险机构字段，结果记作中间表3
-- 4. 利用保险机构字段，mobile_id字段，进行机构理赔人员分布统计

-- 跑数效率:
-- 1. 其中中间表1（小表）和nlp_online.insurance_txt_apply（大表）左连的时候，需要解决小表连接大表的问题。

-- 中间表1
drop table nlp_dev.tdl_qianyu_zhongji_list_tmp;
CREATE TABLE nlp_dev.tdl_qianyu_zhongji_list_tmp AS
SELECT
    a.row_key,
    a.mobile_id,
    a.app_name,
    a.suspected_app_name,
    a.the_date as start_time,
    b.t as end_time,
    CASE
        WHEN  b.mobile_id IS NOT NULL THEN 'A'
        WHEN  b.mobile_id IS NULL THEN 'B'
    END AS claim_status
FROM
    (-- 从投保表中全表筛选出重疾险种名单，保留时间分区字段用于判断两年的存续条件
        SELECT
            row_key,
            mobile_id,
            app_name,
            suspected_app_name,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'    -- '1'的数据不需要过滤掉表现期以外的人员数据
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
    

-- 中间表2
-- 添加msg字段
-- 小表连接大表优化
drop table nlp_dev.tdl_qianyu_zhongji_list_msg_tmp;
CREATE TABLE nlp_dev.tdl_qianyu_zhongji_list_msg_tmp AS
SELECT 
	/*+MAPJOIN(a)*/   --将小表放入内存
    a.row_key,
    a.mobile_id,
    a.app_name,
    a.suspected_app_name,
    a.start_time,
    a.end_time,
    a.claim_status,
    b.msg
FROM nlp_dev.tdl_qianyu_zhongji_list_tmp a
LEFT JOIN 
    (
    SELECT row_key, msg
    FROM nlp_online.insurance_txt_apply 
    WHERE the_date regexp '2019|2020|2021'
    ) b
ON a.row_key=b.row_key
;


-- 中间表3
-- 添加机构字段
drop table nlp_dev.tdl_qianyu_zhongji_list_institute_tmp;
CREATE TABLE nlp_dev.tdl_qianyu_zhongji_list_institute_tmp AS
SELECT 
    row_key,
    mobile_id,
    app_name,
    suspected_app_name,
    start_time,
    end_time,
    claim_status,
    CASE 
        WHEN msg regexp '泰和人寿' THEN '英大泰和人寿保险股份有限公司'
        WHEN msg regexp '友邦保险' THEN '友邦保险有限公司上海分公司'
        WHEN msg regexp '长城人寿' THEN '长城人寿保险股份有限公司'
        WHEN msg regexp '信诺人寿' THEN '招商信诺人寿保险有限公司'
        WHEN msg regexp '正德人寿' THEN '正德人寿保险股份有限公司'    
        WHEN msg regexp '安联人寿' THEN '中德安联人寿保险有限公司'
        WHEN msg regexp '中法人寿' THEN '中法人寿保险有限责任公司'
        WHEN msg regexp '中国人寿' THEN '中国人寿保险股份有限公司'
        WHEN msg regexp '中韩人寿' THEN '中韩人寿保险有限公司'
        WHEN msg regexp '三星人寿' THEN '中银三星人寿保险有限公司'
        WHEN msg regexp '中荷人寿' THEN '中荷人寿保险有限公司'
        WHEN msg regexp '大都会' THEN '中美联泰大都会人寿保险有限公司'
        WHEN msg regexp '恒大人寿' THEN '恒大人寿保险有限公司'
        WHEN msg regexp '中意人寿' THEN '中意人寿保险有限公司'
        WHEN msg regexp '中英人寿' THEN '中英人寿保险有限公司'
        WHEN msg regexp '珠江人寿' THEN '珠江人寿保险股份有限公司'
        WHEN msg regexp '安邦人寿' THEN '安邦人寿保险股份有限公司'
        WHEN msg regexp '百年人寿' THEN '百年人寿保险股份有限公司'
        WHEN msg regexp '方正人寿' THEN '北大方正人寿保险有限公司'
        WHEN msg regexp '安盛人寿' THEN '工银安盛人寿保险有限公司'
        WHEN msg regexp '永明人寿' THEN '光大永明人寿保险有限公司'
        WHEN msg regexp '国泰人寿' THEN '国泰人寿保险有限责任公司'    
        WHEN msg regexp '海康人寿' THEN '海康人寿保险有限公司'
        WHEN msg regexp '合众人寿' THEN '合众人寿保险股份有限公司'
        WHEN msg regexp '和谐健康' THEN '和谐健康保险股份有限公司'
        WHEN msg regexp '标准人寿' THEN '恒安标准人寿保险有限公司'
        WHEN msg regexp '华汇人寿' THEN '华汇人寿保险股份有限公司'
        WHEN msg regexp '汇丰人寿' THEN '汇丰人寿保险有限公司'
        WHEN msg regexp '吉祥人寿' THEN '吉祥人寿保险股份有限公司'
        WHEN msg regexp '君龙人寿' THEN '君龙人寿保险有限公司'
        WHEN msg regexp '昆仑健康' THEN '昆仑健康保险股份有限公司'
        WHEN msg regexp '利安人寿' THEN '利安人寿保险股份有限公司'
        WHEN msg regexp '民生人寿' THEN '民生人寿保险股份有限公司'
        WHEN msg regexp '农银人寿' THEN '农银人寿保险股份有限公司'
        WHEN msg regexp '平安健康' THEN '平安健康保险股份有限公司'
        WHEN msg regexp '平安人寿' THEN '中国平安人寿保险股份有限公司'
        WHEN msg regexp '平安养老' THEN '平安养老保险股份有限公司'
        WHEN msg regexp '前海人寿' THEN '前海人寿保险股份有限公司'
        WHEN msg regexp '人民健康' THEN '中国人民健康保险股份有限公司'    
        WHEN msg regexp '人民人寿' THEN '中国人民人寿保险股份有限公司'
        WHEN msg regexp '瑞泰人寿' THEN '瑞泰人寿保险有限公司'
        WHEN msg regexp '生命人寿' THEN '生命人寿保险股份有限公司'
        WHEN msg regexp '太平人寿' THEN '太平人寿保险有限公司'
        WHEN msg regexp '太平洋人寿' THEN '中国太平洋人寿保险股份有限公司'
        WHEN msg regexp '太平养老' THEN '太平养老保险股份有限公司'
        WHEN msg regexp '泰康人寿' THEN '泰康人寿保险股份有限公司'
        WHEN msg regexp '泰康养老' THEN '泰康养老保险股份有限公司'
        WHEN msg regexp '天安人寿' THEN '天安人寿保险股份有限公司'
        WHEN msg regexp '海航人寿' THEN '新光海航人寿保险有限责任公司'
        WHEN msg regexp '新华人寿' THEN '新华人寿保险股份有限公司'
        WHEN msg regexp '幸福人寿' THEN '幸福人寿保险股份有限公司'
        WHEN msg regexp '阳光人寿' THEN '阳光人寿保险股份有限公司'
        WHEN msg regexp '国华人寿' THEN '国华人寿保险股份有限公司'
        WHEN msg regexp '弘康人寿' THEN '弘康人寿保险股份有限公司'
        WHEN msg regexp '中融人寿' THEN '中融人寿保险股份有限公司'
        WHEN msg regexp '华夏人寿' THEN '华夏人寿保险股份有限公司'    
        WHEN msg regexp '中邮人寿' THEN '中邮人寿保险股份有限公司'
        WHEN msg regexp '安顾人寿' THEN '德华安顾人寿保险有限公司'
        WHEN msg regexp '康联人寿' THEN '交银康联人寿保险有限公司'
        WHEN msg regexp '信诚人寿' THEN '信诚人寿保险有限公司'
        WHEN msg regexp '信泰人寿' THEN '信泰人寿保险股份有限公司'
        WHEN msg regexp '东吴人寿' THEN '东吴人寿保险股份有限公司'
        WHEN msg regexp '华泰人寿' THEN '华泰人寿保险股份有限公司'
        WHEN msg regexp '长生人寿' THEN '长生人寿保险有限公司'
        WHEN msg regexp '德信人寿' THEN '复星保德信人寿保险有限公司'
        WHEN msg regexp '中宏人寿' THEN '中宏人寿保险有限公司'
        WHEN msg regexp '安邦养老' THEN '安邦养老保险股份有限公司'
        WHEN msg regexp '渤海人寿' THEN '渤海人寿保险股份有限公司'
        WHEN msg regexp '陆家嘴国泰人寿' THEN '陆家嘴国泰人寿保险有限责任公司'
        WHEN msg regexp '安联健康' THEN '太保安联健康保险股份有限公司'
        WHEN msg regexp '富德生命人寿' THEN '富德生命人寿保险股份有限公司'
        WHEN msg regexp '国联人寿' THEN '国联人寿保险股份有限公司'
        WHEN msg regexp '上海人寿' THEN '上海人寿保险股份有限公司'    
        WHEN msg regexp '全球人寿' THEN '同方全球人寿保险有限公司'
        WHEN msg regexp '君康人寿' THEN '君康人寿保险股份有限公司'
        WHEN msg regexp '联合人寿' THEN '中华联合人寿保险股份有限公司'
        WHEN msg regexp '横琴人寿' THEN '横琴人寿保险有限公司'
        WHEN msg regexp '和泰人寿' THEN '和泰人寿保险股份有限公司'
        WHEN msg regexp '联合健康' THEN '复星联合健康保险股份有限公司'
        WHEN msg regexp '华贵人寿' THEN '华贵人寿保险股份有限公司'
        WHEN msg regexp '信美人寿' THEN '信美人寿相互保险社'
        WHEN msg regexp '仁和人寿' THEN '招商局仁和人寿保险股份有限公司'
        WHEN msg regexp '爱心人寿' THEN '爱心人寿保险股份有限公司'
        WHEN msg regexp '中航三星' THEN '中航三星保险有限公司'
        WHEN msg regexp '大东方人寿' THEN '中新大东方人寿保险有限公司'
        WHEN msg regexp '保诚人寿' THEN '中信保诚人寿保险有限公司'
        WHEN msg regexp '三峡人寿' THEN '三峡人寿保险股份有限公司'
        WHEN msg regexp '北京人寿' THEN '北京人寿保险股份有限公司'
        WHEN msg regexp '国宝人寿' THEN '国宝人寿保险股份有限公司'
        WHEN msg regexp '海保人寿' THEN '海保人寿保险股份有限公司'    
        WHEN msg regexp '国富人寿' THEN '国富人寿保险股份有限公司'
        WHEN msg regexp '瑞华健康' THEN '瑞华健康保险股份有限公司'
        WHEN msg regexp '鼎诚人寿' THEN '鼎诚人寿保险有限责任公司'
        WHEN msg regexp '建信人寿' THEN '建信人寿保险股份有限公司'
        WHEN msg regexp '大家人寿' THEN '大家人寿保险股份有限公司'
        WHEN msg regexp '大家养老' THEN '大家养老保险股份有限公司'
        WHEN msg regexp '友邦人寿' THEN '友邦人寿保险有限公司'
        WHEN msg regexp '财信吉祥人寿' THEN '财信吉祥人寿保险股份有限公司'
        WHEN msg regexp '永诚财产' THEN '永诚财产保险股份有限公司'
        WHEN msg regexp '太平洋健康' THEN '太平洋健康保险股份有限公司'
        WHEN msg regexp '交银人寿' THEN '交银人寿保险有限公司'
        WHEN msg regexp '小康人寿' THEN '小康人寿保险有限责任公司'
        WHEN msg regexp '国民养老' THEN '国民养老保险股份有限公司'
    END AS insur_institute
FROM nlp_dev.tdl_qianyu_zhongji_list_msg_tmp


-- 重疾险产品，理赔人员关于机构的分布统计
-- 观察点：投保事件发生时间
-- 观察期：720天
SELECT
    insur_institute,
    COUNT(DISTINCT mobile_id)
FROM
    (
        SELECT
            mobile_id,
            insur_institute,
            start_time,
            end_time,
            claim_status
        FROM
            nlp_dev.tdl_qianyu_zhongji_list_institute_tmp
    ) a
WHERE
    end_time > start_time -- 结束时间晚于观察点
    AND end_time <= DATE_ADD(start_time, 720) -- 观察期720天
    AND claim_status = 'A'
GROUP BY
    insur_institute
ORDER BY
    COUNT(DISTINCT mobile_id) desc,insur_institute;

-- 只展示去重人数>=500的统计
-- insur_institute	count(DISTINCT mobile_id)
-- 中国人寿保险	26984


-- 重疾险产品，理赔人员关于机构的分布统计
-- 观察点：投保后90天
-- 观察期：360天
SELECT
    insur_institute,
    COUNT(DISTINCT mobile_id)
FROM
    (
        SELECT
            mobile_id,
            insur_institute,
            start_time,
            end_time,
            claim_status
        FROM
            nlp_dev.tdl_af_insur_list_tmp_new
    ) a
WHERE
    end_time > date_add(start_time,90) -- 结束时间晚于观察点
    AND end_time <= DATE_ADD(date_add(start_time,90),360) -- 观察期360天
    AND claim_status = 'A'
GROUP BY
    insur_institute
ORDER BY
    COUNT(DISTINCT mobile_id) desc,insur_institute;

-- 只展示去重人数>=500的统计
-- insur_institute	count(DISTINCT mobile_id)
-- 中国人寿保险	26984

