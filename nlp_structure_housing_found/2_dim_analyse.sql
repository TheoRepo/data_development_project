-- 需求：确认一下公积金事件库的覆盖率，维度（是否缴存，缴存基数，缴存比例）  
-- 统计思路：将所有结果表UNION ALL，然后根据相关字段（地域字段，行为字段，msg字段）的正则匹配，做行地域维度，行为维度，个人/公司维度的统计


-- 统计
-- 核心思路：大表撞小表，把统计结果呈现在一张表上，方便复制黏贴到excel

-- 围绕基础分析表，展开统计工作
-- nlp_dev.housing_found_txt_area_online_v2
-- nlp_dev.housing_found_txt_area_action_type_online_v2

-- 计算nlp_online中公积金总数据量和人群
SELECT count(1) data_cnt,count(distinct mobile_id) mobile_id_cnt FROM nlp_dev.housing_found_txt_area_action_type_online_v2;
-- 结果：
-- data_cnt     mobile_id_cnt
-- 683152002    71370077


-- -- 按照三种维度进行分析
-- SELECT
--     province_name,
--     city_name,
--     act_type,
--     type,
--     COUNT(1) cnt,
--     COUNT(DISTINCT mobile_id) mobile_id_cnt
-- FROM
--     nlp_dev.housing_found_txt_area_action_type_online_v2
-- GROUP BY
--     province_name, -- 省份
--     city_name, -- 城市
--     act_type, -- 行为类型
--     type; -- 对公对私


-- 按照地域

-- 按照省份分析

-- 1)识别出省份的聚合：
DROP TABLE IF EXISTS nlp_dev.tdl_housing_found_txt_online_province;
CREATE TABLE IF NOT EXISTS nlp_dev.tdl_housing_found_txt_online_province as 
SELECT
    b.province_name,
    COUNT(1) data_cnt,
    COUNT(DISTINCT mobile_id) mobile_id_cnt
FROM
    nlp_dev.housing_found_txt_area_action_type_online_v2 a
    JOIN (
        SELECT
            DISTINCT province_name
        FROM
            nlp_dev.dim_province_city_county_v2
    ) b ON a.province_name = substr(b.province_name, 1, 2)
GROUP BY
    b.province_name
ORDER BY
    data_cnt DESC,
    mobile_id_cnt DESC;


-- 查看结果
SELECT
    *
FROM
    nlp_dev.tdl_housing_found_txt_online_province;
    
-- 2) 计算省份未识别的聚合:
DROP TABLE IF EXISTS nlp_dev.tdl_housing_found_txt_online_province_unknown;
CREATE TABLE IF NOT EXISTS nlp_dev.tdl_housing_found_txt_online_province_unknown as 
SELECT
    'unknown' AS province,
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
FROM
    (
        SELECT
            mobile_id,
            a.province_name,
            b.province_name province
        FROM
            nlp_dev.housing_found_txt_area_action_type_online_v2 a
            LEFT JOIN (
                SELECT
                    DISTINCT province_name
                FROM
                    nlp_dev.dim_province_city_county_v2
            ) b ON a.province_name = substr(b.province_name, 1, 2)
    ) c
WHERE
    c.province IS NULL;

-- 检查结果：
-- SELECT * FROM nlp_dev.tdl_housing_found_txt_online_province_unknown;


-- 单独计算能识别为省直的部分
SELECT
    '省直' as province,
    COUNT(1) data_cnt,
    COUNT(DISTINCT mobile_id) mobile_id_cnt
 FROM
(
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_account_manage
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_change_base
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_deposit
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        NULL AS funds_center
    FROM
        nlp_online.housing_found_txt_salary
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_withdraw
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_apply
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_lEND
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_repay_failed
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_repay_settle
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_repay_overdue
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_repay_success
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_to_repay
    WHERE
        the_date REGEXP '2021'
    UNION ALL
    SELECT
        mobile_id,
        app_name,
        suspected_app_name,
        funds_center
    FROM
        nlp_online.housing_found_txt_loan_unknown_status
    WHERE
        the_date REGEXP '2021'
) a 
WHERE
    a.app_name REGEXP '省直'
    OR a.suspected_app_name REGEXP '省直'
    OR a.funds_center REGEXP '省直';
    
-- 计算结果：
-- province    data_cnt    mobile_id_cnt


-- 按照省份-市分析

-- 1)识别出省、市的聚合：
DROP TABLE nlp_dev.tdl_housing_found_txt_area_province_city;
CREATE TABLE nlp_dev.tdl_housing_found_txt_area_province_city
SELECT
    b.province_name,
    b.city_name,
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
FROM
    nlp_dev.housing_found_txt_area_action_type_online_v2 a
    JOIN (
        SELECT
            DISTINCT province_name,
            city_name
        FROM
            nlp_dev.dim_province_city_county_v2
        WHERE
            substr(city_name, 1, 2) NOT REGEXP '省直|张家'
    ) b ON a.city_name = substr(b.city_name, 1, 2)
GROUP BY
    b.province_name,
    b.city_name
ORDER BY
    b.province_name,
    data_cnt DESC,
    mobile_id_cnt DESC;
    
-- 结果：
-- SELECT * FROM nlp_dev.tdl_housing_found_txt_area_province_city;

-- 2)识别出省，未识别出市的聚合：
SELECT
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
FROM
    (
        SELECT
            mobile_id,
            a.province_name province,
            a.city_name,
            b.city_name city
        FROM
            nlp_dev.housing_found_txt_area_action_type_online_v2 a
            LEFT JOIN (
                SELECT
                    DISTINCT province_name,
                    city_name
                FROM
                    nlp_dev.dim_province_city_county_v2
                WHERE
                    substr(city_name, 1, 2) NOT REGEXP '省直|张家'
            ) b ON a.city_name = substr(b.city_name, 1, 2)
    ) c
WHERE
    c.city IS NULL
    AND c.province REGEXP '北京|天津|上海|重庆|内蒙|广西|西藏|宁夏|新疆|河北|山西|辽宁|吉林|黑龙|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海';

-- 结果：
-- data_cnt    mobile_id_cnt

-- 3)省和市均未识别的聚合：
SELECT
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
FROM
    (
        SELECT
            mobile_id,
            a.province_name province,
            b.city_name city
        FROM
            nlp_dev.housing_found_txt_area_action_type_online_v2 a
            LEFT JOIN (
                SELECT
                    DISTINCT province_name,
                    city_name
                FROM
                    nlp_dev.dim_province_city_county_v2
                WHERE
                    substr(city_name, 1, 2) NOT REGEXP '省直|张家'
            ) b ON a.city_name = substr(b.city_name, 1, 2)
    ) c
WHERE
    c.city IS NULL
    AND c.province not REGEXP '北京|天津|上海|重庆|内蒙|广西|西藏|宁夏|新疆|河北|山西|辽宁|吉林|黑龙|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海'
    ;
-- 结果:
-- data_cnt    mobile_id_cnt




-- 按照行为聚合
SELECT
    act_type,
    COUNT(1) data_cnt,
    COUNT(DISTINCT mobile_id) mobile_id_cnt
FROM
    nlp_dev.housing_found_txt_area_action_type_online_v2
group by 
        act_type
order by
        data_cnt DESC,mobile_id_cnt DESC;
        
-- 结果：
-- act_type    data_cnt    mobile_id_cnt




-- 按照个人/对公和分析
SELECT
    type,
    COUNT(1) data_cnt,
    COUNT(DISTINCT mobile_id) mobile_id_cnt
FROM
    nlp_dev.housing_found_txt_area_action_type_online_v2
group by 
    type
order by
    data_cnt DESC,mobile_id_cnt DESC;

-- 结果：
-- type	data_cnt	mobile_id_cnt
-- 个人    680217056   71283103
-- 对公    2934946     334619




-- 综合分析

-- 按照省份-行为分析

-- 1) 识别出省份的
SELECT
    b.province_name,
    a.act_type,
    COUNT(1) data_cnt,
    COUNT(DISTINCT mobile_id) mobile_id_cnt
FROM
    nlp_dev.housing_found_txt_area_action_type_online_v2 a
    JOIN (
        SELECT
            DISTINCT province_name
        FROM
            nlp_dev.dim_province_city_county_v2
    ) b ON a.province_name = substr(b.province_name, 1, 2)
GROUP BY
    b.province_name,a.act_type
ORDER BY
    b.province_name,
    data_cnt DESC,
    mobile_id_cnt DESC;
    
-- 结果：

-- 2) 未识别出省份的
SELECT
    'unknown' AS province,
    c.act_type,
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
FROM
    (
        SELECT
            mobile_id,
            act_type,
            a.province_name,
            b.province_name province
        FROM
            nlp_dev.housing_found_txt_area_action_type_online_v2 a
            LEFT JOIN (
                SELECT
                    DISTINCT province_name
                FROM
                    nlp_dev.dim_province_city_county_v2
            ) b ON a.province_name = substr(b.province_name, 1, 2)
    ) c
WHERE
    c.province IS NULL
GROUP BY
    c.province,c.act_type
ORDER BY
    data_cnt DESC,
    mobile_id_cnt DESC;

-- 结果：
-- province    act_type    data_cnt    mobile_id_cnt
-- unknown 贷款 101081510 10333194
-- unknown 缴存 85881727 12841574
-- unknown 其它 73660703 15468034
-- unknown 缴存提醒 6834 2162


-- 按照省市-行为分组聚合

-- 1)识别出省、市的聚合：
DROP TABLE nlp_dev.tdl_housing_found_txt_area_province_city_action;
CREATE TABLE nlp_dev.tdl_housing_found_txt_area_province_city_action
SELECT
    b.province_name,
    b.city_name,
    a.act_type,
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
FROM
    nlp_dev.housing_found_txt_area_action_type_online_v2 a
    JOIN (
        SELECT
            DISTINCT province_name,
            city_name
        FROM
            nlp_dev.dim_province_city_county_v2
        WHERE
            substr(city_name, 1, 2) NOT REGEXP '省直|张家'
    ) b ON a.city_name = substr(b.city_name, 1, 2)
GROUP BY
    b.province_name,
    b.city_name,
    a.act_type
ORDER BY
    b.province_name,
    b.city_name,
    data_cnt DESC,
    mobile_id_cnt DESC;
    
    

-- 检查结果：
-- SELECT * FROM  nlp_dev.tdl_housing_found_txt_area_province_city_action;


-- 2)识别出省，未识别出市的聚合：
SELECT
    -- c.province,
    'unknown' AS city,
    c.act_type,
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
-- SELECT count(1) cnt,COUNT(DISTINCT mobile_id) AS mobile_id_cnt             --   128221527   18737091
FROM
    (
        SELECT
            mobile_id,
            a.province_name province,
            a.city_name,
            b.city_name city,
            a.act_type
        FROM
            nlp_dev.housing_found_txt_area_action_type_online_v2 a
            LEFT JOIN (
                SELECT
                    DISTINCT province_name,
                    city_name
                FROM
                    nlp_dev.dim_province_city_county_v2
                WHERE
                    substr(city_name, 1, 2) NOT REGEXP '省直|张家'
            ) b ON a.city_name = substr(b.city_name, 1, 2)
    ) c
WHERE
    c.city IS NULL
    AND c.province REGEXP '北京|天津|上海|重庆|内蒙|广西|西藏|宁夏|新疆|河北|山西|辽宁|吉林|黑龙|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海'
GROUP BY
    -- c.province,
    c.act_type
ORDER BY
    -- c.province,
    data_cnt DESC,
    mobile_id_cnt DESC;


-- 3)省和市均未识别的聚合：
SELECT
    c.act_type,
    COUNT(1) AS data_cnt,
    COUNT(DISTINCT mobile_id) AS mobile_id_cnt
-- SELECT count(1) cnt,COUNT(DISTINCT mobile_id) AS mobile_id_cnt               --144951929  22083564
FROM
    (
        SELECT
            mobile_id,
            a.province_name province,
            b.city_name city,
            a.act_type
        FROM
            nlp_dev.housing_found_txt_area_action_type_online_v2 a
            LEFT JOIN (
                SELECT
                    DISTINCT province_name,
                    city_name
                FROM
                    nlp_dev.dim_province_city_county_v2
                WHERE
                    substr(city_name, 1, 2) NOT REGEXP '省直|张家'
            ) b ON a.city_name = substr(b.city_name, 1, 2)
    ) c
WHERE
    c.city IS NULL
    AND c.province not REGEXP '北京|天津|上海|重庆|内蒙|广西|西藏|宁夏|新疆|河北|山西|辽宁|吉林|黑龙|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海'
GROUP BY
    c.act_type
ORDER BY
    data_cnt DESC,
    mobile_id_cnt DESC;   

