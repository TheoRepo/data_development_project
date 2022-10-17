-- 业务需求：找到重疾险产品
-- 两年内发生理赔的人员，标签记作1；
-- 两年表现期内，没有理赔、退保、保险过期记录的人员，标签记作0；
-- 两年表现期外，拥有理赔、退保、保险过期记录的人员，标签记作0；

-- '0'的统计：

-- 实现逻辑: 投保表和理赔表左连
drop table nlp_dev.tdl_af_insur_list_tmp_new;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_new AS
SELECT
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
    
-- 老策略（'1'的统计）：
 
-- 第二问：
-- 以投保事件时间作为观察点，表现期为2年
-- 1）90天：
SELECT
    COUNT(mobile_id),
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > start_time
    AND end_time <= DATE_ADD(start_time, 90)
    AND claim_status = 'A';
-- result
-- 29734
-- 15752
-- 占比：select 15752/81323=21.4%

-- 2）180天：
SELECT
    COUNT(mobile_id),
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > start_time
    AND end_time <= DATE_ADD(start_time, 180)
    AND claim_status = 'A';
-- result
-- 66487
-- 38139
-- 占比：select 38139/81323=46.9%

-- 3）360天：
SELECT
    COUNT(mobile_id),
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > start_time
    AND end_time <= DATE_ADD(start_time, 360)
    AND claim_status = 'A';
-- result
-- 151748
-- 68154
-- 占比：select 68154/81323=83.8%

-- 4）450天：
SELECT
    COUNT(mobile_id),
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > start_time
    AND end_time <= DATE_ADD(start_time, 450)
    AND claim_status = 'A';
-- result
-- 170907
-- 74318
-- 占比：select 74318/81323=91.4%

-- 4）720天：
SELECT
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > start_time
    AND end_time <= DATE_ADD(start_time, 720)
    AND claim_status = 'A';
-- result
-- 81323

    
-- 月份分布（720天的统计时间区间）
select substr(start_time,1,7) month,count(mobile_id),count(distinct mobile_id) from nlp_dev.tdl_af_insur_list_tmp_new where end_time>start_time AND end_time <= date_add(start_time,720) and claim_status='A' group by substr(start_time,1,7) order by substr(start_time,1,7);
SELECT
    substr(start_time, 1, 7) MONTH,
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > start_time
    AND end_time <= DATE_ADD(start_time, 720)
    AND claim_status = 'A'
GROUP BY
    substr(start_time, 1, 7)
ORDER BY
    substr(start_time, 1, 7);
    
-- result:
-- month	count(DISTINCT mobile_id)
-- 2019-07 2766
-- 2019-08 3167
-- 2019-09	4247
-- 2019-10	3346
-- 2019-11	3782
-- 2019-12	3622
-- 2020-01	4628
-- 2020-02	7034
-- 2020-03	10653
-- 2020-04	8286
-- 2020-05	6996
-- 2020-06	18740
-- 2020-07	5064
-- 2020-08	5978
-- 2020-09	3469
-- 2020-10	3548
-- 2020-11	3986
-- 2020-12	5637
-- 2021-01	3681
-- 2021-02	1068
-- 2021-03	1076
-- 2021-04	932
-- 2021-05	1096
-- 2021-06	1344
-- 2021-07	1444
-- 2021-08	1229
-- 2021-09	212
-- 2021-10	360
-- 2021-11	136
-- 2021-12	1

-- 机构分布（720天的统计时间区间）
-- 机构字段的来历：app_name字段截取'-'前的内容
SELECT
    app_name,
    COUNT(DISTINCT mobile_id)
FROM
    (
        SELECT
            mobile_id,
            CASE
                WHEN app_name NOT REGEXP '-' THEN app_name
                WHEN app_name REGEXP '-' THEN regexp_extract(app_name, '(.*)-.*', 1)
            END AS app_name,
            start_time,
            end_time,
            claim_status
        FROM
            nlp_dev.tdl_af_insur_list_tmp_new
    ) a
WHERE
    end_time > start_time
    AND end_time <= DATE_ADD(start_time, 720)
    AND claim_status = 'A'
GROUP BY
    app_name
ORDER BY
    COUNT(DISTINCT mobile_id) desc,app_name;

-- result(只展示去重人数>=500的统计):
-- app_name	count(DISTINCT mobile_id)
-- 中国人寿保险	26984
-- 水滴保险	16309
-- 未识别	14787
-- 中国联通	13513
-- 众安保险	4542
-- 元保	4220
-- 轻松保	3355
-- 泰康保险	3191
-- 安心保险	2676
-- 中国平安 2544
-- 国华人寿	1976
-- 中信保诚人寿	1694
-- 悟空保	1374
-- 合众人寿	1214
-- 360保险	1105
-- 微保	1017
-- 中国平安保险	978
-- 中国太平洋保险	804
-- 复星联合健康	714
-- 中国人民保险	712
-- 新华保险	673
-- 信泰保险	526
-- 平安银行	517

-- 新策略（'1'的统计）：
-- 以投保后90天为基准时间，基准时间开始1年表现期

-- 1）90天：
SELECT
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > DATE_ADD(start_time, 90)
    AND end_time <= DATE_ADD(DATE_ADD(start_time, 90), 90)
    AND claim_status = 'A';
-- result
-- 25484
-- 占比：select 25484/72699=35%


-- 2）180天：
SELECT
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > DATE_ADD(start_time, 90)
    AND end_time <= DATE_ADD(DATE_ADD(start_time, 90), 180)
    AND claim_status = 'A';
-- result
-- 41084
-- 占比：select 41084/72699=57%

-- 3）360天：
SELECT
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > DATE_ADD(start_time, 90)
    AND end_time <= DATE_ADD(DATE_ADD(start_time, 90), 360)
    AND claim_status = 'A';
-- result
-- 64997
-- 占比：select 64997/72699=89%

-- 3）450天：
SELECT
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > DATE_ADD(start_time, 90)
    AND end_time <= DATE_ADD(DATE_ADD(start_time, 90), 450)
    AND claim_status = 'A';
-- result
-- 70623
-- 占比：  select 70623/72699=97%
  
-- 3）720天：
SELECT
    -- COUNT(DISTINCT mobile_id) 
    *
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > DATE_ADD(start_time, 90)
    AND end_time <= DATE_ADD(DATE_ADD(start_time, 90), 720)
    AND claim_status = 'A';
-- result
-- 72699


-- 月份分布(360天的统计时间区间)
SELECT
    substr(start_time, 1, 7) MONTH,
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_new
WHERE
    end_time > date_add(start_time,90)
    AND end_time <= DATE_ADD(date_add(start_time,90),360)
    AND claim_status = 'A'
GROUP BY
    substr(start_time, 1, 7)
ORDER BY
    substr(start_time, 1, 7);
    
-- result:
-- month	count(DISTINCT mobile_id)
-- 2019-07	111
-- 2019-08	119
-- 2019-09	701
-- 2019-10	1714
-- 2019-11	3409
-- 2019-12	3258
-- 2020-01	4182
-- 2020-02	6514
-- 2020-03	10287
-- 2020-04	7969
-- 2020-05	6779
-- 2020-06	18526
-- 2020-07	4913
-- 2020-08	5794
-- 2020-09	2683
-- 2020-10	1453
-- 2020-11	543
-- 2020-12	700
-- 2021-01	971
-- 2021-02	967
-- 2021-03	967
-- 2021-04	825
-- 2021-05	813
-- 2021-06	817
-- 2021-07	640
-- 2021-08	504
-- 2021-09	2


-- 机构分布（360天的统计时间区间）
SELECT
    app_name,
    COUNT(DISTINCT mobile_id)
FROM
    (
        SELECT
            mobile_id,
            CASE
                WHEN app_name NOT REGEXP '-' THEN app_name
                WHEN app_name REGEXP '-' THEN regexp_extract(app_name, '(.*)-.*', 1)
            END AS app_name,
            start_time,
            end_time,
            claim_status
        FROM
            nlp_dev.tdl_af_insur_list_tmp_new
    ) a
WHERE
    end_time > date_add(start_time,90)
    AND end_time <= DATE_ADD(date_add(start_time,90),360)
    AND claim_status = 'A'
GROUP BY
    app_name
ORDER BY
    COUNT(DISTINCT mobile_id) desc,app_name;

-- result(只展示去重人数>=500的统计):
-- app_name	count(DISTINCT mobile_id)
-- 中国人寿保险    22255
-- 中国联通    13485
-- 水滴保险    13356
-- 未识别  9469
-- 众安保险    2835
-- 安心保险    2316
-- 元保    2263
-- 中国平安    2135
-- 轻松保  2051
-- 泰康保险    1900
-- 国华人寿    795
-- 360保险 776
-- 微保    773
-- 合众人寿    757
-- 中国平安保险    669
-- 中国太平洋保险  604
-- 中国人民保险    505

------------------------------------------------
-- '0'的统计：

-- 先统计旧策略：
-- 添加机构字段,定义为'0'的数据
-- 实现逻辑: 投保表和（理赔表 UNION ALL 过期表 UNION ALL 退保表）左连
drop table nlp_dev.tdl_af_insur_list_tmp_mobile_id_old;
CREATE TABLE nlp_dev.tdl_af_insur_list_tmp_mobile_id_old AS
SELECT
    a.mobile_id,
    a.app_name,
    a.suspected_app_name,    
    a.the_date AS start_time,
    b.t AS end_time,
    CASE
        WHEN b.mobile_id IS NOT NULL THEN 'A'
        WHEN b.mobile_id IS NULL THEN 'B'
    END AS status
FROM
    (
        -- 从投保表中全表筛选出重疾险种名单，保留时间分区字段用于判断2年的存续条件
        SELECT
            mobile_id,
            app_name,
            suspected_app_name,
            the_date
        FROM
            dwb.insurance_txt_apply
        WHERE
            the_date REGEXP '2019|2020|2021'
            AND product_name REGEXP '重疾|重大疾病' 
    ) a
    LEFT JOIN (
        -- 从理赔表的全表中筛选出重疾险种名单，保留时间分区字段，结合投保表中的时间字段用于判断2年的存续条件
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


-- 统计定义为'0'的人群分布(剔除没有2年表现期的数据)：
-- 拥有两年表现期的数据，即2019-07-01至2019-12-16，两年内未发生过过期、退保、理赔的去重人数：
SELECT
    COUNT(mobile_id),
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_mobile_id_old
WHERE 
    the_date <= DATE_SUB('2021-12-05', 720)  --剔除达不2年表现期的数据
    and status = 'B';


-- 2019-07-01至2019-12-16期间投保的人员中，在投保两年后发生过期、退保、理赔的去重人数
SELECT
    COUNT(mobile_id),
    COUNT(DISTINCT mobile_id)
FROM
    nlp_dev.tdl_af_insur_list_tmp_mobile_id_old
WHERE
    end_time > DATE_ADD(start_time, 720); -- (不包括临界点)
    and status = 'A';


-- 剔除没有两年表现期的数据后，定义为0的数据：


