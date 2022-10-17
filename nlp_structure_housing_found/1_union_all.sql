-- 需求：确认一下公积金事件库的覆盖率，维度（是否缴存，缴存基数，缴存比例）  
-- 统计思路：将所有结果表UNION ALL，然后根据相关字段（地域字段，行为字段，msg字段）的正则匹配，做行地域维度，行为维度，个人/公司维度的统计




-- 第一步
-- 地域映射字典表的加工处理：省-市-县/区映射表
DROP TABLE IF EXISTS nlp_dev.dim_province_city_county_v2;
CREATE TABLE IF NOT EXISTS nlp_dev.dim_province_city_county_v2 
(
    province_name STRING COMMENT '省份名',
    city_name STRING COMMENT '城市名',
    county_name STRING COMMENT '县/区名'
) COMMENT '省-市-县/区映射表'
STORED AS
    orc
lifecycle
    365
;

with 
a as(
SELECT  area.parent_area_name,area.area_name FROM nlp_dev.area where area.level='2'
),
b as(
SELECT  area.parent_area_name,area.area_name FROM nlp_dev.area where area.level='3'
)
insert OVERWRITE table nlp_dev.dim_province_city_county_v2 
SELECT
    a.parent_area_name as province_name,
    a.area_name as city_name,
    b.area_name as county_name
FROM a
join b
on a.area_name=b.parent_area_name
order by province_name,city_name
;




-- 第二步
-- 创建中间表: 公积金地域维度统计分析基础表
DROP TABLE IF EXISTS nlp_dev.housing_found_txt_area_online_v2;
CREATE TABLE IF NOT EXISTS nlp_dev.housing_found_txt_area_online_v2 (
    mobile_id STRING COMMENT 'mobile_id',
    tag STRING COMMENT '记录来源的标志', -- account_manage,change_base,deposit,salary,withdraw,loan
    msg STRING COMMENT '文本信息',
    city_name STRING COMMENT '城市名',
    province_name STRING COMMENT '省份/直辖市/特别行政区/自治区名'
) COMMENT '公积金地域维度统计分析基础表'
STORED AS
    orc
lifecycle
    365
;

INSERT
    OVERWRITE TABLE nlp_dev.housing_found_txt_area_online_v2
SELECT
    mobile_id,
    tag,
    msg,
    c.city_name,
    CASE
        WHEN tmp.city_name IS NOT NULL THEN substr(tmp.province_name, 1, 2)
        WHEN tmp.city_name IS NULL THEN (
            CASE
                WHEN c.app_name REGEXP '北京|天津|上海|重庆|内蒙|广西|西藏|宁夏|新疆|河北|山西|辽宁|吉林|黑龙|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海' THEN substr(c.app_name, 1, 2)
                WHEN c.suspected_app_name REGEXP '北京|天津|上海|重庆|内蒙|广西|西藏|宁夏|新疆|河北|山西|辽宁|吉林|黑龙|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海' THEN substr(c.suspected_app_name, 1, 2)
                WHEN c.funds_center REGEXP '北京|天津|上海|重庆|内蒙|广西|西藏|宁夏|新疆|河北|山西|辽宁|吉林|黑龙|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海' THEN substr(c.funds_center, 1, 2)
                ELSE ''
            END
        )
        ELSE NULL
    END AS province_name
FROM
    (
        SELECT
            mobile_id,
            app_name,
            suspected_app_name,
            funds_center,
            tag,
            city_name,
            msg
        FROM
            (
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'corporate' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_corporate_v2 -- 公积金_对公
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'base_adjustment' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_base_adjustment_v2 -- 公积金_基数调整
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'withdraw' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_withdraw_v2 -- 公积金_提取
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'deposit' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_deposit_v2 -- 公积金_个人缴存
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    corporate_name as funds_center,
                    'paystub' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_paystub_v2 -- 公积金_工资
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'loan_repayment' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_loan_repayment_v2 -- 公积金_贷款_还款
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'loan_release' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_loan_release_v2 -- 公积金_贷款_发放
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'loan_overdue' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_loan_overdue_v2 -- 公积金_贷款_逾期
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'loan_repayment' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_loan_repayment_reminder_v2 -- 公积金_贷款_还款提醒
                WHERE
                    the_date REGEXP '2021'
                UNION ALL
                SELECT
                    mobile_id,
                    app_name,
                    suspected_app_name,
                    funds_center,
                    'loan_approval' as tag,
                    msg,
                    substr(suspected_app_name, 1, 2) AS city_name
                FROM
                    nlp_online.housing_found_txt_loan_approval_v2 -- 公积金_贷款_审批
                WHERE
                    the_date REGEXP '2021'
            ) a 
    ) c
    LEFT JOIN (
        SELECT
            DISTINCT province_name,
            substr(city_name, 1, 2) city_name
        FROM
            nlp_dev.dim_province_city_county_v2
        WHERE 
            substr(city_name,1,2) NOT REGEXP '省直|张家'
    ) tmp 
    ON c.city_name = tmp.city_name;




-- 第三步
-- 创建中间表: 公积金地域/行为/公私维度统计分析基础表
DROP TABLE IF EXISTS nlp_dev.housing_found_txt_area_action_type_online_v2;
CREATE TABLE IF NOT EXISTS nlp_dev.housing_found_txt_area_action_type_online_v2 (
    mobile_id STRING COMMENT 'mobile_id',
    city_name STRING COMMENT '城市名',
    province_name STRING COMMENT '省份/直辖市/特别行政区/自治区名',
    act_type STRING COMMENT '行为',
    type STRING COMMENT '公/私'
) COMMENT '公积金地域/行为/公私维度统计分析基础表'
STORED AS
    orc
lifecycle
    365
;


INSERT
    OVERWRITE TABLE nlp_dev.housing_found_txt_area_action_type_online_v2
SELECT
    mobile_id,
    city_name,
    province_name,
    CASE 
        WHEN msg REGEXP '(缴纳|缴存|补缴|(补)缴|汇缴|存入|缴款|缴入|缴交|代扣|缴至|扣缴)((登入|总|金)?额|公积金)?为?[0-9]+(\.[0-9]+)?元?|办理(缴纳缴存|补缴|汇缴).*[0-9]+(\.[0-9]+)?元?'
            AND msg not REGEXP '贷款|放款|还款|扣款|逾期|还贷|租赁提取待发送短信|未能按时扣划缴存|按时足额缴存公积金|存金额入账|您.*应缴纳.*将于.*代扣|账户.*于.*应按月扣划缴存.*失败|请您及时.*(汇缴|缴纳|缴存)+.*以免影响|您.*公积金缴存额为[0-9]+元.将于.*扣缴,请.*存入|将于.*代扣.*住房公积金.*[0-9]+元|补缴材料|情况说明|调为|调整|申请不通过|手续|对账单|不符合(规定|.*公积金的条件|租房(提取|支取)+)+|资料不全|不予受理|(未满足|需|需满足|需要|需要满足)+(连续|正常|足额)+(缴存|缴纳)+[0-9]+个月|提取未能成功|办理失败|未能通过审核|已成功办理(住房)?公积金(提取|支取)+|退缴|停缴|决定书|投诉|【.*】(剩余)?公积金账户余额[0-9]+元'
            then '缴存' 
        WHEN msg  REGEXP '未能按时扣划缴存|按时足额缴存公积金|请您及时.*(汇缴|缴纳|缴存)+.*以免影响|账户.*于.*应按月扣划缴存.*失败|您.*应缴纳.*将于.*代扣|您.*公积金缴存额为[0-9]+元.将于.*扣缴,请.*存入|将于.*代扣.*住房公积金.*[0-9]+元'
            then '缴存提醒'
        WHEN tag regexp 'loan' AND msg not REGEXP  '(缴纳|缴存|补缴|(补)缴|汇缴|存入|缴款|缴入|缴交|代扣|缴至|扣缴)((登入|总|金)?额|公积金)?为?[0-9]+(\.[0-9]+)?元?|办理(缴纳缴存|补缴|汇缴).*[0-9]+(\.[0-9]+)?元?|租赁提取待发送短信|未能按时扣划缴存|按时足额缴存公积金|存金额入账|您.*应缴纳.*将于.*代扣|账户.*于.*应按月扣划缴存.*失败|请您及时.*(汇缴|缴纳|缴存)+.*以免影响|补缴材料|情况说明|调为|调整|申请不通过|手续|对账单|不符合(规定|.*公积金的条件|租房(提取|支取)+)+|资料不全|不予受理|(未满足|需|需满足|需要|需要满足)+(连续|正常|足额)+(缴存|缴纳)+[0-9]+个月|提取未能成功|办理失败|未能通过审核|已成功办理(住房)?公积金(提取|支取)+|退缴|停缴|决定书|投诉|【.*】(剩余)?公积金账户余额[0-9]+元'
            then '贷款'
        ELSE '其它'
    END as act_type,
    CASE 
        WHEN msg REGEXP '(单位|公司|企业|机构)(您好)?.*您于.*办理.*业务|(单位|公司|企业|机构).*(汇缴)(资金|金额)已于[0-9]+成功入账.(汇缴金额)为[0-9]+(\.[0-9]+)?元?|(单位|公司|企业|机构)申请.*(启封|封存)?已于.*审核(通过)?|(单位|公司|企业|机构).*(缴存|汇缴|托收|缴纳|缴交|缴入).*公积金[0-9]+(\.[0-9]+)?元?(已成功到账)?|(单位|公司|企业|机构)账户已开设|(单位|公司|企业|机构).*开设公积金账户'
            AND msg not REGEXP '职工|专管员|缴存人|缴纳人|(..|...)您好:(单位|公司|企业|机构).*为您存入公积[0-9]+(\.[0-9]+)?元?'
            then '对公'
        ELSE '个人'
    END as type
FROM
    nlp_dev.housing_found_txt_area_online_v2
;