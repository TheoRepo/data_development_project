-- 统计枚举值
-- block
select distinct block from 
(
select 
    block 
from 
    dws_ent.enterprise_large_screen_enterprise_indicators
UNION ALL
select 
    block 
from 
    dws_ent.enterprise_large_screen_individual_indicators
) tmp;

-- indicator(省份)
select distinct indicator from 
(
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_enterprise_indicators
where block = '省份分布'
UNION ALL
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_individual_indicators
where block = '省份分布'
) tmp;


-- indicator(经营状态分布)
select distinct indicator from 
(
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_enterprise_indicators
where block = '经营状态分布'
UNION ALL
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_individual_indicators
where block = '经营状态分布'
) tmp;


-- indicator(类型分布)
select distinct indicator from 
(
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_enterprise_indicators
where block = '类型分布'
UNION ALL
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_individual_indicators
where block = '类型分布'
) tmp;


-- indicator(行业分布)
select distinct indicator from 
(
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_enterprise_indicators
where block = '行业分布'
UNION ALL
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_individual_indicators
where block = '行业分布'
) tmp;



-- 类型分布枚举值有脏数据
--  验证清洗正则没有问题
select
    distinct entity_type
from 
    (
    -- 清洗脏数据
    select 
        SHXYDM,
        regexp_replace(ENTTYPE, '\\([\\d]+\\)', '') as entity_type
    from 
        ds_ent.ds_bus_register_info_fdt
    ) a
;

-- 分区
show partitions dws_ent.enterprise_large_screen_individual_indicators;


select distinct indicator from 
(
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_enterprise_indicators
where block = '类型分布' and dt='2022-10-24'
UNION ALL
select 
    indicator 
from 
    dws_ent.enterprise_large_screen_individual_indicators
where block = '类型分布' and dt='2022-10-24'
) tmp;







select 
    distinct entity_type
from
    (
    select 
        distinct regexp_replace(ENTTYPE, '\\([\\d]+\\)', '') as entity_type 
    from 
        ds_ent.ds_bus_register_info_fdt
    UNION ALL
    select 
        distinct regexp_replace(ENTTYPE, '\\([\\d]+\\)', '') as entity_type 
    from 
        ds_ent.ds_bus_register_info_fdt
    ) tmp
;

-- 实验
select regexp_replace('有限责任公司(国有独资)(1110)', '\\([\\d]+\\)', '');
select regexp_replace('有限责任公司分公司(自然人投资或控股)()', '\\([\\d]+\\)', '');
select regexp_replace('有限责任公司(自然人投资或控股)(1130)', '\\([\\d]+\\)', '');



-- 市场主体省份类型统计
select 
    distinct province
from
    (
    select distinct PROVINCE as province from ds_ent.ds_bus_register_info_fdt
    UNION ALL
    select distinct PROVINCE as province from ds_ent.ds_bus_individual_info_fdt
    ) tmp
;

-- 异常值排查
select * from ds_ent.ds_bus_register_info_fdt where province regexp 100000;
select * from ds_ent.ds_bus_individual_info_fdt where province regexp 100000;


-- 市场主体成立时间分布
select distinct ESDATE from ds_ent.ds_bus_register_info_fdt;
-- 成立时间计算
select
    ESDATE,
    FLOOR(datediff(CURRENT_DATE,ESDATE)/365)
from
    ds_ent.ds_bus_register_info_fdt
limit 
    2
;

-- 成立时间枚举值
select 
    distinct establish_time
from 
    (
        select 
            FLOOR(datediff(CURRENT_DATE,ESDATE)/365) as establish_time
        from 
            ds_ent.ds_bus_register_info_fdt
        UNION ALL 
        select 
            FLOOR(datediff(CURRENT_DATE,ESDATE)/365) as establish_time
        from 
            ds_ent.ds_bus_individual_info_fdt
    ) tmp
order by
    establish_time
;

-- 查看异常值
-- 企业
select 
    ESDATE
from 
    ds_ent.ds_bus_register_info_fdt
where 
    FLOOR(datediff(CURRENT_DATE,ESDATE)/365) > 150;
order by
    ESDATE
;

-- 个体户
select 
    *
from 
    ds_ent.ds_bus_individual_info_fdt
where 
    FLOOR(datediff(CURRENT_DATE,ESDATE)/365) >= 0
    and FLOOR(datediff(CURRENT_DATE,ESDATE)/365) < 122  
order by
    ESDATE desc
limit 
    5
;



-- 经营状态分布枚举值
select 
    distinct entity_status
from
    (
    select distinct ENTSTATUS as entity_status from ds_ent.ds_bus_register_info_fdt
    UNION ALL
    select distinct ENTSTATUS as entity_status from ds_ent.ds_bus_individual_info_fdt
    ) tmp
;

-- 资本分布枚举值

select REGCAP from ds_ent.ds_bus_register_info_fdt limit 10;

-- 资本枚举值
select 
    distinct capital
from 
    (
    select 
        case 
            when REGCAP is not null then cast(REGCAP/10000 as int)
            else REGCAP
        end as capital
    from 
        ds_ent.ds_bus_register_info_fdt
    ) tmp
;


-- 取值范围
select 
    max(distinct capital),
    min(distinct capital)
from 
    (
    select 
        case 
            when REGCAP is not null then cast(REGCAP/10000 as int)
            else REGCAP
        end as capital
    from 
        ds_ent.ds_bus_register_info_fdt
    ) tmp
;



-- 行业枚举值（不包含空字符）
select 
    distinct industry
from
    (
    select distinct INDUSTRY as industry from ds_ent.ds_bus_register_info_fdt
    UNION ALL
    select distinct INDUSTRY as industry from ds_ent.ds_bus_individual_info_fdt
    ) tmp
order by
    industry
;




-- 正则替换
select regexp_replace('有限责任公司(国有独资)(1110)', '\\([\\d]+\\)', '');
select regexp_replace('外国(地区)其他形式公司分支机构','\\([\\d]+\\)', '');
select regexp_replace('其他有限责任公司分公司(2190)','\\([\\d]+\\)', '');
select regexp_replace('联营(3500)','\\([\\d]+\\)', '');


-- 脏数据清洗
-- 清洗掉100000，转化成北京市
select
    distinct province
from
    (
    select 
        -- 脏数据清洗
        case 
            when PROVINCE regexp 100000 then '北京市'
            else PROVINCE
        end as province
    from 
        ds_ent.ds_bus_individual_info_fdt
    ) tmp
;
