-- 代码测试通过
-- 市场主体动态信息——企业成立信息更新

insert overwrite table dws_ent.enterprise_large_screen_new_enterprise partition (dt)
select
    entname,
    shxydm,
    esdate,
    entstatus,
    dom,
    province,
    CURRENT_DATE as dt
from ds_ent.ds_bus_register_info_fdt
-- 过滤一些错误数据
where ESDATE <= CURRENT_DATE
order by 
    ESDATE desc
limit 100
;

-- 市场主体动态信息——个体户成立信息更新
insert overwrite table dws_ent.enterprise_large_screen_new_individual partition (dt)
select
    entname,
    uniscid,
    esdate,
    entstatus,
    dom,
    province,
    CURRENT_DATE as dt
from ds_ent.ds_bus_individual_info_fdt
-- 过滤一些错误数据
where ESDATE <= CURRENT_DATE
order by 
    ESDATE desc
limit 100
;

-- 市场主体动态信息——企业变更信息更新
insert overwrite table dws_ent.enterprise_large_screen_enterprise_change partition (dt)
select
    entname,
    altitem,
    altbe,
    altaf,
    altdate,
    CURRENT_DATE as dt
from ds_ent.ds_bus_register_change_info_fdt
-- 过滤一些错误数据
where ALTDATE <= CURRENT_DATE
order by 
    ALTDATE desc
limit 100
;

-- 市场主体动态信息——个体户变更信息更新
insert overwrite table dws_ent.enterprise_large_screen_individual_change partition (dt)
select
    entname,
    uniscid,
    altitem,
    altbe,
    altaf,
    altdate,
    CURRENT_DATE as dt
from ds_ent.ds_bus_individual_change_info_fdt
-- 过滤一些错误数据
where ALTDATE <= CURRENT_DATE
order by 
    ALTDATE desc
limit 100
;
