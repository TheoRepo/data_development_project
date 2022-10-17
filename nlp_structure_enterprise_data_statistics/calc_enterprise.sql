-- 计划把个体户的所有统计结果写到一张表
-- 把企业的所有统计结果写到另一张表

-- 企业测试通过,结果表数据有问题，在找原因
insert overwrite table dwb_ent.enterprise_large_screen_enterprise_indicators partition (dt)
select
    block,
    indicator,
    cnt,
    CURRENT_DATE as dt
from
    (
    -- 总数统计
    -- 总数量（？？）
    select
        '总数统计' as block,
        '总数量' as indicator,
        count(distinct SHXYDM) as cnt
    from 
        ds_ent.ds_bus_register_info_fdt
    UNION ALL
    -- 近七天新增（？？）
    select
        '总数统计' as block,
        '近七天新增' as indicator,
        count(distinct SHXYDM) as cnt
    from 
        ds_ent.ds_bus_register_info_fdt
    where 
        ESDATE > date_sub(CURRENT_DATE,7)
    UNION ALL
    -- 类型分布（？？）
    select 
        '类型分布' as block,
        entity_type as indicator,
        count(distinct SHXYDM) as cnt
    from
        (
        -- 清洗脏数据
        select 
            SHXYDM,
            regexp_replace(ENTTYPE, '\\([\\d]+\\)|\\(\\)', '') as entity_type
        from 
            ds_ent.ds_bus_register_info_fdt
        ) a
    group by
        entity_type
    order by
        entity_type
    UNION ALL
    -- 省份分布（？？）
    select
        '省份分布' as block,
        PROVINCE as indicator,
        count(distinct SHXYDM) as cnt
    from 
        (
        select 
            -- 脏数据清洗
            SHXYDM,
            case 
                when PROVINCE regexp 100000 then '北京市'
                else PROVINCE
            end as province
        from 
            ds_ent.ds_bus_register_info_fdt
        ) tmp
    group by
        province
    order by 
        province
    UNION ALL
    -- 时间分布
    select
        '时间分布' as block, 
        establish_time as indicator,
        count(distinct SHXYDM) as cnt
    from
        (
        -- 计算成立时间
        select
            SHXYDM,
            -- 数据类型转换成string
            CAST(FLOOR(datediff(CURRENT_DATE,ESDATE)/365) as string) as establish_time
        from 
            ds_ent.ds_bus_register_info_fdt
        ) tmp
    where
        establish_time >= 0
        and establish_time <= 122
    group by
        establish_time
    order by 
        establish_time
    UNION ALL
    -- 经营状态分布（？？）
    select
        '经营状态分布' as block,
        ENTSTATUS as indicator,
        count(distinct SHXYDM) as cnt
    from 
        ds_ent.ds_bus_register_info_fdt
    group by
        ENTSTATUS
    order by 
        ENTSTATUS
    UNION ALL
    -- 资本分布
    select
        '资本分布' as block,
        capital as indicator,
        count(distinct SHXYDM) as cnt
    from 
        (
        -- 计算资本
        select
            SHXYDM,
            case 
                when REGCAP is not null then cast(FLOOR(REGCAP/10000) as string)
                else cast(REGCAP as string)
            end as capital
        from
            ds_ent.ds_bus_register_info_fdt
        ) tmp
    group by
        capital
    order by 
        capital
    UNION ALL
    -- 行业分布
    select
        '行业分布' as block,
        INDUSTRY as indicator,
        count(distinct SHXYDM) as cnt
    from 
        ds_ent.ds_bus_register_info_fdt
    group by
        INDUSTRY
    order by 
        INDUSTRY
    ) tmp
-- 汇总加过滤条件
where 
    indicator != ''
    and indicator != '无'
    and indicator is not null 
order by
    block, indicator
;
