CREATE TABLE IF NOT EXISTS dwb_ent.enterprise_large_screen_enterprise_indicators
(
    block String COMMENT '板块',
    indicator String COMMENT '统计指标',
    cnt int COMMENT '统计结果'
)   comment '企业统计结果'
partitioned by
(
    dt String COMMENT '跑数时间'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orcfile;


CREATE TABLE IF NOT EXISTS dwb_ent.enterprise_large_screen_individual_indicators
(
    entity_type String COMMENT '统计主体',
    indicator String COMMENT '统计指标',
    cnt int COMMENT '统计结果'
)   comment '个体户统计结果'
partitioned by
(
    dt String COMMENT '跑数时间'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orcfile;


CREATE TABLE IF NOT EXISTS dwb_ent.enterprise_large_screen_new_enterprise
(
    entname String COMMENT '企业名称',
    shxydm String COMMENT '统一社会信用代码',
    esdate String COMMENT '成立日期',
    entstatus String COMMENT '经营状态',
    dom String COMMENT '地址',
    province String COMMENT '省份'
)   comment '市场主体动态信息——企业成立信息更新'
partitioned by
(
    dt String COMMENT '跑数时间'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orcfile;


CREATE TABLE IF NOT EXISTS dwb_ent.enterprise_large_screen_new_individual
(
    entname String COMMENT '个体户名称',
    uniscid String COMMENT '统一社会信用代码',
    esdate String COMMENT '成立日期',
    entstatus String COMMENT '经营状态',
    dom String COMMENT '地址',
    province String COMMENT '省份'
)   comment '市场主体动态信息——个体户成立信息更新'
partitioned by
(
    dt String COMMENT '跑数时间'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orcfile;


CREATE TABLE IF NOT EXISTS dwb_ent.enterprise_large_screen_enterprise_change
(
    entname String COMMENT '企业名称',
    altitem String COMMENT '变更事项',
    altbe String COMMENT '变更前内容',
    altaf String COMMENT '变更后内容',
    altdate String COMMENT '变更时间'
)   comment '市场主体动态信息——企业变更信息更新'
partitioned by
(
    dt String COMMENT '跑数时间'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orcfile;


CREATE TABLE IF NOT EXISTS dwb_ent.enterprise_large_screen_individual_change
(
    entname String COMMENT '个体户名称',
    uniscid String COMMENT '统一社会信用代码',
    altitem String COMMENT '变更事项',
    altbe String COMMENT '变更前内容',
    altaf String COMMENT '变更后内容',
    altdate String COMMENT '变更日期'
)   comment '市场主体动态信息——个体户变更信息更新'
partitioned by
(
    dt String COMMENT '跑数时间'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orcfile;