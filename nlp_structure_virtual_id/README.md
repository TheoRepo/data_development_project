<center><b><font size=20>规则结构化项目</font></b></center>

## 项目说明
+ 项目描述
    + 本项目实现了原始数据对虚拟ID项目进行实体抽取的工作

## 数据流拓扑图及数据⾎缘
[config:source_table(数据源底层表)] -->> [config:target_table(规则实体结构化目标表)]

## 库表说明
+ 输入库表说明：预处理加工结果表
+ 输出库表说明：规则实体结构化目标表（字段说明详见建表语句）

## 部署说明
+ 项目结构
```angular2
nlp_structure_virtual_id
│
├─config # 配置信息备份模块,包含核心路径配置、分类映射内容配置等
│      config.toml # 生产环境配置项
│      rule_ner_info.txt  # 规则内容文件
│
├─lib # 结构化依赖的模块,包含一些辅助函数与包
│      spark_base_connector.py
│      __init__.py
│
├─src # 结构化执行的模块,主要逻辑为输出每条记录命中的规则情况
│        data_extractor.py
│        data_extractor.sh
│        export_sql_table.py
│        extractor.py
│        __init__.py
│create_table.sh # 生成Hive建表语句
│run.sh # 执行规则实体结构化主程序
│run_whole_date.sh # 全量历史数据跑数
│run_random_date.sh # 随机抽样跑数
│README.md # 说明文档
```
+ 依赖版本
```angular2
python 3.7.4
    pyspark 3.2.0
    toml 0.10.2
```
+ 部署流程
    + 基于config.toml文件执行如下命令生成建表语句并完成表创建
    ```
    sh create_table.sh --config ./config/config.toml --output create_table.sql
    ```
    + 基于生产账号和资源设置调整./src/data_extractor.sh
    + 执行如下命令完成某一天的跑数工作
    ```
    # file_no为all,启用批量模式,the_date为正则匹配
    # file_no不为all如merge_20200304_23807_L0,启用单批次模式,the_date为具体日期
    sh run.sh --config ./config/config.toml --the_date 2021-10-01 --file_no merge_20211001_0123_L0
    ```
    + 执行如下命令完成历史补数(注意前开后闭,例子中的2021-11-01是不进行跑数的)
    sh run_whole_date.sh 2021-09-01 2021-11-01
    nohup sh run_whole_date.sh 2021-09-01 2021-11-01 > output.log 2>&1 &
    + 执行如下命令完成随机抽样(模型处理10日的原始数据用时12小时左右)
    sh run_random_date.sh 30 2021-01-01 2022-01-01
    nohup sh run_random_date.sh 10 2021-01-01 2022-01-01 > output.log 2>&1 &

+ 注意事项
    + 如果shell脚本在win下压缩后存在doc与unix不兼容问题,在Linux下将run.sh与data_extractor.sh格式转换,进入命令行操作模式:set ff=unix