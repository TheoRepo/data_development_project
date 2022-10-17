<center><b><font size=20>规则分类结构化项目</font></b></center>

## 项目说明
+ 项目描述
    + 本项目实现了原始数据进行规则分类的工作
+ 业务方
    + 智慧金融一部
+ 数据更新周期
    + 随上游结果实时更新
+ 实时性要求
    + 上游数据更新后2个小时内完成

## 数据流拓扑图及数据⾎缘
[config:source_table(数据源底层表)] -->> [config:target_table(规则分类结构化目标表)]

## 库表说明
+ 输入库表说明：预处理加工结果表
+ 输出库表说明：规则分类结构化目标表（字段说明详见建表语句）

## 部署说明
+ 项目结构
```angular2
nlp_structure_rule_classify
│
├─config # 配置信息备份模块,包含核心路径配置、分类映射内容配置等
│      config.toml # 生产环境配置项
│      config_dev.toml # 开发环境配置项
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
│run.sh # 执行规则分类结构化主程序
│README.md # 说明文档
```
+ 依赖版本
```angular2
python 3.7.4
    pyspark 3.2.0
    toml 0.10.2
```
+ 部署流程
    + 基于生产环境情况修改config.toml文件
    + 基于config.toml文件执行如下命令生成建表语句并完成表创建
    ```
    sh create_table.sh --config ./config/config_dev.toml --output create_table.sql
    ```
    + 基于生产账号和资源设置调整./src/data_extractor.sh
    + 执行如下命令完成跑数工作
    ```
    # file_no为all,启用批量模式,the_date为正则匹配
    # file_no不为all如merge_20200304_23807_L0,启用单批次模式,the_date为具体日期
    sh run.sh --config ./config/config_dev.toml --the_date 2021-11-04 --file_no merge_20211104_8427_L0
    ```
+ 注意事项
    + 如果shell脚本在win下压缩后存在doc与unix不兼容问题,在Linux下将run.sh与data_extractor.sh格式转换,进入命令行操作模式:set ff=unix