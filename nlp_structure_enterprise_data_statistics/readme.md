# 企业大屏数据统计

## 跑数命令
```bash
# 查看错误日志
nohup python dataflow.py > dataflow.log 2>&1 &
```

## 开发流程
1. 在dataworks或者beeline上完成sql开发
2. 将sql代码保存在代码仓库的主路径，使用dataflow.py脚本驱动sql运行

## 核验流程
1. 查看日志，数据流运行成功
2. 查看结果表，有结果数据，且字段齐全
3. 抽样指标，计算结果正确

## 结果表
详细信息请参考table_structure文件夹下的

- 企业数据大屏原始统计数据（表结构设计）.pdf
- 企业数据大屏原始统计数据（枚举值明细）.xlsx

## 优化方向
目前使用beeline的API，运行SQL
后续可以尝试使用spark_sql的API
