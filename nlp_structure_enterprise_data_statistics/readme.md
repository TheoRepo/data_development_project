# 企业大屏数据统计

## 跑数命令
```bash
# 查看错误日志
nohup python dataflow.py > dataflow.log 2>&1 &
```

## 任务调度
1. 检查crontab服务是否启动
```bash
service crond status
```

2. 修改用户的配置文件
用`crontab -e`命令在最后添加一行
```bash
00 02 * * * sh /home/ds/qianyu/nlp_structure_enterprise_data_statistics/run.sh
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

