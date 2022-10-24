import json
import sys
import os
import re
import subprocess


# 调用beeline,空格的地方直接存放sql
beeline_str = "beeline -u \"jdbc:hive2://spark-coprocessor-010050010012-bigdata-cm5.spark.com:2181,spark-coprocessor-010050010013-bigdata-cm5.spark.com:2181,spark-coprocessor-010050010014-bigdata-cm5.spark.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -e \"{}\"\n"                

# 调用spark_sql,空格的地方直接存放sql
spark_sql_str = """/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-sql --driver-memory 4g \
    --executor-memory 6g \
    --executor-cores 2 \
    --conf spark.yarn.executor.memoryOverhead=4g \
    --conf spark.driver.memoryOverhead=1g \
    --conf spark.sql.autoBroadcastJionThreshold=500485760 \
    --conf spark.network.timeout=800000 \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.rpc.message.maxSize=500 \
    --conf spark.rpc.askTimeout=600 \
    --conf spark.executor.heartbeatInterval=60000 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=200 \
    --conf spark.dynamicAllocation.executorIdleTimeout=100s \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=300s \
    --conf spark.scheduler.mode=FAIR \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=2s \
    --conf spark.default.parallelism=400 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.broadcastTimeout=1800 \
    --conf spark.maxRemoteBlockSizeFetchToMem=512m \
    --name \"dws_ent_qianyu\" \
    -e \"{}\"\n
"""

def one_line_sql_transformer(str):
    # 删除注释
    note_deleter = re.compile(r'\-\-.*?\n')
    _sql_tmp_1 = note_deleter.sub('', str)
    # 去掉换行符
    tab_deleter = re.compile(r'\n')
    _sql_tmp_2 = tab_deleter.sub(' ', _sql_tmp_1)
    # 去掉多余的空格
    space_deleter = re.compile(r'\s+')
    _sql_tmp_3 = space_deleter.sub(' ', _sql_tmp_2)
    # 将两个转义符替换成四个
    _sql = _sql_tmp_3.replace(r"\\", r"\\\\" )
    return _sql

def sql_to_shell(sql_file,shell_file,command_line):
    # 读取建表SQL，建立建表的shell脚本
    with open(sql_file,"r") as f1:
        _sql = ''
        create_tmp_table_str = f1.readlines() # 每一行是列表的一个元素
        sql_string = "".join(create_tmp_table_str) # 将列表转化成string
        for i in sql_string.split(';')[:-1]:
            if i[-1] != '\n':
                i = i+'\n'
            one_line_sql = one_line_sql_transformer(i)
            # 忽略注释
            if one_line_sql == '' or one_line_sql == ' ':
                continue
            else:
                _sql = _sql + one_line_sql + '; '
        with open(shell_file,"w") as f2:
            print(_sql)
            f2.write(command_line.format(_sql))
            print('%s文件成功生成' %shell_file)


if __name__ =="__main__":

    sql_to_shell("create_table.sql","create_table.sh", beeline_str)
    sql_to_shell("calc_individual.sql","calc_individual.sh", beeline_str)
    sql_to_shell("calc_enterprise.sql","calc_enterprise.sh", beeline_str)
    sql_to_shell("info_update.sql","info_update.sh", beeline_str)


    # 执行建表命令
    result1 = subprocess.call(["sh", "create_table.sh"])
    if result1 != 0:
        raise ValueError("建表失败")

    # 执行写数命令
    result2 = subprocess.call(["sh", "calc_individual.sh"])
    if result2 != 0:
        raise ValueError("写数失败")

    result3 = subprocess.call(["sh", "calc_enterprise.sh"])
    if result3 != 0:
        raise ValueError("写数失败")

    result4 = subprocess.call(["sh", "info_update.sh"])
    if result4 != 0:
        raise ValueError("写数失败")
