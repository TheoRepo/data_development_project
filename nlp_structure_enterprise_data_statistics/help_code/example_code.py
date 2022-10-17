import json
import sys
import os
import re
import subprocess

the_date = sys.argv[1]

# 自定义函数
def upper_deal(l):
    ln = ""
    if l=="getChattelMortgageDetaiZXlInfo":
        l = "getChattelMortgageDetailZXInfo"
    if l=="getIndividualAnnreportList":
        l = "getIndividualAnnReportList"
    if l == "getEntSFCAnnreportList":
        l = "getEntSFCAnnReportList"
    l_list_o = list(l[3:])
    l_list = []
    for i in range(len(l_list_o)):
        if l_list_o[i-1].isupper() and l_list_o[i+1].isupper():
            l_list.append(l_list_o[i].lower())
        else:
            l_list.append(l_list_o[i])
    for i in range(len(l_list)):
        if l_list[i].isupper() and l_list[i-1].islower():
            ln += "_"+l_list[i].lower()
        else:
            ln += l_list[i]
    ln = ln[1:].lower()
    return ln

def rule_func(match):
    return match.group()[1:-1].replace(",","，")

if not os.path.exists("./"+the_date):
    os.mkdir("./"+the_date) 

with open("table_dict.json") as f:
    table_dict = json.load(f)
if os.path.exists(the_date+"/loaded_files.txt"):
    with open(the_date+"/loaded_files.txt") as f:
        worked_files = f.read()
    worked_files = worked_files.strip().split("\n")
else:
    worked_files=[]
    
path = r"/data1/NECI/daily"
first_dir_list = os.listdir(path)


create_temp_table = []
load_data = []
to_orctable = []
loaded_files = []
statistics_table_result = []
statistics_file_result = []


for d in first_dir_list:
    d_new = upper_deal(d.replace("_001",""))
    if the_date in os.listdir(os.path.join(path,d)):
        for file_name in os.listdir(os.path.join(path,d,the_date)):
            if file_name.endswith(".txt") and file_name+".done" in os.listdir(os.path.join(path,d,the_date)) and file_name not in worked_files:
                num = 0
                # 数据处理
                with open("/data1/data_handling/"+file_name, "w") as f1:
                    with open(os.path.join(path,d,the_date,file_name),"r",encoding='utf-8') as f2:
                        try:
                            if f2.readline().strip()!=",".join(table_dict[d_new]["structure"]):
                                raise Exception("{0}-{1}: 表结构与数据结构不匹配".format(the_date, d_new))
                            for da in f2:
                                da = re.sub("\"(.*?)\"",rule_func,da)
                                f1.write(da)
                                num+=1
                        except:
                            print("未知文件："+file_name)
                            continue
                # 建立临时表
                create_temp_table.append(table_dict[d_new]["create_table"])
                # 将数据导入临时表
                load_data.append("hdfs dfs -put {0} hdfs://sparkcm5/user/hive/warehouse/ds_ent.db/tdl_ds_bus_{1}_fdt\n".format("/data1/data_handling/"+file_name,d_new))
                # 将数据从临时表导入分区表
                to_orctable.append("INSERT OVERWRITE TABLE ds_ent.ds_bus_{0}_fdt partition(dt='{1}') SELECT * FROM ds_ent.tdl_ds_bus_{0}_fdt;".format(d_new,the_date))
                # 统计入表数据量，并存入统计表中
                statistics_table_result.append("INSERT INTO TABLE ds_ent.ds_bus_enterprise_data_statistics_fdt partition(dt='{1}') select 'ds_ent.ds_bus_{0}_fdt' as table_name,'入库' as stage,'数据量' as statistics_content,count(1) as result from ds_ent.ds_bus_{0}_fdt where dt='{1}';".format(d_new,the_date))
                # 统计原文件数据量
                statistics_file_result.append("INSERT INTO TABLE ds_ent.ds_bus_enterprise_data_statistics_fdt partition(dt='{1}') values ('{0}','源文件','数据量','{2}');".format(file_name,the_date,num))
                loaded_files.append(file_name)

beeline_str1 = "beeline -u \"jdbc:hive2://spark-coprocessor-010050010012-bigdata-cm5.spark.com:2181,spark-coprocessor-010050010013-bigdata-cm5.spark.com:2181,spark-coprocessor-010050010014-bigdata-cm5.spark.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -e \"{}\"\n"                
beeline_str = """
/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-sql --driver-memory 4g \
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
    --name \"ds_ent_ahao\" \
    -e \"{}\"\n
"""


if loaded_files:               
    with open(the_date+"/run_create_tmp.sh","w") as f:
        create_tmp_table_str = "\n".join(create_temp_table)
        f.write(beeline_str1.format(create_tmp_table_str))
    with open(the_date+"/run_load_hdfs.sh","w") as f:
        for d in load_data:
            f.write(d+"\n")
    with open(the_date+"/run_to_orctable.sh","w") as f:
        to_orctable_str = "\n".join(to_orctable)
        f.write(beeline_str.format(to_orctable_str))
    with open(the_date+"/run_statistics_table_result.sh","w") as f:
        statistics_table_result_str = "\n".join(statistics_table_result)
        f.write(beeline_str.format(statistics_table_result_str))
    with open(the_date+"/run_statistics_file_result.sh","w") as f:
        statistics_file_result_str = "\n".join(statistics_file_result)
        f.write(beeline_str.format(statistics_file_result_str))
    
    result1 = subprocess.call(["sh", the_date+"/run_create_tmp.sh"])
    if result1 != 0:
        raise ValueError("建立临时表失败")
    result2 = subprocess.call(["sh", the_date+"/run_load_hdfs.sh"])
    if result2 != 0:
        raise ValueError("数据导入临时表失败")
    for fi in os.listdir("/data1/data_handling/"):
        os.remove("/data1/data_handling/"+fi)
    result3 = subprocess.call(["sh", the_date+"/run_to_orctable.sh"])
    if result3 != 0:
        raise ValueError("临时表数据转到orc表，执行失败")
    result4 = subprocess.call(["sh", the_date+"/run_statistics_table_result.sh"])
    if result4 != 0:
        raise ValueError("统计存入表中的每日数据量，执行失败")
    result5 = subprocess.call(["sh", the_date+"/run_statistics_file_result.sh"])
    if result5 != 0:
        raise ValueError("统计源数据的每日数据量，执行失败") 
    
    with open(the_date+"/loaded_files.txt","a+") as f:
        for name in loaded_files:
            f.write(name+"\n") 
