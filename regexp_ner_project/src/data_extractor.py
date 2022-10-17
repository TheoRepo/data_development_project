import argparse
import json
from logging import raiseExceptions
import os
import re
import subprocess
from datetime import datetime as dt
from datetime import timedelta
from functools import partial
from string import Template

import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from functools import partial
import sys,toml
sys.path.append('..')
from lib.spark_base_connector import BaseSparkConnector


# 配置文件信息
os.environ['PYSPARK_PYTHON'] = "/usr/local/python3.7.4/bin/python3"
ABSPATH = os.path.dirname(os.path.abspath(__file__))  # 将文件所在的路径记为绝对路径


# 读取规则原始文件
rule_info_df = pd.read_csv(os.path.join(ABSPATH, "../config/rule_ner_info.txt"), sep="\t")


# 判断字符串是否全为中文
def is_all_chinese(strs):
    for _char in strs:
        if not '\u4e00' <= _char <= '\u9fa5':
            return False
    return True


# 规则拆解成字典
def rule_info_seperator(rule_info_df):
    rule_info_dict = {}
    rule_id_list = rule_info_df[(rule_info_df['is_active']==1)]['rule_id'].values.tolist()     # 只读取被激活的规则
    rule_info_df_id = rule_info_df.set_index('rule_id')
    for rule_id in rule_id_list:
        rule_info_dict[rule_id] = {
                "level_1_forward_rule_app": rule_info_df_id.loc[rule_id]['level_1_forward_rule_app'],
                "level_1_forward_rule_content":rule_info_df_id.loc[rule_id]['level_1_forward_rule_content'],
                "level_1_backward_rule_content":rule_info_df_id.loc[rule_id]['level_1_backward_rule_content'],
                "extract_rule_content":rule_info_df_id.loc[rule_id]['extract_rule_content'],
                "entity_index":rule_info_df_id.loc[rule_id]['entity_index'],
                "entity_type":rule_info_df_id.loc[rule_id]['entity_type'],
                "is_owner":rule_info_df_id.loc[rule_id]['is_owner'],
                "scene_classification":rule_info_df_id.loc[rule_id]['scene_classification']
        }
    return rule_info_dict, rule_id_list        


def rule_extractor(msg, app_name, suspected_app_name, rule_info_dict, rule_id_list):
    for rule_id in rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)
        # 注意app_name,suspected_app_name的正则规则匹配时,大小写不敏感
        if is_all_chinese(rule_info_part.get("level_1_forward_rule_app")):
            if (app_name == rule_info_part.get("level_1_forward_rule_app") or suspected_app_name == rule_info_part.get("level_1_forward_rule_app")) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    result = re.search(rule_info_part.get("extract_rule_content"), msg)
                    index = rule_info_part.get("entity_index")
                    # print(rule_info_part.get("extract_rule_content"))
                    return result.group(int(index))            
        else:
            if (re.search(rule_info_part.get("level_1_forward_rule_app"), app_name, re.IGNORECASE) or re.search(rule_info_part.get("level_1_forward_rule_app"), suspected_app_name, re.IGNORECASE)) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    result = re.search(rule_info_part.get("extract_rule_content"), msg)
                    index = rule_info_part.get("entity_index")
                    return result.group(int(index))
    return ""


def rule_labeler(msg, app_name, suspected_app_name, rule_info_dict, rule_id_list):
    for rule_id in rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)
        # 注意app_name,suspected_app_name的正则规则匹配时,大小写不敏感
        if is_all_chinese(rule_info_part.get("level_1_forward_rule_app")):
            if (app_name == rule_info_part.get("level_1_forward_rule_app") or suspected_app_name == rule_info_part.get("level_1_forward_rule_app")) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    return rule_info_part.get("entity_type")            
        else:
            if (re.search(rule_info_part.get("level_1_forward_rule_app"), app_name, re.IGNORECASE) or re.search(rule_info_part.get("level_1_forward_rule_app"), suspected_app_name, re.IGNORECASE)) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    return rule_info_part.get("entity_type")
    return ""


def rule_owner(msg, app_name, suspected_app_name, rule_info_dict, rule_id_list):
    for rule_id in rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)
        # 注意app_name,suspected_app_name的正则规则匹配时,大小写不敏感
        if is_all_chinese(rule_info_part.get("level_1_forward_rule_app")):
            if (app_name == rule_info_part.get("level_1_forward_rule_app") or suspected_app_name == rule_info_part.get("level_1_forward_rule_app")) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    return rule_info_part.get("is_owner")         
        else:
            if (re.search(rule_info_part.get("level_1_forward_rule_app"), app_name, re.IGNORECASE) or re.search(rule_info_part.get("level_1_forward_rule_app"), suspected_app_name, re.IGNORECASE)) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    return rule_info_part.get("is_owner")
    return ""


def rule_classifier(msg, app_name, suspected_app_name, rule_info_dict, rule_id_list):
    for rule_id in rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)
        # 注意app_name,suspected_app_name的正则规则匹配时,大小写不敏感
        if is_all_chinese(rule_info_part.get("level_1_forward_rule_app")):
            if (app_name == rule_info_part.get("level_1_forward_rule_app") or suspected_app_name == rule_info_part.get("level_1_forward_rule_app")) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    return rule_info_part.get("scene_classification") 
        else:
            if (re.search(rule_info_part.get("level_1_forward_rule_app"), app_name, re.IGNORECASE) or re.search(rule_info_part.get("level_1_forward_rule_app"), suspected_app_name, re.IGNORECASE)) \
                    and re.search(rule_info_part.get("level_1_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    return rule_info_part.get("scene_classification") 
    return ""


rule_info_dict, rule_id_list = rule_info_seperator(rule_info_df)
def rule_ner_1(rule_info_dict, rule_id_list):
    return partial(rule_extractor, rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)

def rule_ner_2(rule_info_dict, rule_id_list):
    return partial(rule_labeler, rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)

def rule_ner_3(rule_info_dict, rule_id_list):
    return partial(rule_owner, rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)

def rule_ner_4(rule_info_dict, rule_id_list):
    return partial(rule_classifier, rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)

class DataExtractor(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark

        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        # 设置副本数为2
        self.spark.sql("set dfs.replication=3")

    def run(self, source_table, target_table, the_date, file_no):
        """
        执行目标数据获取任务

        Args:
            source_table: 必填参数，上游数据表;  str
            target_table: 必填参数，目标数据表;  str
            the_date: 必填参数，待处理分区;  str
            file_no: 必填参数，待处理分区;  str
        """
        # 读取分区数据
        data = self.read_partition(source_table=source_table, the_date=the_date, file_no=file_no)

        rule_ner_entity = rule_ner_1(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)
        rule_ner_entity_type = rule_ner_2(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)
        rule_ner_entity_owner = rule_ner_3(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)
        rule_classifier = rule_ner_4(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list)
        self.spark.udf.register("rule_ner_entity_udf", rule_ner_entity, returnType=StringType()) # 把python的函数定义成spark的UDF
        self.spark.udf.register("rule_ner_entity_type_udf", rule_ner_entity_type, returnType=StringType()) # 把python的函数定义成spark的UDF
        self.spark.udf.register("rule_ner_entity_owner_udf", rule_ner_entity_owner, returnType=StringType()) # 把python的函数定义成spark的UDF
        self.spark.udf.register("rule_classifier_udf", rule_classifier, returnType=StringType()) # 把python的函数定义成spark的UDF
        data = data.selectExpr("row_key", "mobile_id", "event_time", "app_name", "suspected_app_name",
                               "main_call_no","abnormal_label", "hashcode", "msg",
                               "rule_ner_entity_udf(msg, app_name, suspected_app_name) as virtual_id",
                               "rule_ner_entity_type_udf(msg, app_name, suspected_app_name) as virtual_id_type",
                               "rule_ner_entity_owner_udf(msg, app_name, suspected_app_name) as is_owner",
                               "rule_classifier_udf(msg, app_name, suspected_app_name) as scene",
                               "the_date", "file_no")

        data = data.filter("virtual_id != ''") # 把UDF返回""的数据过滤掉
        # 数据条数大小来设置一下分区数据
        data.cache()
        cnt = data.count()
        repartition = cnt//16000000
        # 写入数据
        data.createOrReplaceTempView("tmp_table")
        _sql = "insert overwrite table {} partition(the_date,file_no) select * from tmp_table distribute by pmod(hash(1000*rand(1)), {})".format(target_table, repartition)
        self.logger.info("将要执行如下sql进行数据插入:")
        self.logger.info(_sql)
        self.spark.sql(_sql)

if __name__=="__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="数据抽取模块")
    parser.add_argument("--app_name", default="rule_ner_extractor", dest="app_name", type=str, help="spark任务名称")
    parser.add_argument("--source_table", default=None, dest="source_table", type=str, help="上游数据表")
    parser.add_argument("--target_table", default=None, dest="target_table", type=str, help="目标数据表")
    parser.add_argument("--the_date", default=None, dest="the_date", type=str, help="需要处理的the_date分区")
    parser.add_argument("--file_no", default=None, dest="file_no", type=str, help="需要处理的file_no分区")
    args = parser.parse_args()
    # 初始化
    data_extractor = DataExtractor(app_name=args.app_name)

    data_extractor.run(source_table=args.source_table, target_table=args.target_table, the_date=args.the_date, file_no=args.file_no)
    # 结束
    data_extractor.stop()
