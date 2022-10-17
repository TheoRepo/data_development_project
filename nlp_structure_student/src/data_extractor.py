#!/usr/bin/python
# -*- coding:utf-8 -*-
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

import json
import sys,toml
sys.path.append('..')
from libs.spark_base_connector import BaseSparkConnector
from libs.keyword import KeywordProcessor

# 配置文件信息
os.environ['PYSPARK_PYTHON'] = "/usr/local/python3.7.4/bin/python3"
ABSPATH = os.path.dirname(os.path.abspath(__file__))  # 将文件所在的路径记为绝对路径


# 读取规则原始文件
rule_info_df = pd.read_csv(os.path.join(ABSPATH, "../config/rule_classifier_info.txt"), sep="\t")
white_name_dict_address = os.path.join(ABSPATH, "../config/dict_list_file.json")


# 规则拆解成字典
def rule_info_seperator(rule_info_df):
    rule_info_dict = {}
    rule_id_list = rule_info_df[(rule_info_df['is_active']==1)]['rule_id'].values.tolist()     # 只读取被激活的规则
    rule_info_df_id = rule_info_df.set_index('rule_id')
    for rule_id in rule_id_list:
        rule_info_dict[rule_id] = {
                "level_1_forward_rule_app": rule_info_df_id.loc[rule_id]['level_1_forward_rule_app'],
                "level_1_forward_rule_msg":rule_info_df_id.loc[rule_id]['level_1_forward_rule_msg'],
                "level_2_forward_rule_msg":rule_info_df_id.loc[rule_id]['level_2_forward_rule_msg'],
                "level_3_forward_rule_msg":rule_info_df_id.loc[rule_id]['level_3_forward_rule_msg'],
                "level_4_forward_rule_msg":rule_info_df_id.loc[rule_id]['level_4_forward_rule_msg'],
                "level_1_backward_rule_msg":rule_info_df_id.loc[rule_id]['level_1_backward_rule_msg'],
                "behavior_type":rule_info_df_id.loc[rule_id]['behavior_type'],
                "owner_identity":rule_info_df_id.loc[rule_id]['owner_identity']
        }
    return rule_info_dict, rule_id_list        


# 以字典树的形式加载白名单
def load_white_name_trie(address):

    kp = KeywordProcessor()

    with open(address,'r') as f:
        jsonData = f.readline()
        texts = json.loads(jsonData)

    for text in texts['suspected_app_name']:
        kp.add_keyword(keyword=text)  # 关键词

    return kp


def rule_behavior(msg, app_name, suspected_app_name, rule_info_dict, rule_id_list, white_name_trie):
    for rule_id in rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)   
        if rule_info_part.get("level_1_forward_rule_app") == "white_name_list":
            result = white_name_trie.extract_keywords(suspected_app_name)
            if len(result) != 0:
                if re.search(rule_info_part.get("level_1_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_2_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_3_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_4_forward_rule_msg"),msg):
                    if rule_info_part.get("level_1_backward_rule_msg") == '.' or re.search(rule_info_part.get("level_1_backward_rule_msg"),msg) is None:
                        return rule_info_part.get("behavior_type")
        else:
            if (re.search(rule_info_part.get("level_1_forward_rule_app"), app_name, re.IGNORECASE) or re.search(rule_info_part.get("level_1_forward_rule_app"), suspected_app_name, re.IGNORECASE)) \
                    and re.search(rule_info_part.get("level_1_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_2_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_3_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_4_forward_rule_msg"),msg):
                if rule_info_part.get("level_1_backward_rule_msg") == '.' or re.search(rule_info_part.get("level_1_backward_rule_msg"),msg) is None:
                    return rule_info_part.get("behavior_type")
    return ""


def rule_owner(msg, app_name, suspected_app_name, rule_info_dict, rule_id_list, white_name_trie):
    for rule_id in rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)   
        if rule_info_part.get("level_1_forward_rule_app") == "white_name_list":
            result = white_name_trie.extract_keywords(suspected_app_name)
            if len(result) != 0:
                if re.search(rule_info_part.get("level_1_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_2_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_3_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_4_forward_rule_msg"),msg):
                    if rule_info_part.get("level_1_backward_rule_msg") == '.' or re.search(rule_info_part.get("level_1_backward_rule_msg"),msg) is None:
                        return rule_info_part.get("owner_identity")
        else:
            if (re.search(rule_info_part.get("level_1_forward_rule_app"), app_name, re.IGNORECASE) or re.search(rule_info_part.get("level_1_forward_rule_app"), suspected_app_name, re.IGNORECASE)) \
                    and re.search(rule_info_part.get("level_1_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_2_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_3_forward_rule_msg"),msg) \
                    and re.search(rule_info_part.get("level_4_forward_rule_msg"),msg):
                if rule_info_part.get("level_1_backward_rule_msg") == '.' or re.search(rule_info_part.get("level_1_backward_rule_msg"),msg) is None:
                    return rule_info_part.get("owner_identity")
    return ""


# 加载偏函数的参数
# 顶格，全局变量
white_name_trie = load_white_name_trie(white_name_dict_address)
rule_info_dict, rule_id_list = rule_info_seperator(rule_info_df)

# 把这个类实例化的过程写成一个函数，可以避免某个节点实例化的过程中找不到libs路径



def rule_ner_1(rule_info_dict, rule_id_list, white_name_trie):
    return partial(rule_behavior, rule_info_dict=rule_info_dict, rule_id_list=rule_id_list, white_name_trie=white_name_trie)

def rule_ner_2(rule_info_dict, rule_id_list, white_name_trie):
    return partial(rule_owner, rule_info_dict=rule_info_dict, rule_id_list=rule_id_list, white_name_trie=white_name_trie)


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

        # 注册sparkUDF 
        rule_classifier_behavior = rule_ner_1(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list, white_name_trie=white_name_trie)
        rule_classifier_owner = rule_ner_2(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list, white_name_trie=white_name_trie)
        self.spark.udf.register("rule_classifier_behavior_udf", rule_classifier_behavior, returnType=StringType()) # 把python的函数定义成spark的UDF
        self.spark.udf.register("rule_classifier_owner_udf", rule_classifier_owner, returnType=StringType()) # 把python的函数定义成spark的UDF
        data = data.selectExpr("row_key", "mobile_id", "event_time", "app_name", "suspected_app_name",
                               "main_call_no","abnormal_label", "hashcode",
                               "rule_classifier_behavior_udf(msg, app_name, suspected_app_name) as behavior_type",
                               "rule_classifier_owner_udf(msg, app_name, suspected_app_name) as owner_identity",
                               "the_date", "file_no")

        data = data.filter("behavior_type != ''") # 把UDF返回""的数据过滤掉
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
    parser.add_argument("--app_name", default="rule_classifier_extractor", dest="app_name", type=str, help="spark任务名称")
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
