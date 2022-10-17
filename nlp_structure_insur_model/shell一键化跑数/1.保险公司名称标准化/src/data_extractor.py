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
# rule_info_df = pd.read_csv(os.path.join(ABSPATH, "../config/rule_classifier_info.txt"), sep="\t")
dict_list_file = os.path.join(ABSPATH, "../config/dict_list_file.json")
mapping = os.path.join(ABSPATH, "../config/mapping.json")

# 以字典树的形式加载白名单
def load_white_name_trie(address1, address2):

    kp = KeywordProcessor()

    with open(address1,'r', encoding='UTF-8') as f:
        texts = json.load(f)

    for text in texts['insur_name']:
        kp.add_keyword(keyword=text)  # 关键词

    with open(address2,'r', encoding='UTF-8') as f:
        dict = json.load(f)

    return kp, dict


def rule_cleaner(msg, white_name_trie, mapping_dict):
    result = white_name_trie.extract_keywords(msg)
    if len(result) != 0:
        # return result[0]
        return mapping_dict[result[0]]
    else:
        return '未识别'


######################
# product_name_udf 产品名称清洗
######################

P_PRODUCT_NAME = r'[^一-龢0-9A-Za-z\(\)（）]'

def _is_empty_str(text):
    if not text or re.search(r'^\s+$', text):
        return True
    return False

def _basic_clean(text, least_length=1):
    """基本清洗

    把算法输出的分隔符去掉，实体的索引去掉，单字段多实体的情况，以空格相连，
    return list
    空值返回None
    """
    if _is_empty_str(text):
        return None
    text = text.replace(' ','')
    words = text.split('#ALGO_ITEM_SEP#')  # 去掉多实体分隔符
    words = [re.sub(r"[0-9]+\$@\$", "", word) for word in words]  # 去掉实体索引
    words = [word.strip() for word in words]  # 去除空格
    words_ = []
    for word in words:
        if len(word) >= least_length and word not in words_:
            words_.append(word)
    if len(words_) == 0:
        return None
    return words_

def _format(text, format_func):
    entities = _basic_clean(text)
    if entities:
        entities = [format_func(entity) for entity in entities]
        entities = [
            entity for entity in set(entities) if not _is_empty_str(entity)
        ]
        if entities:
            return entities[0]

def product_name_format(text):
    """
    只保留中文数字英文和小括号，如果无任何相应字符，返回None；
    """
    left = re.sub(P_PRODUCT_NAME, '', text)
    if left != '':
        return left
    return None

def product_name_clean(text):
    """
    只保留非标点符号清洗
    """
    return _format(text, product_name_format)

# UDF使用方法
# product_name_udf = udf(lambda x: product_name_clean(x), returnType=StringType())


# 加载偏函数的参数
# 顶格，全局变量
white_name_trie, mapping_dict = load_white_name_trie(address1=dict_list_file,address2=mapping)


def rule_ner(white_name_trie,mapping_dict):
    return partial(rule_cleaner, white_name_trie=white_name_trie, mapping_dict=mapping_dict)


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
        rule_insur_name = j(white_name_trie=white_name_trie, mapping_dict=mapping_dict)
        self.spark.udf.register("rule_insur_name_udf", rule_insur_name, returnType=StringType()) # 把python的函数定义成spark的UDF
        self.spark.udf.register("product_name_clean_udf", product_name_clean, returnType=StringType()) 
        # data.show(truncate=False)
        # 注意：spark_base_connector.py中定义的模板SQL
        # 模板SQL包含如下字段
        # row_key,mobile_id,event_time,app_name,suspected_app_name,msg,main_call_no,abnormal_label,hashcode,product_name,the_date,file_no
        # 如果想加入额外的字段需要修改模板SQL
        data = data.selectExpr("row_key", "mobile_id", "event_time", "app_name", "suspected_app_name", "msg", "main_call_no",
                               "abnormal_label", "hashcode",
                               "product_name_clean_udf(product_name) as product_name",
                j              "rule_insur_name_udf(msg) as insur_institute", 
                               "the_date", "file_no")

        # data = data.filter("behavior_type != ''") # 把UDF返回""的数据过滤掉
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
