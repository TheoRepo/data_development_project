#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import argparse
import toml
sys.path.append('..')

# 定义参数
parser = argparse.ArgumentParser(description="生成SQL表")
parser.add_argument('--config', default=None, dest='config', type=str, help='配置信息')
parser.add_argument('--output', default=None, dest='output', type=str, help='输出结果')
args = parser.parse_args()
print('数据抽取任务解析接受到如下参数 config:{0} output:{1}'.format(args.config, args.output))
# 解析配置信息
with open(args.config, 'r', encoding='utf-8') as f:
    config_dict = toml.load(f)

sql = """
create table if not exists {0}
(
    row_key string COMMENT '唯一编码',
    mobile_id string COMMENT '手机号映射id',
    event_time string COMMENT '发信时间，yyyy-MM-dd hh24:mi:ss取实际收到时间',
    app_name string COMMENT '清洗签名',
    suspected_app_name string COMMENT '原始签名',
    msg string COMMENT '短文本内容',
    main_call_no string COMMENT '发信号码',
    abnormal_label string COMMENT '是否为正常文本',
    hashcode string COMMENT 'msg的simhash编码',
    product_name string COMMENT '保险产品名称',
    insur_institute String COMMENT '保险机构名称'
)COMMENT '{0}' partitioned BY(
    the_date string COMMENT '业务日期yyyy-MM-dd格式',
    file_no string COMMENT 'file_no'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
""".format(config_dict.get('target_table'))

with open(args.output, 'w', encoding='utf-8') as f:
    f.write(sql)

