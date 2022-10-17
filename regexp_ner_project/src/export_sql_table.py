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
    row_key String COMMENT '唯一编码',
    mobile_id String COMMENT '手机号映射id',
    event_time String COMMENT '发信时间',
    app_name String COMMENT '清洗签名',
    suspected_app_name String COMMENT '原始签名',
    main_call_no String COMMENT '发信号码',
    abnormal_label String COMMENT '是否为正常文本',
    hashcode String COMMENT 'msg的simhash编码',
    msg String COMMENT '短文本内容',
    virtual_id String COMMENT '虚拟ID',
    virtual_id_type String COMMENT '虚拟ID归属的APP类型',
    is_owner String COMMENT '虚拟ID是否属于手机号持有者',
    scene String COMMENT '虚拟ID归属的场景'
)COMMENT '{0}' partitioned BY(
    the_date string COMMENT '业务日期yyyy-MM-dd格式',
    file_no string COMMENT 'file_no'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS orc;
""".format(config_dict.get('target_table'))

with open(args.output, 'w', encoding='utf-8') as f:
    f.write(sql)
