#!/usr/bin/python
# -*- coding:utf-8 -*-
import re
import sys
import json
sys.path.append('..')


def college_name_regexp(input_address,out_address1, out_address2 = None):
    """
    生成白名单字典
    """
    college_name_dict = {}
    college_name_list = []
    pattern = re.compile(r'\（.*?\）')
    with open(input_address, 'r') as f:
        data = f.readlines()
        for line in data:
            if len(line.strip()) > 0:
                result1 = re.sub(pattern, '', line.strip())
                college_name_list.append(result1)
    # print('全国一共有%s所普通高等学院' %len(college_name_list))
    college_name_dict['suspected_app_name'] = college_name_list
    with open(out_address1, 'w') as f:
        # 创建带有双引号作为默认引号格式的Python词典
        f.write(json.dumps(college_name_dict, ensure_ascii=False))

    if out_address2 is not None:
        regexp_tmp = ''
        regexp_string = ''
        with open(input_address, 'r') as f:
            data = f.readlines()
        for line in data:
            if len(line.strip()) > 0:
                result1 = re.sub(pattern, '', line.strip())
                college_name_list.append(result1)
                result2 = result1 + '|'
                regexp_tmp = regexp_tmp + result2
                regexp_string = '.*?(' + regexp_tmp.strip('|') + ').*?'
        with open(out_address2, 'w') as f:
            f.write(regexp_string)

if __name__=='__main__':
    input_address = '../data/全国普通高等学院名单_2022_05_31.txt'
    out_address1 = '../config/dict_list_file.json'
    # out_address2 = '../data/long_rules.txt'
    college_name_regexp = college_name_regexp(input_address, out_address1)
