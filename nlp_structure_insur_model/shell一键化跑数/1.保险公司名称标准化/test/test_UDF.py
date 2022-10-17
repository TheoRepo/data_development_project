import os
import json
import sys
sys.path.append('..')
from src.data_extractor import rule_ner,load_white_name_trie

# 读取规则原始文件
ABSPATH = os.path.dirname(os.path.abspath(__file__))  # 将文件所在的路径记为绝对路径
dict_list_file = os.path.join(ABSPATH, "../config/dict_list_file.json")
mapping = os.path.join(ABSPATH, "../config/mapping.json")

# 定义参数（顶格，全局变量）
white_name_trie, mapping_dict = load_white_name_trie(address1=dict_list_file,address2=mapping)
rule_insur_name = rule_ner(white_name_trie=white_name_trie,mapping_dict=mapping_dict)

def test_rule_insur_name(msg):
    a = rule_insur_name(msg)
    return a

def test1():
    print("开始测试")
    # 测试一
    msg = '尊敬的马军先生:您投保的国寿祥瑞终身寿险已于2019年06月30日生效,合同号为2019320502546015348767。请留意接听95519回访电话。同时温馨提醒您:我公司销售人员不会向您销售非保险第三方理财产品,为了保障您的资金安全,请您远离非法集资。【中国人寿】'
    print(msg)
    print(test_rule_insur_name(msg))


if __name__ == "__main__":
    test1()
