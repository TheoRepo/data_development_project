import pandas as pd
import os
import re
import sys,toml
sys.path.append('..')
from src.data_extractor import rule_info_seperator,rule_ner_1,rule_ner_2,load_white_name_trie

# 读取规则原始文件
ABSPATH = os.path.dirname(os.path.abspath(__file__))  # 将文件所在的路径记为绝对路径
rule_info_df = pd.read_csv(os.path.join(ABSPATH, "../config/rule_classifier_info.txt"), sep="\t")
white_name_dict_address = os.path.join(ABSPATH, "../config/dict_list_file.json")

# 定义参数（顶格，全局变量）
white_name_trie = load_white_name_trie(white_name_dict_address)
rule_info_dict, rule_id_list = rule_info_seperator(rule_info_df)

# 使用偏函数传参数
rule_classifier_behavior = rule_ner_1(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list, white_name_trie=white_name_trie)
rule_classifier_owner = rule_ner_2(rule_info_dict=rule_info_dict, rule_id_list=rule_id_list, white_name_trie=white_name_trie)

def test_rule_behavior(msg,app_name,suspected_app_name):
    a = rule_classifier_behavior(msg,app_name,suspected_app_name)
    return a


def test_rule_owner(msg,app_name,suspected_app_name):
    a = rule_classifier_owner(msg,app_name,suspected_app_name)
    return a


def test_honor_certificate():
    print("开始荣誉证书的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【大赛组委会】证书领取-大学生近代史知识竞赛：李诗泳同学你的证书已发放，请关注微信公众号：华学竞赛网回复：6领取证书。回T退订'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_freshman_service():
    print("开始在校生服务的测试")
    # 测试一
    app_name = '广东外语外贸大学'
    suspected_app_name = '广东外语外贸大学'
    msg = '【广东外语外贸大学】亲爱的同学,母校正在进行毕业生培养质量评估,请登录您的腾讯邮箱(内含母校通知入口)支持母校教改。'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_student_app():
    print("开始学生专属应用的测试")
    # 测试一
    app_name = '今日校园'
    suspected_app_name = '今日校园'
    msg = '【今日校园】嗨~你的老师发来了一条收集《2021级新生邮箱信息收集》,请尽快打开移动APP平台查看!'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_school_recruitment():
    print("开始校招的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【前程无忧】宋天奇同学,您好!恭喜您通过四川联通校园招聘初筛,现邀请您进行在线测评,请务必于3月'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_campus_competition():
    print("开始校园竞赛的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【学分帮】黄英超同学,您参加全国大学生人口普查知识竞赛全国决赛获奖名单已公布,请搜索微信公众号:竞赛库,查看您的获奖成绩,领取奖品'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_university_credit_course():
    print("开始大学学分课程的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【智慧树】唐远航同学,《军事理论-综合版》06月01日后学习不记进度,作业提交截止,抓紧学习咯!'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_student_loan():
    print("开始助学贷款发放的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【北京银行】尊敬的客户肖开提江·努尔,您在北京银行学知支行办理的国家助学贷款5300.0元已发放,感谢您对我行的支持。详情请向您的贷款支行或95526问讯。'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_scholarship_bursary():
    print("开始助学贷款发放的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【北京银行】尊敬的客户肖开提江·努尔,您在北京银行学知支行办理的国家助学贷款5300.0元已发放,感谢您对我行的支持。详情请向您的贷款支行或95526问讯。'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_scholarship_bursary():
    print("开始助学贷款发放的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【北京银行】尊敬的客户肖开提江·努尔,您在北京银行学知支行办理的国家助学贷款5300.0元已发放,感谢您对我行的支持。详情请向您的贷款支行或95526问讯。'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_student_certification():
    print("开始学生认证的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【同程旅游】恭喜您成功认证学生身份,购机票可享单单立减!点 https://s.ly.com/fieb1h1tc 立即抽神秘盲盒!回TD退订'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_postgraduate_entrance_examination():
    print("开始考研的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【高顿】同学迟到啦~您报名的《22届考研小白全程备考规划》"直播:考研政治备考规划-毕中毅"已开始,快快'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_student_calling_card():
    print("开始学生电话套餐的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【订购提醒】尊敬的客户,您好!您已成功订购:2019校园客户送权益48元档【每月享100G校园区域流量、100分钟国内语音(不含港澳台本月实时话费0.00元,当前可用余额0.00元。动感地带移动校园48套餐基本通话优惠分钟数余86分钟,国内通用流量共17.09GB,当'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_student_ticket():
    print("开始购学生票的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【飞猪门票】您在武隆仙女山旅游专营购买的仙女山 大门票 大学生票,确认码219584227于2021-04-15 16:03已经核销成功1份,还剩0份.如有疑问,请联系卖家: 18983338334 .飞猪服务热线9510208 .'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_courier_address_on_campus():
    print("开始快递地址在校园的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【韵达快递】快递给你放到化工学校门卫了 记得取一下 门卫的快递柜里面 有问题请联系:18703011146'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


def test_student_insurance():
    print("开始学生保险的测试")
    # 测试一
    app_name = '.'
    suspected_app_name = '.'
    msg = '【平安养老险】尊敬的陈建国,您购买的学业福浙江学生平安保险(尊享款)保单号PC1200A246186410即将满期,请在2021年09'
    print(msg)
    print(test_rule_behavior(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))


if __name__ == "__main__":
    test_honor_certificate()
    test_freshman_service()
    test_student_app()
    test_school_recruitment()
    test_campus_competition()
    test_university_credit_course()
    test_student_loan()
    test_scholarship_bursary()
    test_student_certification()
    test_postgraduate_entrance_examination()
    test_student_calling_card()
    test_student_ticket()
    test_courier_address_on_campus()
    test_student_insurance()


