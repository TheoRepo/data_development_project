import pandas as pd
import os
import re
import sys,toml
sys.path.append('..')
from src.data_extractor import rule_info_seperator,rule_extractor,rule_labeler,rule_owner, rule_classifier

# 配置参数
ABSPATH = os.path.dirname(os.path.abspath(__file__))  # 将文件所在的路径记为绝对路径
rule_info_df = pd.read_csv(os.path.join(ABSPATH, "../config/rule_ner_info.txt"), sep="\t")
rule_info_dict, rule_id_list = rule_info_seperator(rule_info_df)


def test_rule_extractor(msg,app_name,suspected_app_name,rule_info_dict=rule_info_dict,rule_id_list=rule_id_list):
    a = rule_extractor(msg,app_name,suspected_app_name,rule_info_dict, rule_id_list)
    return a


def test_rule_labeler(msg,app_name,suspected_app_name,rule_info_dict=rule_info_dict,rule_id_list=rule_id_list):
    a = rule_labeler(msg,app_name,suspected_app_name,rule_info_dict, rule_id_list)
    return a


def test_rule_owner(msg,app_name,suspected_app_name,rule_info_dict=rule_info_dict,rule_id_list=rule_id_list):
    a = rule_owner(msg,app_name,suspected_app_name,rule_info_dict, rule_id_list)
    return a

def test_rule_classifier(msg,app_name,suspected_app_name,rule_info_dict=rule_info_dict,rule_id_list=rule_id_list):
    a = rule_classifier(msg,app_name,suspected_app_name,rule_info_dict, rule_id_list)
    return a

def test_rules_bosszhipin():
    print("开始boss直聘类虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】任正阳你好,孙寿红等5位老板给您留言,机会往往来得很突然,快去聊聊吧!http://zpurl.cn/uu5Bq 退订回N'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")

    # 测试二
    print("测试二开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】万和医药连锁招聘专员向女士就[药店店员]一职向您发起开聊,向女士:“你好,我们万和医药连锁正在诚聘药房营业员(小龙坎),可以聊聊吗?”;查看详情 http://zpurl.cn/DZueR'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")

    # 测试三
    print("测试三开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】客串出品烧烤精酿经理庄沁回复了您,庄沁:“你好,现在还缺21:00-02:00这个时间段的,每小时13”;查看详情 http://zpurl.cn/PUyLs0 ,回复TD退订。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")

    # 测试四
    print("测试四开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】范女士,您有6条聊天消息未处理,来自海天力人事专员张金超等,去查看 http://zpurl.cn/msTmO 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")

    # 测试五
    print("测试五开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】上海奕己信息科技有限公司Boss董泽亮对您非常感兴趣,希望就ASO优化师职位和您进一步沟通。点击http://zpur'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")

    # 测试六
    print("测试六开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】亮牛半导体BossClaire对您非常感兴趣,想要一份您的附件简历,希望就射频/模拟/音频集成电路设计工程师职位和您进一步沟通。点击http://zpurl.cn/P7UcBG,了解职位详情。回复TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")

    # 测试七
    print("测试七开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】刘女士,美团点评城市经理郑磊新发布了行政专员/助理 (4-5k,不限,本科),去查看 http://zpurl.cn/ln4xE 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")
    
    # 测试八
    print("测试八开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】WONGJIRATHUNMANATSANUN,北京微海管理咨询招聘经理邓先生新发布了西安海底捞直招店长助理保底6500包吃住 (6-10k,不限,本科),去查看 http://zpurl.cn/EJPLc 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")

    # 测试九
    print("测试九开始")
    app_name = 'Boss直聘'
    suspected_app_name = 'Boss直聘'
    msg = '【BOSS直聘】戴丽(安徽凯聚特招聘专员):安徽凯聚特招聘,本周五(25号)上午9点公司统一安排招聘面试 http://zpurl.cn/PUSPFb 回复TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")

    # 测试十
    print("测试十开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】Darion(云上译舍创始人、执行创意总监):你好,我们这边需要一位设计师,能否沟通下? http://zpurl.cn/qxnLU 回复TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")

    # 测试十一
    print("测试十一开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】火巨文化传媒有限公司人事文晓琴仍在等您回复,还望您查看一下。 http://zpurl.cn/P2g9Pn 回复TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")

    # 测试十二
    print("测试十二开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】牛人(吴艳阳)您好,(平安期货)负责(金融客户服务岗(客服))职位的Boss(吴金瑞)向您发送了1.88￥现金红包,并'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")

    # 测试十三
    print("测试十三开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】报告BOSS,张静静(四川亿磊健康管理-促销员,2年)正在寻找服务员的职位,快去看看是不是你心中的优质候选人>>,http://zpurl.cn/PVNOCB 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")

    # 测试十四
    print("测试十四开始")
    app_name = 'BOSS直聘'
    suspected_app_name = 'BOSS直聘'
    msg = '【BOSS直聘】报告BOSS,LUCKY(中正信造价咨询-造价员,5年)正在寻找审计的职位,快去看看是不是你心中的优质候选人>>,'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")

    # 测试十五
    print("测试十五开始")
    app_name = 'boss直聘'
    suspected_app_name = 'boss直聘'
    msg = '【BOSS直聘】(Boss直聘)李扬,时泽林-漳州奇美化工有限公司-机械工程师就「机械工程师」向您发起沟通,去回应 http://zpurl.cn/Ph8uKU 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")

    # 测试十六
    print("测试十六开始")
    app_name = 'boss直聘'
    suspected_app_name = 'boss直聘'
    msg = '【BOSS直聘】分时传媒BossTina对您非常感兴趣,想要一份您的附件简历,希望就高级销售经理职位和您进一步沟通。点击http://'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十六结束")

    # 测试十七
    print("测试十七开始")
    app_name = 'boss直聘'
    suspected_app_name = 'boss直聘'
    msg = '【BOSS直聘】新经典美容公司人事主任钟丽萍仍在等您回复,还望您查看一下。 http://zpurl.cn/P27NbW 回复TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十七结束")

def test_rules_zhifubao():
    print("开始支付宝类虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【花呗】您的支付宝377***@qq.com因余额无法扣款导致花呗自动还款失败,请登录支付宝-花呗用储蓄卡或余额宝还款,逾期未还款会有利息。【和多号副号:135****0556】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")

    # 测试二
    print("测试二开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '尊敬的郝雨露女士:您账户*9395于07月07日19:06在【支付宝】发生快捷支付扣款人民币1,992元,余额55,847.35元。【'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")

    # 测试三
    print("测试三开始")
    app_name = '未识别'
    suspected_app_name = '支付宝'
    msg = '【花呗】你的支付宝账户不吵不闹(*书欣)182******65已开通花呗,打开支付宝App-我的-花呗查看详情。注意账户安全,如有疑问,请致电官方客服95188'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")

    # 测试四
    print("测试四开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】您的151*1304账户有一笔余额转入余额宝失败,请登录支付宝按照引导手动完成余额宝转入。【和多号副号:151****1304】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")

    # 测试五
    print("测试五开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】您申请的理赔金额已经打至您的支付宝账户du******************om。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")

    # 测试六
    print("测试六开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】您已修改邮箱,xht*@tmpmail.*的账户服务已停止。如有疑问,请致电95188'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")

    # 测试七
    print("测试七开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】账户zl5***@gmail.com在Airbnb爱彼迎开通免密支付功能,校验码868015,您正在开通免密支付功能,请确保仅在正确的商户页面输入验证码,谨防钓鱼欺诈!【和多号副号:158****0975】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")

    # 测试八
    print("测试八开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】你的零花钱bmw***@163.com于2021年09月09日11时45分在商户成功付款0.5元,当前余额116.50元。【和多号副号:178****1278】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")

    # 测试九
    print("测试九开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '你的好友*双全送你10元话费红包,你领红包后他也可得,用191****0056登录领取 ds.alipay.com 。退订回复sm【支付宝】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")

    # 测试十
    print("测试十开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '9月13日00:29因你办理参加【9/12 周末蛋...,冻结10.00元在zha*@sina.*账户。详询95188【支付宝】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")

    # 测试十一
    print("测试十一开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '支付宝账号242***@qq.com,你有一笔来自XIAOH****SHEN的境外汇款将汇入你的青岛银行账户(****8850) https://ur.alipay.com/1NwPLG【支付宝】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")


    # 测试十二
    print("测试十二开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】深圳市福田区…向您付款1.00元,已存入您的支付宝764*@qq.*,备注:往来。详询 d.alipay.com/z'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")

    # 测试十三
    print("测试十三开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】您好!手机打开支付宝,点右下角【我的】-【登录】,输入账户:170***@qq.com,点击换个方式登录-【密码登录】-【忘记密码】-系统会问您【能否接收验证码】,系统会验证您的信息,(若是银行卡并非一定是绑定银行卡,本人名下卡号一般都可以验证)验证通过后,重置登录密码尝试登录感谢您的支持!'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")

    # 测试十四
    print("测试十四开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】尊敬的深圳和来达商贸有限公司,您的商家资料已审核通过,账户名是18929931888@189.cn。请点击 https://m.alipay.com/96OwOvy 登录蚂蚁商家平台完成协议签署。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")

    # 测试十五
    print("测试十五开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '*都肉苏·阿布都克比尔于2021年09月08日 11:18向您的账户274***@qq.com发起0.01元转账,备注:转账。请查询您的支付宝账户确认是否入账。【支付宝】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")

    # 测试十六
    print("测试十六开始")
    app_name = '支付宝'
    suspected_app_name = '支付宝'
    msg = '【支付宝】您管理的企业账号*兴市心悦壶兮紫砂文化有限公司(hu1***@163.com)正在解除绑定员工电子工牌,支付宝验证码:146610,泄露验证码会影响资金安全,唯一热线95188。电子工牌为千万商家安全护航。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十六结束")


def test_rules_douyinhuoshanban():
    print("开始抖音火山版虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】下午好,@梦想点燃人生~你喜欢的视频有80人评论了,快来看看他们在说啥,打开「火山」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")

    # 测试二
    print("测试二开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@王者,你喜欢的「美琪粉条」发布了新视频,20w人喊你来看,打开「火山」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")

    # 测试三
    print("测试三开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@明天会更好_14r,你的好友「牡丹花开,可爱温柔。」发了新视频,691人在看,打开「火山」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")

    # 测试四
    print("测试四开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@感悟人生,你曾经观看过的主播「中国加油」最近又开播啦,八卦消息了解一下?打开「火山」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")

    # 测试五
    print("测试五开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@海洋,你关注的「百合ad」发了新视频,15w人在看,打开「火山」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")

    # 测试六
    print("测试六开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@小熊熊,「泉州」有2771条新视频等你来看,快来看看老乡们都在发啥,打开「火山」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")

    # 测试七
    print("测试七开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@火山看世界,您已获得10.9火力,更多钻石、火苗等你来换,z.huoshan.com/5oOHPx3 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")

    # 测试八
    print("测试八开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@曾经走过心,您发布的评论吸引了39w人围观,1186条评论等您看,z.huoshan.com/rWWLOQf 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")

    # 测试九
    print("测试九开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】@金子,你账户中仍有2钻石余额,来火山遇见值得打赏的那个人吧~z.huoshan.com/9t0Sn3N 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")

    # 测试十
    print("测试十开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '	【抖音火山版】雯雯(感谢陪伴)你好,诚邀你成为金牌主播,享有更多收益、流量、提现额度等福利,更多详情可登录APP,前往账号——主播中心查看。 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")

    # 测试十一
    print("测试十一开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】提醒!您的火山号663896565在异地的一台新设备Huawei nova 6登录。时间:2021-02-15 23:04:53参考地点:辽宁省营口市如非本人操作,帐号可能被盗,建议立刻对帐号锁定保护。方法:进入火山客户端—我的—右上角设置图标—账号管理—火山安全中心,按指引操作(客户端版本需要更新到5.0以上)。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")

    # 测试十二
    print("测试十二开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】已为您恢复手机号166****2895与火山帐号81291063693的绑定关系,并锁定该帐号。如需解锁,请进入火山客户端「登录界面—遇到问题—火山安全中心」按指引操作。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")

    # 测试十三
    print("测试十三开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】抖音钱包异动提醒:您的账号90649567129于2021年09月06日22:01 完成3000元充值,可进入抖音(或火山版)app钱包页查看充值记录。如有充值问题或您需要退订钱包异动提醒业务,请您打开抖音短视频或抖音火山版app,进入"消息"-「系统消'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")

    # 测试十四
    print("测试十四开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】火山号25648722630已被锁定,此前登录的所有设备都被踢下线,锁定期间任何方式都不能登录。如需解锁,可以进入火山客户端,进入登录界面—遇到问题—账户安全中心,按指引操作(客户端版本需要更新到5.0以上)。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")

    # 测试十五
    print("测试十五开始")
    app_name = '抖音火山版'
    suspected_app_name = '抖音火山版'
    msg = '【抖音火山版】晚上好,@你~「二哥幸福生活」邀你来看新视频,43w人热议中,打开「火山」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")

def test_rules_douyin():
    print("开始抖音虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】你的抖音号dy19jaqut7ro已更换头条第三方授权帐号。如非本人操作,请短信回复'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")

    # 测试二
    print("测试二开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@健宇奶奶,你关注的「通迅录好友」发了新视频,1448人在看,新消息>z.douyin.com/IAbkX2B 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")

    # 测试三
    print("测试三开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@蒸汽妹妹,1人回复了你发布的评论,还有2w条精彩评论等你来看,新消息>z.douyin.com/aels4cr 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")

    # 测试四
    print("测试四开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@鑫金典汽车座套,2人喜欢了你发布的视频,还有49个人在看,新消息>z.douyin.com/uULS302 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")

    # 测试五
    print("测试五开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@开心妙招,2人评论了你发布的视频,还有507个人在看,新消息>z.douyin.com/uje0Pmh 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")

    # 测试六
    print("测试六开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@星空,1人分享了你发布的视频,还有130个人在看,z.douyin.com/CaL9uOe 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")

    # 测试七
    print("测试七开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@张健,你的好友「日日红食品」发了新视频,有119人在看,新消息>z.douyin.com/aYibKjV 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")

    # 测试八
    print("测试八开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】都可以你好,诚邀你成为金牌主播,享有更多收益、流量、提现额度等福利,更多详情可登录APP,前往设置——创作者服务中心——主播中心查看。 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")

    # 测试九
    print("测试九开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】单身麻麻@小青(求守护)你好,恭喜你成为金牌主播,可前往主播中心查看专属权益。合同文件已发送到你的邮箱,请注意查收'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")

    # 测试十
    print("测试十开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@萌萌啊你好,很抱歉地通知你,你没有通过金牌主播的申请,原因是身份证信息与本人填写信息不匹配,请重新在主播中心进行操作,如需人工协助,可前往主播中心-我的客服进行咨询'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")

    # 测试十一
    print("测试十一开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@你,你关注的「奥Zha(义父孙源辛)」发了新视频,95w人在看,新消息>z.douyin.com/0adxMFK 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")

    # 测试十二
    print("测试十二开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@亲爱的,你关注的「小丫头」发了新视频,82w人在看,新消息>z.douyin.com/aiJFxC6 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")

    # 测试十三
    print("测试十三开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '你的抖音号lucky___ypf行为异常,已被锁定。如需解锁,请进入抖音客户端登录,在弹出对话框中点击「解除锁定」按指引操作。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")

    # 测试十四
    print("测试十四开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@亲爱的,2人喜欢了你发布的视频,还有60个人在看,新消息>z.douyin.com/5EG8QIt 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")

    # 测试十五
    print("测试十五开始")
    app_name = '抖音'
    suspected_app_name = '抖音'
    msg = '【抖音】@,你关注的「英英讲营销」发了新视频,97w人在看,新消息>z.douyin.com/mFi6VCp 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")

def test_rules_jinritoutiao():
    print("开始今日头条虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@哈哈泡泡酷,你关注的「跟蔡照明学买房」发了新内容,已有2w人评论,s.zjurl.cn/JGBbVvu 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")

    # 测试二
    print("测试二开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@小杨哥抖,94w人邀你继续观看「龙年档案」,1442人在热议中,打开「头条」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")

    # 测试三
    print("测试三开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@志丹装载机配件批发,2人阅读了你发布的小视频,还有2个人正在围观,打开「头条」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")

    # 测试四
    print("测试四开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@深藏的记忆祥,「合肥」频道刚更新了2w条精彩内容,吸引了92w人围观,打开「头条」查看一条新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")

    # 测试五
    print("测试五开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@江九斤,你喜欢的「都市大巴车」发布了新内容,96w人喊你来看,s.zjurl.cn/Jso6Bkr 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")

    # 测试六
    print("测试六开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@黑金收割机,你的评论有22w人围观,2715条热评等你看,打开「头条」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")

    # 测试七
    print("测试七开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@d烟抽寂寞,刚刚上新了剧情视频《换心爱结》,已有17次播放,打开「头条」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")

    # 测试八
    print("测试八开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@注意安全,你收藏过的内容新增787条热评,17w人正在看,打开「头条」查看一条新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")

    # 测试九
    print("测试九开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@一岁一礼一寸欢喜密探,1人回复了你发布的小视频,还有2个人正在围观,s.zjurl.cn/JsEF22K 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")


    # 测试十
    print("测试十开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】@青青的日常分享,1人点赞了你发布的小视频,还有3个人正在围观,打开「头条」查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")

    # 测试十一
    print("测试十一开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = ' 【今日头条】@广西云阁装饰有限公司,1人分享了你发布的文章,还有37个人正在围观,s.zjurl.cn/JGshXQ4 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")

    # 测试十二
    print("测试十二开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】亲爱的长沙菲戈室内游戏有限公司,您好!感谢您注册今日头条广告平台账号!您的账号已审核通过,欢迎您登陆使用!【今日头条】投放管理平台'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")

    # 测试十三
    print("测试十三开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】亲爱的房产置业军,你已连续175天未登录你的头条帐号,根据平台冻结规则,连续180天未登录帐号,将会被系统冻结帐号,请在2021年9月12日前登录今日头条APP或头条号官方后台。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")

    # 测试十四
    print("测试十四开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '	【今日头条】您的帐号宝山大耿已成功解锁,帐号现已可以正常登录。建议您加强帐号安全保护,不要把密码或短信验证码泄露给任何其他人。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")

    # 测试十五
    print("测试十五开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】您的帐号台湾观察家通过抖音成功登录。如非本人操作,请短信回复AT6NAA,解除该第三方绑定关系并对帐号进行锁定保护(此操作24小时内有效)。锁定成功后,已登录设备将被踢下线,解锁前不允许登录。登录时间:2021-01-26 00:06:36参考地点:河南省郑州市登录方式:第三方授权登录'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")

    # 测试十六
    print("测试十六开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】亲爱的天空多一道彩虹,根据平台冻结规则,你连续180天未登录帐号,已被系统冻结帐号,请尽快登录今日头条APP或头条号官方后'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十六结束")

    # 测试十七
    print("测试十七开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '	【今日头条】您的帐号公主的日常在一台新设备成功登录。如非本人操作,请短信回复AT8CLV,对帐号进行锁定保护(此操作24小时内有效)。锁定成功后,已登录设备将被踢下线,解锁前不允许登录。登录时间:2021-05-26 19:23:42参考地点:上海市登录方式:手机号验证码'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十七结束")

    # 测试十八
    print("测试十八开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】您的帐号潮流达人开原3b已被锁定,此前登录的所有设备都已被踢下线。锁定期间不允许任何方式登录。如需解锁,请将今日头条客户端升级至最新版本,在登录首页面进入「遇到问题—怀疑帐号被盗—锁定解除」,按指引操作。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十八结束")

    # 测试十九
    print("测试十九开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】您的帐号安迪财经论已更换绑定手机。如非本人操作,请短信回复AT9Q9B,还原手机绑定关系并对帐号进行锁定(此操作24小时内有效)。锁定成功后,已登录设备将被踢下线,解锁前不允许登录。时间:2021-02-01 18:10:03详情:原绑定手机137****5054更换为199****2057'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十九结束")

    # 测试二十
    print("测试二十开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】已为您恢复手机号188****3952与帐号六吓灰的绑定关系,并锁定该帐号。如需解锁,请将今日头条客户端升级至最新版本,在'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十结束")

    # 测试二十一
    print("测试二十一开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】您的帐号乐逍遥59376691已更换微信第三方授权帐号。如非本人操作,请短信回复AT6N67,解除该第三方绑定关系并对帐号进行锁定保护(此操作24小时内有效)。锁定成功后,已登录设备将被踢下线,解锁前不允许登录。操作时间:2021-05-11 22:08:34'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十一结束")

    # 测试二十二
    print("测试二十二开始")
    app_name = '今日头条'
    suspected_app_name = '今日头条'
    msg = '【今日头条】亲爱的娱扒爷,恭喜你获得「头条娱乐万粉俱乐部」入群资格!入群即享优质内容助推,还能第一时间了解平台资讯、娱乐影视热点,与更多平台高粉作者互动,助你在平台成长更快。更有最新免费观影、明星见面会和官方盛典等福利提供。请添加官方运营微信号:wulidada,备注「头条号全称」 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十二结束")


def test_rules_jingdong():
    print("开始京东虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的夜明珠373258794,您在京东购买的订单214275915578已完成,期待您的评价,将有机会获得京豆奖励哦,立即评论。http://3.cn/nPM619 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")    

    # 测试二
    print("测试二开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的我能为你做什么,为您可能感兴趣的换装娃娃找到了优惠福利,下单更实惠,赶紧抢购吧 3.cn/1-al85xo 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")  

    # 测试三
    print("测试三开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的小跳蛙美美哒,好久不见!今天悄悄为您申请限量补贴,24卷纸9.9元包邮到家,棒球帽5.9元仅限今天!低到离谱,还不来抢!'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")  

    # 测试四
    print("测试四开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的人走茶凉0086,您有京豆即将过期,请尽快使用,点击查看 3.cn/1dmXkg-p 回复ND退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")  

    # 测试五
    print("测试五开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的吴小瑜,您的2021年海南银雁科技中秋福利 : 300 积分 已到账,请于2021-09-15 15:27:03至2021-10-16 15:27:03期间访问3.cn/59s-mJB兑换您心仪的商品吧~'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")  

    # 测试六
    print("测试六开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的源于一氏,恭喜您被花YOUNG学生会员礼包砸中啦!猜猜有什么? 3.cn/-1aF4A6r 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")  

    # 测试七
    print("测试七开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】用户一米粮油,您的验证码是:818371,请您在5分钟内输入验证码,切勿泄露于他人'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")  

    # 测试八
    print("测试八开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的永升物业人力: 您有订单220598445680正在等待审批,未审核的超时订单会自动失效,请尽快登录京东慧采进行审批。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")  

    # 测试九
    print("测试九开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的jd_背影狠孤单,您有一份花YOUNG学生会员礼包待领取,戳 3.cn/-196Gvwp 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")  

    # 测试十
    print("测试十开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】尊敬的王一博老婆,您购买的智能冰箱尚未激活,下载小京鱼App,享受智能新生活!点击 3.cn/nAQAzkz'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")  

    # 测试十一
    print("测试十一开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的孤城的,新边界原味独立包装坚果优惠给你安排上了!速戳 3.cn/1cyz-GUh 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")

    # 测试十二
    print("测试十二开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的我能|京东会员为你做什么,为您可能感兴趣的换装娃娃找到了优惠福利,下单更实惠,赶紧抢购吧 3.cn/1-al85xo 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")

    # 测试十三
    print("测试十三开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的京东会员,恭喜您被花YOUNG学生会员礼包砸中啦!猜猜有什么? 3.cn/1erZ-JOu 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")

    # 测试十四
    print("测试十四开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的东粉!五月惠装家,生活家送您万元装修大礼包,德国原装进口主辅材免费升级,戳 3.cn/1d-DvNpG 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")

    # 测试十五
    print("测试十五开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的京东会员,恭喜您被花YOUNG学生会员礼包砸中啦!猜猜有什么? 3.cn/1erZ-JOu 回复BK退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")

    # 测试十六
    print("测试十六开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的团长,您的冷冻品申请已经审核通过,快前往微信搜索“京东区区购店长版”进入小程序,分享商品赚佣金吧'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十六结束")

    # 测试十七
    print("测试十七开始")
    app_name = '京东'
    suspected_app_name = '京东'
    msg = '【京东】亲爱的访客您好,张鑫已为您预约2021-04-02来北京-京东总部1号楼访问,预约码:600915(可供1人使用),也可点击'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十七结束")


def test_rules_kuaishou():
    print("开始快手虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】尊敬的姜忠臣,您拍下的"【金色】剃须刀"未付款,点击链接 s.ihaveyou.cn/o/265Jn5 打开快手付款,再不抢就没了!'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")     

    # 测试二
    print("测试二开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】亲爱的李建功,您刚刚在小店拍下的宝贝库存不多了,请尽快付款以免缺货哦。 回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")   

    # 测试三
    print("测试三开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '一笑倾城4430 您好,您申请的礼品已为您备好!从今天起连续使用快手极速版7天,系统将会为您安排发货,点击下方链接赶快看看吧!→https://ksurl.cn/tII4l0Nq'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")   

    # 测试四
    print("测试四开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】小乖乖今晚6点徐楠楠国庆大放价0元冰箱洗衣机放不停https://v.kuaishouapp.com/s/QeqI0oL4 回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")   

    # 测试五
    print("测试五开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】亲爱的徐明彦,您购买的宝贝已发货,请注意查收哦~回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")   

    # 测试六
    print("测试六开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】亲爱的高浩,宝贝还满意吗,满意请给我们五星评价哦,有问题随时联系我们,一定给您个满意的结果。感谢支持~米米 回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")  

    # 测试七
    print("测试七开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '欲戴皇冠@?必承其重 您好,因未连续7天使用快手极速版,赠品不满足寄出条件,感谢您的参与。继续使用快手极速版看视频,可以赚金币提现哦~'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")   

    # 测试八
    print("测试八开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '小姐姐不想理你 您好,您的赠品收货信息已提交成功。从2021年03月27日起连续使用7天,系统将会自动为您发货,别忘了每天来看哦'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")  

    # 测试九
    print("测试九开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = 'A四川亿信中源贸易有限公司 您好,您今天还未使用快手极速版哦~ 截止到昨日,您已连续使用3天了,继续使用4天礼品将会寄出,点击下方链接'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")   

    # 测试十
    print("测试十开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】楠宝贝20121024给了您童年记忆098一条差评,请尽快去处理吧 退订TD'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")   

    # 测试十一
    print("测试十一开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】亲爱的曹婵娟,您所购买的嘉嘉美体束腰衣为快手官方认证的优选好物,官方活动限时优惠,请放心购买。回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")   

    # 测试十二
    print("测试十二开始")
    app_name = '快手'
    suspected_app_name = '快手'
    msg = '【快手】尊敬的唐国军您好,购买的主品已发,赠品会在近期发货.请耐心等待,有疑问联系4006097880回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")   


    # 测试十三
    print("测试十三开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】246400 快手验证码,15分钟内有效,仅用于帐号注销,请勿告知他人。 如非本人操作,你的密码可能泄露,请立即前往 设置-帐号与安全 页修改密码。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")   

    # 测试十四
    print("测试十四开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】你的好友你是我的,永远都是发布了新作品。戳k.ksurl.cn/8yeK17IX。回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")   

    # 测试十五
    print("测试十五开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】尊敬的用户您好,您于2021年2月9日咨询快手客服的问题已处理完成,请问是否解决您的问题?“解决”请回复“1AY”,未解决'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")  

    # 测试十六
    print("测试十六开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】伊伊呀(开心快乐)终于更新啦!上周你是1w+个老铁最爱看的快手作者。保持上升趋势,快去发布优质作品吧!回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十六结束")  

    # 测试十七
    print("测试十七开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】你的快手账号156274419正在尝试提现,如非本人操作请不要泄露以下信息。确认是本人请回复“KS001”进行认证,验证通过即可正常提现。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十七结束")  

    # 测试十八
    print("测试十八开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】你好,机构“柏芽传媒(mcn)”邀请你开通游戏发行合伙人合作,请登录快手APP查看消息确认。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十八结束")   

    # 测试十九
    print("测试十九开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】亲爱的创作者,由于你的作品质量优质,特此邀请你完成「本周发布3条视频必得1万流量」的官方任务!完成任务不仅必得1到2万流量券,作品达到目标播放量更有最高5万的额外流量奖励,'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十九结束")   

    # 测试二十
    print("测试二十开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】尊敬的用户,你好!你申请的付费视频《满分阅读通关课》已通过审核并发布,建议尽快开始售卖,通过直播和作品进行宣传。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十结束")   


    # 测试二十一
    print("测试二十一开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】熊佳佳佳终于更新啦!上周你是160个老铁最爱看的快手作者。保持上升趋势,快去发布优质作品吧!回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十一结束")   

    # 测试二十二
    print("测试二十二开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】你的快手账号ab871824806正在尝试提现,如非本人操作请不要泄露以下信息。确认是本人请回复“KS001”进行认证,验'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十二结束")   

    # 测试二十三
    print("测试二十三开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】2.11~2.21期间,发作品带#快来一起牛#,及得3万流量,最高得12万流量,点击「http://ksurl.cn/7ISMjW6T」参与,回复T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十三结束")   

    # 测试二十四
    print("测试二十四开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】Yx.♈️终于更新啦!上周你是173个老铁最爱看的快手作者。保持上升趋势,快去发布优质作品吧!回T退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十四结束")   

    # 测试二十五
    print("测试二十五开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】Hi,你2021年2月6日发布的作品「@今天拍‬…」快手官方觉得不错,当前加速推荐中>>官方将关注你的新作品,发作品可能获得额外助推'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十五结束")   

    # 测试二十六
    print("测试二十六开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】He,,,k,,,,你的好友――小任性――发布了新作品。戳k.ksurl.cn/xSjHchII。回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十六结束")   

    # 测试二十七
    print("测试二十七开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】尊敬的用户,你好!你申请的付费直播《快手电商 新人起步》已通过审核并发布。建议你尽快开始售卖,通过直播和作品进行宣传'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十七结束")   

    # 测试二十八
    print("测试二十八开始")
    app_name = '快手科技'
    suspected_app_name = '快手科技'
    msg = '【快手科技】你可能感兴趣的打松塔杆,核桃竿批发零售发布了新作品。戳v.kuaishou.com/s/IZkhfevS。回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十八结束")  


def test_rules_lianxin():
    print("开始连信虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】同乐补胎,申琼香连续3次请求添加你为好友,点击 https://lx1.cn/1Dyrea 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")     

    # 测试二
    print("测试二开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】齐桂红,甜蜜的诱惑已通过你的好友申请,快打开连信和他聊天吧! https://lx1.cn/uI7xuC 查看 退订回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")  

    # 测试三
    print("测试三开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】世界哪有真情在,离你500m的李女士已经连着2天来看你是否在线,点 https://lx1.cn/2Nu7hG 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")  

    # 测试四
    print("测试四开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】风雨来客,艳儿在连信给你发了3条新消息,点 https://lx1.cn/2OpdwJ 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")  

    # 测试五
    print("测试五开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】我是勇士,那个附近300m的张**已经反复4天给你发消息了,点 https://lx1.cn/tECaSj 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")  

    # 测试六
    print("测试六开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】风吹过得街道,离你500m的李女士,悄悄给你留言”好近呀, 可以...”点 https://lx1.cn/4AgBJc 查看拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")  

    # 测试七
    print("测试七开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】最小贱,收到离你300m的张女士连续发来的3条新消息,点 https://lx1.cn/2UbHr7 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")  

    # 测试八
    print("测试八开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】野狼,黑玫瑰发了好友圈照片并@你,点 https://lx1.cn/30NGs7 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")  

    # 测试九
    print("测试九开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信app】就在人群中多看了你一眼,你的手机朋友拾荒的姑娘想加你为连信朋友,点击https://lx1.cn/yqdirT 查看 拒收回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")  

    # 测试十
    print("测试十开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】谁是谁的谁…,附近300m李女士认识你,并发来3条新消息“在吗..”,点 https://lx1.cn/63jYNq 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")  

    # 测试十一
    print("测试十一开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】老头的车,26岁的李**距离你300米已经关注你2天了,点 https://lx1.cn/3AzirX 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")  

    # 测试十二
    print("测试十二开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】幸福为我停留,认识你的附近31岁的李女士,发来3条新消息“你好..”,点 https://lx1.cn/2UIyX8 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")  

    # 测试十三
    print("测试十三开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '刘艳,半生已过。学会沉默。连 续3次请求添加你为好友,点击 https://x.lx1.cn/xPYMTj 查看 拒收回t【连信】'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")  

    # 测试十四
    print("测试十四开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】(一盘没有下完的棋),附近300m的张女士邀请你一起来聊聊周末干什么,点 https://lx1.cn/wC9lFM 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")  

    # 测试十五
    print("测试十五开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】长青,老实人等认识你的朋友正在线申请加你为好友 点 https://lx1.cn/1dqz7G 答复 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")  

    # 测试十六
    print("测试十六开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】安全感,有个附近500m的李**访问了你的主页,他想跟你聊天,点 https://lx1.cn/1xWIqG 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十六结束")  

    # 测试十七
    print("测试十七开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】甘肃李哥,你可能认识的28岁张**,她刚加入了连信并给你留言,点 https://lx1.cn/3G8Gf4 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十七结束")  

    # 测试十八
    print("测试十八开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】运动健将,你被3名附近好友标记为『最感兴趣的人』,点 https://lx1.cn/vnXBLd 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十八结束")  

    # 测试十九
    print("测试十九开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】代价 是折磨,和你有着相同爱好的张**,她现在在线希望周末和你一同出去,点 https://lx1.cn/wBu1SU 查看'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十九结束")  

    # 测试二十
    print("测试二十开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】为你而活,好友张**离你300m刚更新了动态,悄悄@了你,点 https://lx1.cn/ukWsTS 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十结束")    

    # 测试二十一
    print("测试二十一开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】的不行,又遇见她了,你和她已经擦肩而过2次了,戳 https://lx1.cn/vLLg2K 查看 拒T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十一结束")  

    # 测试二十二
    print("测试二十二开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】惩罚,你的通讯录好友张女士也加入连信了~戳 https://lx1.cn/vMVLOH 查看她是谁 拒T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十二结束")  

    # 测试二十三
    print("测试二十三开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】信仰,离你500m的李女士,“好近呀,可以..”2分钟查看有效 快点 https://lx1.cn/113HKW 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十三结束")  

    # 测试二十四
    print("测试二十四开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信app】任国庆商店,附近1.3公里有31岁的李女士连续多次发来好友申请,点 https://lx1.cn/yQRpf4 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十四结束")  

    # 测试二十五
    print("测试二十五开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信app】云南妹,浪漫情人多次请求添加你为好友,点击 https://t.lx1.cn/yVypsp 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十五结束")  

    # 测试二十六
    print("测试二十六开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】吉祥如意,收到一份通讯录好友推荐,您有5位好友已经加入连信,点 https://lx1.cn/uX6B5P 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十六结束")  

    # 测试二十七
    print("测试二十七开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】自由人,你收到1条新消息,点 https://lx1.cn/19z9lT 查看 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十七结束")  

    # 测试二十八
    print("测试二十八开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】限时领现金!尊敬的徐做防腐木用户,您有十元现金奖励即将消失!点 https://lx1.cn/to05Vl 领取 拒收回t'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十八结束") 

    # 测试二十九
    print("测试二十九开始")
    app_name = '连信'
    suspected_app_name = '连信'
    msg = '【连信】幺幺yao切克闹,男,你的手机好友阿狸添加你为连信好友,点击 https://short2.lx-qa.com/s/uUuxNK 查看 退订回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二十九结束")  


def test_rules_ttyuyin():
    print("开始TT语音虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】5分钟前,[对抗路◎将臣]隐身访问了你的主页,登录链接查看 https://s.52tt.com/q/26qR'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束") 

    # 测试二
    print("测试二开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】上周用户[温柔扑了空?]悄悄将您设为特别关注,详情点击 https://s.52tt.com/q/28TZ'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束") 

    # 测试三
    print("测试三开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】三天前您有[1]条来自[肉包子]未读私信,登陆链接查看 https://s.52tt.com/q/27MT'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")

    # 测试四
    print("测试四开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】?九霄(接涂鸦)发来一条[语音],点击链接查看: https://s.52tt.com/q/2lqm'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束") 

    # 测试五
    print("测试五开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】您的账号1182168382正在绑定手机,验证码:757517(验证码5分钟内有效,请勿向他人泄露,以免账号被盗或造成财产损失)'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")

    # 测试六
    print("测试六开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】尊敬的用户,申诉流水号TT476611631881,账号11035087申诉失败,原因为:充值记录不齐全,如有疑问,可登录'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")

    # 测试七
    print("测试七开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】好友小妖精:“你是不是完全不记得我了...”,点击查看 https://s.52tt.com/b/3nNq 退订回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")

    # 测试八
    print("测试八开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】你的TT好友小梦应聘投诉+已将你设为特别关注,可登录链接查看 https://s.52tt.com/q/1EaT'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试八结束")

    # 测试九
    print("测试九开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】好友失败尽常态连续2天看你头像,点击回应 https://s.52tt.com/b/3mwM 退订回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试九结束")

    # 测试十
    print("测试十开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】来自好友宇博快乐消息:“我回来了你人呢...”,点击查看 https://s.52tt.com/b/3nNz 退订回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十结束")

    # 测试十一
    print("测试十一开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】最近过得还好吗?好友看星星还在原地等你 https://s.52tt.com/b/3nNs 退订回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十一结束")

    # 测试十二
    print("测试十二开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】你好友Ru. 养乐多的2条未读消息,点击查看 https://s.52tt.com/b/3mwS 退订回T'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十二结束")

    # 测试十三
    print("测试十三开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】您的帐号【91421759】在14:51:03时通过Redmi K20 Pro Premium Edition手机登陆,若'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十三结束")

    # 测试十四
    print("测试十四开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】您好,账号269238809 姓名(刘英语)反馈的未成年人退款申请已提交成功,我司正常在10个工作日内审核处理,请您留意平台状态(TT官网:http://52tt.com/)或短信通知,谢谢。'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十四结束")

    # 测试十五
    print("测试十五开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】你的好友西红柿已经开播了,快来跟他/她互动吧~'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十五结束")

    # 测试十六
    print("测试十六开始")
    app_name = 'TT语音'
    suspected_app_name = 'TT语音'
    msg = '【TT语音】你的昔日玩伴(id1182108182)给你准备了一份回归大礼包,点击链接领取: https://s.52tt.com/q/2bNi'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试十六结束")


def test_rules_fanqiexiaoshuo():
    print("开始番茄小说虚拟ID的规则测试")
    # 测试一
    print("测试一开始")
    app_name = '番茄小说'
    suspected_app_name = '番茄小说'
    msg = '【番茄小说】@五月没有蝉鸣,您的账户还有3.11元待提现,持续看书可赚29元,打开『番茄小说』查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试一结束")

    # 测试二
    print("测试二开始")
    app_name = '番茄小说'
    suspected_app_name = '番茄小说'
    msg = '【番茄小说】@馨兰小区的狮子猫,您看过的小说《桃运神医》已入选本月大热榜,超过25万人正在围观,打开『番茄小说』查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试二结束")

    # 测试三
    print("测试三开始")
    app_name = '番茄小说'
    suspected_app_name = '番茄小说'
    msg = '【番茄小说】@东平的杨管家,31天没来您错过了99200金币,今日看书最高可赚6800金币,打开『番茄小说』查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试三结束")

    # 测试四
    print("测试四开始")
    app_name = '番茄小说'
    suspected_app_name = '番茄小说'
    msg = '【番茄小说】@爱吃香辣兔头的云照,6.0万人赞了您看过的小说《龙王医婿》,还有32万人正在围观,打开『番茄小说』查看最新消息 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试四结束")

    # 测试五
    print("测试五开始")
    app_name = '番茄小说'
    suspected_app_name = '番茄小说'
    msg = '【番茄小说】@优雅小猫Gm,您在《魔银》的评论已经被1人点赞>>reading.snssdk.com/z/CkpMFOY 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试五结束")

    # 测试六
    print("测试六开始")
    app_name = '番茄小说'
    suspected_app_name = '番茄小说'
    msg = '【番茄小说】@北斗,今天还有金币任务未完成,马上完成赚海量金币>>t.bcy.net/WrHPSK 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试六结束")

    # 测试七
    print("测试七开始")
    app_name = '番茄小说'
    suspected_app_name = '番茄小说'
    msg = '【番茄小说】@无添加,您找的《赘婿》已为您购入上架,立即免费阅读>>reading.snssdk.com/z/9DjRuOe 回TD退订'
    print(test_rule_extractor(msg,app_name,suspected_app_name))
    print(test_rule_labeler(msg,app_name,suspected_app_name))
    print(test_rule_owner(msg,app_name,suspected_app_name))
    print(test_rule_classifier(msg,app_name,suspected_app_name))
    print("测试七结束")



if __name__ == "__main__":
    test_rules_bosszhipin()
    test_rules_zhifubao()
    test_rules_douyinhuoshanban()
    test_rules_douyin()
    test_rules_jinritoutiao()
    test_rules_jingdong()
    test_rules_kuaishou()
    test_rules_lianxin()
    test_rules_ttyuyin()
    test_rules_fanqiexiaoshuo()

