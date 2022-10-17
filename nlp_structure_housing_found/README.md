# nlp_structure_housing_found

从地域、行为、个人/公司三个维度统计我司公积金业务2021年所有的数据量和覆盖人群

## 跑数方法
1. 将1_union_all.sql的代码复制在dataworks的任务栏里，一键运行，生成分析需要使用的中间表
2. 将2_dim_analyse.sql的代码复制在dataworks的任务栏里，逐行运行，得到地域，行为，对公对私三个维度的统计结果，结果复制到excel
3. 将3_non_blanck_percent.sql的代码复制在dataworks的任务栏里，逐行运行，得到字段非空占比的统计结果，结果复制到excel
## 结构化结果表

| dwb表名 | online表名 | 主题 | msg |
| ---- | ---- | ---- | ---- |
| dwb.housing_found_txt_loan_repayment_v2 | nlp_online.housing_found_txt_loan_repayment_v2 | 社会保障_公积金_贷款_还款 | 您好!您于2019年6月30日偿还公积金贷款2455.21,其中本金1250.00元、利息1205.21元。剩余贷款本金为443750.00元。 |
| dwb.housing_found_txt_loan_release_v2 | nlp_online.housing_found_txt_loan_release_v2 | 社会保障_公积金_贷款_发放 | 尊敬的客户:您的个人住房公积金委托贷款于20190701发放成功,金额为500000.00元,到期日为20340701,月利率为2.708333‰,首次还款日为20190801,首次还款金额为3513.34元。您可联系网点获取借款合同和还款计划表,或登录手机银行查询贷 |
| dwb.housing_found_txt_loan_overdue_v2 | nlp_online.housing_found_txt_loan_overdue_v2 | 	社会保障_公积金_贷款_逾期 | 【省监狱住房公积金】*思龙,您的公积金贷款截止上月末已逾期1期,逾期金额66.31元,请速到盐城大丰支行柜面还款,以免影响您的个人征信记录 |
| dwb.housing_found_txt_loan_repayment_reminder_v2 | nlp_online.housing_found_txt_loan_repayment_reminder_v2 | 社会保障_公积金_贷款_还款提醒 | 【荆州公积金】尊敬的客户陈峰,您好!04日是您的约定还款日,本次应还1856.41元。请您留意还款账户金额是否充足,以便我们按时划款。 |
| dwb.housing_found_txt_loan_approval_v2 | nlp_online.housing_found_txt_loan_approval_v2 | 社会保障_公积金_贷款_审批 | 尊敬的客户,您的长沙市直公积金点贷申请已审批通过,本次贷款金额9.9万元,欢迎确认、签约。【浦发银行】 |
| dwb.housing_found_txt_corporate_v2 | nlp_online.housing_found_txt_corporate_v2 | 社会保障_公积金_对公 | 【市公积金中心】尊敬的单位经办人,感谢您对贵阳市住房公积金中心工作的支持,请您对我们19年4月至19年6月的服务进行评价,回复数字1为 |
| dwb.housing_found_txt_base_adjustment_v2 | nlp_online.housing_found_txt_base_adjustment_v2 | 社会保障_公积金_基数调整 | 【广州公积金中心】尊敬的甘峰,您尾号为671700的住房公积金账户,从2019年07月01日起,缴存基数调整为7523.0元,月汇缴金额调整 |
| dwb.housing_found_txt_withdraw_v2 | nlp_online.housing_found_txt_withdraw_v2 | 社会保障_公积金_提取 | 【昆明住房公积金】您好!您正在个人公积金提取,短信动态验证码420301,请注意保管。 |
| dwb.housing_found_txt_deposit_v2 | nlp_online.housing_found_txt_deposit_v2 | 社会保障_公积金_个人缴存 | 【住房公积金】您尾号4104的公积金账户于07月01日汇缴1550元,缴至2019-06月,当前余额38796.02元。<盘锦> |
| dwb.housing_found_txt_paystub_v2 | nlp_online.housing_found_txt_paystub_v2 | 社会保障_公积金_工资 | 【海南农信社】您本月应发工资7809,代扣个人所得税0,公积金926,社保775.74,职业年金295.52,代扣款0,实发5811.74。国库支付局 |


