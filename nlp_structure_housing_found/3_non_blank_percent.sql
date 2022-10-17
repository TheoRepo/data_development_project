--********************************************************************--
-- author:      谦豫（虞天）#qianyu
-- create time: 2022-09-06 15:59:57
--********************************************************************--

-- 1. 确认一下公积金事件库的覆盖率，维度（是否缴存，缴存基数，缴存比例）  
-- 2. 验证一下公积金事件的准确度（抽样）

-- 结果表
-- "社会保障_公积金_贷款_还款":"dwb.housing_found_txt_loan_repayment_v2",
-- "社会保障_公积金_贷款_发放":"dwb.housing_found_txt_loan_release_v2",
-- "社会保障_公积金_对公":"dwb.housing_found_txt_corporate_v2",
-- "社会保障_公积金_基数调整":"dwb.housing_found_txt_base_adjustment_v2",
-- "社会保障_公积金_提取": "dwb.housing_found_txt_withdraw_v2",
-- "社会保障_公积金_个人缴存":"dwb.housing_found_txt_deposit_v2",
-- "社会保障_公积金_贷款_逾期":"dwb.housing_found_txt_loan_overdue_v2",
-- "社会保障_公积金_贷款_还款提醒":"dwb.housing_found_txt_loan_repayment_reminder_v2",
-- "社会保障_公积金_工资":"dwb.housing_found_txt_paystub_v2",
-- "社会保障_公积金_贷款_审批":"dwb.housing_found_txt_loan_approval_v2"




-- 所有实体字段的空缺率计算
--- dwb.housing_found_txt_loan_repayment_v2
--- 获取字段
select * from dwb.housing_found_txt_loan_repayment_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_loan_repayment_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(name),
    count(funds_center),
    count(repayment_amount),
    count(repayment_interest),
    count(funds_balance),
    count(loan_balance),
    count(hedge_amount),
    count(repayment_principal),
    count(repayment_date),
    count(bank_amount),
    count(outstanding_amount),
    count(repayment_status) 
from 
    dwb.housing_found_txt_loan_repayment_v2 
where 
    the_date regexp '2019|2020|2021';
    
    
--- dwb.housing_found_txt_loan_release_v2
--- 获取字段
select * from dwb.housing_found_txt_loan_release_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_loan_release_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(repayment_date),
    count(release_date),
    count(loan_amount),
    count(name),
    count(funds_center),
    count(loan_interest),
    count(loan_maturity)
from 
    dwb.housing_found_txt_loan_release_v2 
where 
    the_date regexp '2019|2020|2021';


--- dwb.housing_found_txt_loan_overdue_v2
--- 获取字段
select * from dwb.housing_found_txt_loan_overdue_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_loan_overdue_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(overdue_amount),
    count(funds_center),
    count(name),
    count(overdue_time)
from 
    dwb.housing_found_txt_loan_overdue_v2 
where 
    the_date regexp '2019|2020|2021';
    


--- dwb.housing_found_txt_loan_repayment_reminder_v2
--- 获取字段
select * from dwb.housing_found_txt_loan_repayment_reminder_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_loan_repayment_reminder_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(overdue_amount),
    count(funds_center),
    count(name),
    count(overdue_time)
from 
    dwb.housing_found_txt_loan_repayment_reminder_v2 
where 
    the_date regexp '2019|2020|2021';
    




--- dwb.housing_found_txt_loan_approval_v2
--- 获取字段
select * from dwb.housing_found_txt_loan_approval_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_loan_approval_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(name),
    count(funds_center),
    count(loan_maturity),
    count(loan_amount),
    count(approval_status)
from 
    dwb.housing_found_txt_loan_approval_v2 
where 
    the_date regexp '2019|2020|2021';


--- dwb.housing_found_txt_corporate_v2
--- 获取字段
select * from dwb.housing_found_txt_corporate_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_corporate_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(corporate_name),
    count(bank_balance),
    count(funds_center),
    count(transaction_amount),
    count(transaction_type)
from 
    dwb.housing_found_txt_corporate_v2 
where 
    the_date regexp '2019|2020|2021';
    
    
--- dwb.housing_found_txt_base_adjustment_v2
--- 获取字段
select * from dwb.housing_found_txt_base_adjustment_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_base_adjustment_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(funds_center),
    count(adjusted_individual_base),
    count(name),
    count(adjusted_individual_monthly_deposit),
    count(individual_base_before),
    count(adjusted_deposit_ratio)
from 
    dwb.housing_found_txt_base_adjustment_v2 
where 
    the_date regexp '2019|2020|2021';
    

--- dwb.housing_found_txt_withdraw_v2
--- 获取字段
select * from dwb.housing_found_txt_withdraw_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_withdraw_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(name),
    count(withdraw_amount),
    count(funds_center),
    count(funds_balance),
    count(principle),
    count(interest),
    count(withdraw_type)
from 
    dwb.housing_found_txt_withdraw_v2 
where 
    the_date regexp '2019|2020|2021';
    
    
    
--- dwb.housing_found_txt_deposit_v2
--- 获取字段
select * from dwb.housing_found_txt_deposit_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_deposit_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(corporate_name),
    count(name),
    count(funds_balance),
    count(deposit_amount),
    count(funds_center),
    count(deposit_type)
from 
    dwb.housing_found_txt_deposit_v2 
where 
    the_date regexp '2019|2020|2021';
    

--- dwb.housing_found_txt_paystub_v2
--- 获取字段
select * from dwb.housing_found_txt_paystub_v2 where the_date regexp '2019|2020|2021' limit 1;
--- 首先统计总量：
select count(*) from dwb.housing_found_txt_paystub_v2 where the_date regexp '2019|2020|2021';
--- 然后统计每个字段空置率
select 
    count(medical_insurance),
    count(payable_amount),
    count(paid_amount),
    count(tax),
    count(corporate_name),
    count(unemployment_insurance),
    count(fund_amount),
    count(pension_insurance),
    count(name),
    count(social_security_amount)
from 
    dwb.housing_found_txt_paystub_v2 
where 
    the_date regexp '2019|2020|2021';
    
