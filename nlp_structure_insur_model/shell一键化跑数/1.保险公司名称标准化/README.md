# 机构分布统计新增需求

**补充需求**

机构分布的统计结果中，不需要投保渠道：比如水滴保险、轻松保，只要投保机构，比如中国人寿

**解决措施**

措施：将[保险公司名称清单](http://www.cbirc.gov.cn/cn/view/pages/ItemList.html?itemPId=937&itemId=941&itemUrl=zaixianfuwu/renshenxian.html&itemName=%E4%BA%BA%E8%BA%AB%E9%99%A9%E5%A4%87%E6%A1%88%E4%BA%A7%E5%93%81%E7%9B%AE%E5%BD%95%E6%9F%A5%E8%AF%A2#1)中的保险机构名称做成白名单，和nlp_online.insurance_txt_apply表的msg字段发生碰撞，得到保险机构名称字段，最后根据新增的保险机构名称字段去统计机构分布

**跑数指令**

1. 基于config.toml文件执行如下命令生成建表语句并完成表创建
```bash
sh create_table.sh --config ./config/config.toml --output create_table.sql
```

2. 执行特定一天跑数任务
```bash
sh run.sh --config ./config/config.toml --the_date 2021-10-01 --file_no all
```

3. 执行如下命令完成历史补数(注意前开后闭,例子中的2021-12-06是不进行跑数的)
```bash
sh run_whole_date.sh 2019-07-01 2021-12-06
nohup sh run_whole_date.sh 2019-07-01 2021-12-06 > output.log 2>&1 &
```

**结果表(nlp_dev.insurance_txt_apply_white_list)**

| row_key | mobile_id | event_time | app_name | suspected_app_name | msg | main_call_no | abnormal_label | hashcode | product_name | insur_institute | the_date | file_no |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| ea9c658a203487a8b765921d6f2df55a8be4e13bazludehh | 94377162 | 2021-10-01 16:51:06 | 中国人寿保险-财险 | 中国人寿财险 | 【中国人寿财险】尊敬的陆贝贝先生/女士,您投保的六安非机动车第三者责任保险(方案二)起保日期为2021-10-02,终保日期为2022-10-01,投保单号为6215182021341594002509,请您点击以下链接完成支付rscx.cc/ThioAO,祝您生活愉快 | 1069106895519 | 正常文本 | 110100011001100100001010011001010001000000000000010100100000100 | 六安非机动车第三者责任保险(方案二) | 中国人寿保险股份有限公司 | 2021-10-01 | merge_20211001_0123_L0 |
| cd8bae7b299d395783f15b08d97f7a58dcdbe0cclydylhov | 121243442 | 2021-10-01 09:04:03 | 中国平安保险-健康险 | 平安健康险 | 【平安健康险】尊敬的李绍琴女士,您的“平安e生保2020保险产品组合”保险(保单尾号481183)已承保。因产品条款变更,请点击 ht | 10694097152 | 正常文本 | 1000111010000001000000111010000100001000001001011001100010100000 | 平安e生保2020保险产品组合保险 | 平安健康保险股份有限公司 | 2021-10-01 | merge_20211001_0123_L0 |


**数据量抽样核验**

```sql
select count(*) from nlp_online.insurance_txt_apply where the_date = '2021-09-01';
+-----------+--+
|    _c0    |
+-----------+--+
| 12044556  |
+-----------+--+
```

```sql
select count(*) from nlp_dev.insurance_txt_apply_white_list where the_date = '2021-09-01';
+-----------+--+
|    _c0    |
+-----------+--+
| 12044556  |
+-----------+--+
```

```sql
select count(*) from nlp_online.insurance_txt_apply where the_date = '2019-07-01';
+----------+--+
|   _c0    |
+----------+--+
| 4695350  |
+----------+--+
```

```sql
select count(*) from nlp_dev.insurance_txt_apply_white_list where the_date = '2019-07-01';
+----------+--+
|   _c0    |
+----------+--+
| 4695350  |
+----------+--+
```

**跑数效率**
| 任务 | 天数 | 任务开始时间 | 任务结束时间 | 任务耗时 |
| ---- | ---- | ---- | ---- | ---- |
| 完成2019-07-01到2021-12-05历史补充任务 | 888天 | 2022-08-31 18：38 | | |

[时间差计算器](http://www.atoolbox.net/Tool.php?Id=740)

