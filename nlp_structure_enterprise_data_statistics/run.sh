# 切换crontab的执行路径
cd /home/etl/qianyu/nlp_structure_enterprise_data_statistics
/usr/local/python3/bin/python dataflow.py >> /home/etl/qianyu/nlp_structure_enterprise_data_statistics/log/enterprise_big_screen_`date +%Y%m%d`.log 2>&1 & 