# 切换crontab的执行路径
cd /home/ds/qianyu/nlp_structure_enterprise_data_statistics
/usr/local/python3/bin/python dataflow.py >> /home/ds/qianyu/log/enterprise_big_screen_`date +%Y%m%d`.log 2>&1 & 