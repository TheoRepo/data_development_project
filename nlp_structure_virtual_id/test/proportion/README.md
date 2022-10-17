# 实体数据量占比核验

```bash
beeline -u "jdbc:hive2://coprocessor01-fcy.hadoop.dztech.com:2181,coprocessor02-fcy.hadoop.dztech.com:2181,coprocessor03-fcy.hadoop.dztech.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --showHeader=false --outputformat=tsv2 -e "select msg from nlp_dev.virtual_id_verification_20220720">>virtualID_20220720.txt
```

```bash
curl -H 'Content-Type: multipart/form-data' -F "file=@virtualID_20220720.txt" "http://10.30.103.146:8080/nlp/file/upload/1122"
```