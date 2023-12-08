# spark_query

```bash
# spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-cloud-storage:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0 \
    ./spark_query.py --s3accesskey haruband --s3secretkey haru1004 --input s3a://test0 --query "select company,count(*) from test0 group by company"
```
