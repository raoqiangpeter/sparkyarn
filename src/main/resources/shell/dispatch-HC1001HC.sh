#!/bin/bash
date1="$(date +%Y%m%d)"
source /home/hadoop/rm-outpath.sh HC1001HC
# change path to spark_path
cd $SPARK_HOME
# execute spark submit, then upload file to local from hdfs
./bin/spark-submit --class com.raoqiang.scala.BureauM \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 6g \
    --executor-memory 4g \
    --executor-cores 2 \
    --queue homeCredit \
    myjar/sparkyarn.jar $date1 > /usr/mysoft/spark/hc_logs/HC1001HC"$date1".log 2>&1  && hdfs dfs -get /user/spark/HC1001HC/$date1/pa* ~/hc_data/HC2001HC"$date1".csv && HADOOP_CLASSPATH=/usr/mysoft/hbase/lib/hbase-protocol-1.2.6.jar:/usr/mysoft/hbase/conf/ hadoop \
    jar /usr/mysoft/phoenix/phoenix-4.14.1-HBase-1.2-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool \
    -z hadoop00:2181 \
    --table T_BUREAU_AGG \
    --input /user/spark/HC1001HC/$date1/part* >> /usr/mysoft/spark/hc_logs/HC1001HC"$date1".log 2>&1 &

