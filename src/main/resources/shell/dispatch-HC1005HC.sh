#!/bin/bash
date1="$(date +%Y%m%d)"
source /home/hadoop/rm-outpath.sh HC1005HC
# change path to spark_path
cd $SPARK_HOME
# execute spark submit, then upload file to local from hdfs
./bin/spark-submit --class com.raoqiang.scala.PosCash \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 6g \
    --executor-memory 4g \
    --executor-cores 2 \
    --queue homeCredit \
    myjar/sparkyarn.jar $date1 > /usr/mysoft/spark/hc_logs/HC1005HC"$date1".log 2>&1  && hdfs dfs -get /user/spark/HC1005HC/$date1/pa* ~/hc_data/HC2005HC"$date1".csv & 

