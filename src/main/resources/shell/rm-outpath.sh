#!/bin/bash
date1="$(date +%Y%m%d)"
echo $date1
hdfs dfs -ls /user/spark/$1/$date1/
if [ $? == 0 ]
then
    echo "delect output file in hdfs"
    hdfs dfs -rm -R /user/spark/$1/$date1/
fi

