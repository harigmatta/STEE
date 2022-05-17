#! /bin/bash
set -euo pipefail
IFS=$'\n\t'

db=$1
table=$2
conf=$3
load_type=$4

if [[ $4 == 'H' ]]
then
 read -p 'Enter business_date(yyyy): ' business_date
else
 business_date=`date -u +"%Y-%m-%d"`
fi
echo $business_date
spark-submit --master yarn --deploy-mode client \
--jars /opt/cloudera/parcels/CDH/lib/hbase_connectors/lib/hbase-spark.jar,/opt/cloudera/parcels/CDH/lib/hbase_connectors/lib/hbase-spark-protocol-shaded.jar \
--files /etc/hbase/conf/hbase-site.xml#hbase-site.xml,conf/OPS_RLT_KPI_TRAIN_SUMMARY#OPS_RLT_KPI_TRAIN_SUMMARY --py-files job/execute.py,util/spark_session.py hbase_hive_etl.py -s $db \
-t $table -j $conf -d $business_date -k $load_type

