#!/bin/bash

# Replace j-XXXXXXXXXXXXX with your actual EMR cluster ID
CLUSTER_ID=$1

if [ -z "$CLUSTER_ID" ]; then
  echo "Usage: ./submit-emr-job.sh <cluster-id>"
  exit 1
fi

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=Spark,Name="FileRead-FileWrite-ETL",ActionOnFailure=CONTINUE,Args=[--class,com.qwshen.etl.Launcher,--deploy-mode,cluster,--conf,spark.yarn.submit.waitAppCompletion=true,s3://samir-apache-spark-demo/jars/spark-etl-framework-0.1-SNAPSHOT.jar,--pipeline-def,s3://samir-apache-spark-demo/pipelines/pipeline_fileRead-fileWrite.xml,--application-conf,s3://samir-apache-spark-demo/config/application-emr.conf,--var,process_date=20231201]
