#!/bin/bash

aws emr create-cluster \
  --name "Spark-ETL-Cluster" \
  --release-label emr-6.5.0 \
  --applications Name=Spark Name=Hadoop \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
  --configurations file://emr-configurations.json \
  --log-uri s3://samir-apache-spark-demo/logs/
