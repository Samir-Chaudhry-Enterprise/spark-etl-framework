#!/bin/bash

# Upload pipeline definition
aws s3 cp src/test/resources/pipelines/pipeline_fileRead-fileWrite.xml s3://samir-apache-spark-demo/pipelines/

# Upload application configuration  
aws s3 cp src/test/resources/application-emr.conf s3://samir-apache-spark-demo/config/

# Upload SQL transformation script (location based on the pipeline's reference to ${application.scripts_uri}/transform-user-train.sql)
aws s3 cp src/test/resources/scripts/transform-user-train.sql s3://samir-apache-spark-demo/scripts/

# Upload framework JAR (build the project first if needed)
aws s3 cp target/spark-etl-framework-0.1-SNAPSHOT.jar s3://samir-apache-spark-demo/jars/

# Upload input data (adjust paths based on where your test data is located)
aws s3 cp src/test/resources/data/users/ s3://samir-apache-spark-demo/input/users/ --recursive
aws s3 cp src/test/resources/data/train/ s3://samir-apache-spark-demo/input/train/ --recursive
