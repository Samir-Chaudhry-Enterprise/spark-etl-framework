# Cloud Migration Guide for pipeline_fileRead-fileWrite

This guide explains how to run the `pipeline_fileRead-fileWrite.xml` job on AWS EMR with S3 storage.

## Prerequisites

1. AWS CLI configured with appropriate credentials
2. An EMR cluster running (Spark 3.x, Hadoop 3.x)
3. S3 bucket with proper IAM permissions

## S3 Bucket Structure

Upload your files to the following structure:

```
s3://samir-apache-spark-demo/v1/
├── conf/
│   └── application-cloud.conf
├── input/
│   ├── users/
│   │   └── users.csv
│   └── train/
│       ├── train.txt
│       └── train.csv
├── lib/
│   └── spark-etl-framework.jar
├── pipelines/
│   └── pipeline_fileRead-fileWrite.xml
├── scripts/
│   └── transform-user-train.sql
├── output/
│   └── features/  (created by job)
├── staging/
│   └── events/    (created by job)
└── metrics/
    └── events/    (created by job)
```

## Configuration Files

### application-cloud.conf

Located at `src/test/resources/application-cloud.conf`, this configuration file contains:
- S3 paths for input/output data
- S3A filesystem configuration for AWS
- Spark runtime settings optimized for cloud

### application-onprem.conf

Located at `src/test/resources/application-onprem.conf`, this configuration file contains:
- Local filesystem paths for on-premises execution
- Local Spark settings (`spark.master = local[*]`)

## Uploading Files to S3

```bash
# Set your bucket
BUCKET="s3://samir-apache-spark-demo/v1"

# Upload configuration
aws s3 cp src/test/resources/application-cloud.conf ${BUCKET}/conf/

# Upload pipeline definition
aws s3 cp src/test/resources/pipelines/pipeline_fileRead-fileWrite.xml ${BUCKET}/pipelines/

# Upload SQL scripts
aws s3 cp docs/examples/transform-user-train.sql ${BUCKET}/scripts/

# Upload input data
aws s3 cp docs/examples/data/users.csv ${BUCKET}/input/users/
aws s3 cp docs/examples/data/train.txt ${BUCKET}/input/train/

# Build and upload JAR
mvn clean package -DskipTests
aws s3 cp target/spark-etl-framework-*.jar ${BUCKET}/lib/spark-etl-framework.jar
```

## Job Submission

### Option 1: Using the submission script

```bash
# Set your cluster ID
export EMR_CLUSTER_ID="j-OEUS8V7M7YAM"

# Run the submission script
./scripts/submit-emr-job.sh
```

### Option 2: Using AWS CLI directly

```bash
aws emr add-steps \
    --cluster-id j-OEUS8V7M7YAM \
    --steps "Type=Spark,Name=event-consolidation,ActionOnFailure=CONTINUE,Args=[--class,com.qwshen.etl.Launcher,--master,yarn,--deploy-mode,cluster,--driver-memory,4g,--executor-memory,8g,s3://samir-apache-spark-demo/v1/lib/spark-etl-framework.jar,--pipeline-def,s3://samir-apache-spark-demo/v1/pipelines/pipeline_fileRead-fileWrite.xml,--application-conf,s3://samir-apache-spark-demo/v1/conf/application-cloud.conf]"
```

### Option 3: SSH to EMR master and use spark-submit

```bash
# SSH to EMR master node
aws emr ssh --cluster-id j-OEUS8V7M7YAM --key-pair-file your-key.pem

# On the master node, run spark-submit
spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 4g --executor-memory 8g \
    --class com.qwshen.etl.Launcher \
    s3://samir-apache-spark-demo/v1/lib/spark-etl-framework.jar \
    --pipeline-def s3://samir-apache-spark-demo/v1/pipelines/pipeline_fileRead-fileWrite.xml \
    --application-conf s3://samir-apache-spark-demo/v1/conf/application-cloud.conf
```

## Monitoring

### Check step status

```bash
aws emr describe-step --cluster-id j-OEUS8V7M7YAM --step-id <step-id>
```

### View logs

Logs are stored in the S3 bucket configured for your EMR cluster. You can also use:

```bash
# View YARN logs
aws emr ssh --cluster-id j-OEUS8V7M7YAM --key-pair-file your-key.pem \
    --command 'yarn logs -applicationId <application-id>'
```

## Output

After successful execution, the output will be available at:
- Features: `s3://samir-apache-spark-demo/v1/output/features/`
- Staging (debug): `s3://samir-apache-spark-demo/v1/staging/events/`
- Metrics: `s3://samir-apache-spark-demo/v1/metrics/events/`

The output is partitioned by `gender` and `interested` fields for optimized querying.

## Switching Between Environments

To run on-premises instead of cloud:

```bash
spark-submit --master local[*] --deploy-mode client \
    --class com.qwshen.etl.Launcher \
    target/spark-etl-framework-*.jar \
    --pipeline-def src/test/resources/pipelines/pipeline_fileRead-fileWrite.xml \
    --application-conf src/test/resources/application-onprem.conf
```
