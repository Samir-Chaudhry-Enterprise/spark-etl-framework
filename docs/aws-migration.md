# AWS Migration Guide for pipeline_fileRead-fileWrite.xml

This document describes the migration of the `pipeline_fileRead-fileWrite.xml` Spark ETL job from on-premises infrastructure to AWS using S3 for storage and EMR for compute.

## Overview

The migration maintains the same pipeline definition file while using environment-specific configuration files to handle path differences between on-prem and cloud environments.

## Configuration Files

### application-onprem.conf
Configuration for running the job locally with local filesystem paths:
- `spark.master = "local[*]"` for local execution
- Local paths for input data (`./data/users`, `./data/train`)
- Local path for output (`./data/features`)
- Local path for scripts (`./scripts`)

### application-cloud.conf
Configuration for running the job on AWS EMR with S3 storage:
- `spark.master = "yarn"` for EMR execution
- S3 paths for input data (`s3://samir-apache-spark-demo/v1/input/users`, `s3://samir-apache-spark-demo/v1/input/train`)
- S3 path for output (`s3://samir-apache-spark-demo/v1/output/features`)
- S3 path for scripts (`s3://samir-apache-spark-demo/v1/scripts`)
- S3A Hadoop configurations for optimal performance

## S3 Path Mappings

| On-Prem Path | S3 Path |
|--------------|---------|
| `./data/users` | `s3://samir-apache-spark-demo/v1/input/users` |
| `./data/train` | `s3://samir-apache-spark-demo/v1/input/train` |
| `./data/features` | `s3://samir-apache-spark-demo/v1/output/features` |
| `./scripts` | `s3://samir-apache-spark-demo/v1/scripts` |

## S3A Optimizations

The cloud configuration includes the following S3A settings for optimal performance:

```
fs.s3a.impl = "org.apache.hadoop.fs.s3a.S3AFileSystem"
fs.s3a.aws.credentials.provider = "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
fs.s3a.connection.maximum = 100
fs.s3a.fast.upload = true
fs.s3a.fast.upload.buffer = "bytebuffer"
fs.s3a.multipart.size = "104857600"
fs.s3a.block.size = "134217728"
```

## AWS Credentials

The cloud configuration uses environment variables for AWS credentials:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

These must be set before running the job on EMR.

## Running the Job

### On-Prem Execution

```bash
spark-submit \
  --class com.qwshen.etl.Launcher \
  --master local[*] \
  target/spark-etl-framework_3.5_2.12-1.0.1-jar-with-dependencies.jar \
  --pipeline-def src/test/resources/pipelines/pipeline_fileRead-fileWrite.xml \
  --application-conf src/test/resources/application-onprem.conf
```

### EMR Execution

The `submit-emr-job.sh` script handles the full lifecycle: creating an EMR cluster, running the job, and auto-terminating the cluster when complete.

**Basic usage (creates new cluster with auto-terminate):**
```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
export AWS_DEFAULT_REGION=us-east-1

./submit-emr-job.sh
```

**Use an existing cluster:**
```bash
./submit-emr-job.sh --use-existing-cluster j-XXXXXXXXXXXXX
```

**Customize cluster configuration via environment variables:**
```bash
export EMR_CLUSTER_NAME="my-spark-job"
export EMR_RELEASE_LABEL="emr-6.15.0"
export EMR_MASTER_INSTANCE_TYPE="m5.xlarge"
export EMR_CORE_INSTANCE_TYPE="m5.xlarge"
export EMR_CORE_INSTANCE_COUNT="2"
export EMR_LOG_URI="s3://my-bucket/emr-logs/"
export EC2_KEY_NAME="my-key-pair"  # Optional, for SSH access
export EMR_SUBNET_ID="subnet-xxx"  # Optional

./submit-emr-job.sh
```

The script will:
1. Create an EMR cluster with Spark and Hadoop installed
2. Configure S3A settings for optimal S3 access
3. Wait for the cluster to be ready
4. Submit the Spark job
5. Monitor job progress
6. Auto-terminate the cluster when the job completes

**Manual EMR step submission (if using existing cluster):**
```bash
aws emr add-steps \
  --cluster-id <cluster-id> \
  --steps "Type=Spark,Name=spark-etl-fileRead-fileWrite,ActionOnFailure=CONTINUE,Args=[--class,com.qwshen.etl.Launcher,--master,yarn,--deploy-mode,cluster,--driver-memory,4g,--executor-memory,8g,s3://samir-apache-spark-demo/v1/lib/spark-etl-framework.jar,--pipeline-def,s3://samir-apache-spark-demo/v1/pipelines/pipeline_fileRead-fileWrite.xml,--application-conf,s3://samir-apache-spark-demo/v1/conf/application-cloud.conf]"
```

## Validation

### Validation Test Suite

The `MigrationValidationTest.scala` test suite validates:
- Schema validation: Output schema matches expected columns
- Input validation: Users and train data load correctly with expected row counts
- Data quality: No null user_ids, valid gender values, valid birthyear range
- Business rules: Join produces expected row counts

### Output Comparison

Use the `compare-outputs.py` script to compare on-prem and cloud outputs:

```bash
python compare-outputs.py \
  --onprem-path /tmp/onprem_baseline \
  --cloud-path /tmp/cloud_output
```

The script compares:
- Row counts per partition
- Data values (excluding runtime-generated `process_date` column)
- Reports pass/fail status for each partition

## S3 Artifacts

The following artifacts are deployed to S3:

| Artifact | S3 Path |
|----------|---------|
| JAR | `s3://samir-apache-spark-demo/v1/lib/spark-etl-framework.jar` |
| Pipeline | `s3://samir-apache-spark-demo/v1/pipelines/pipeline_fileRead-fileWrite.xml` |
| Config | `s3://samir-apache-spark-demo/v1/conf/application-cloud.conf` |
| Input Users | `s3://samir-apache-spark-demo/v1/input/users/` |
| Input Train | `s3://samir-apache-spark-demo/v1/input/train/` |
| Scripts | `s3://samir-apache-spark-demo/v1/scripts/` |
| Output | `s3://samir-apache-spark-demo/v1/output/features/` |

## Output Schema

The pipeline produces output with the following schema, partitioned by `gender` and `interested`:

| Column | Type | Description |
|--------|------|-------------|
| user_id | long | User identifier |
| gender | string | User gender (partition column) |
| birthyear | int | User birth year |
| timestamp | string | Event timestamp |
| interested | int | Interest flag (partition column) |
| process_date | string | Runtime-generated process date (excluded from comparison) |
| event_id | long | Event identifier |

## Notes

- The pipeline definition file (`pipeline_fileRead-fileWrite.xml`) remains unchanged
- All path differences are handled via configuration variables
- The `process_date` column contains runtime-generated values and should be excluded from output comparisons
- S3 handles empty partitions differently than local filesystem; use `__HIVE_DEFAULT_PARTITION__` for null partition values
