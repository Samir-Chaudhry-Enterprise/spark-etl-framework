#!/bin/bash
# EMR Job Submission Script for pipeline_fileRead-fileWrite.xml
#
# This script creates an EMR cluster with a bootstrap action to copy config files
# from S3 to local filesystem (required because the framework's FileChannel doesn't
# support S3 URLs directly), then submits the Spark ETL job.
#
# Usage:
#   ./submit-emr-job.sh [--use-existing-cluster <cluster-id>]

set -e

# Configuration
S3_BUCKET="s3://samir-apache-spark-demo"
JOB_NAME="spark-etl-fileRead-fileWrite"

# S3 paths for artifacts
JAR_PATH="${S3_BUCKET}/v1/lib/spark-etl-framework.jar"
S3_PIPELINE_PATH="${S3_BUCKET}/v1/pipelines/pipeline_fileRead-fileWrite.xml"
S3_CONFIG_PATH="${S3_BUCKET}/v1/conf/application-cloud.conf"
S3_SCRIPTS_PATH="${S3_BUCKET}/v1/scripts"

# Local paths (where bootstrap action copies files)
LOCAL_PIPELINE_PATH="/home/hadoop/pipeline.xml"
LOCAL_CONFIG_PATH="/home/hadoop/application-cloud.conf"
LOCAL_SCRIPTS_PATH="/home/hadoop/scripts"

# Spark configuration
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="8g"
DEPLOY_MODE="cluster"

# EMR cluster configuration
# Note: S3 bucket samir-apache-spark-demo is in us-east-2
CLUSTER_NAME="${EMR_CLUSTER_NAME:-spark-etl-migration}"
RELEASE_LABEL="${EMR_RELEASE_LABEL:-emr-6.15.0}"
MASTER_INSTANCE_TYPE="${EMR_MASTER_INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_TYPE="${EMR_CORE_INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_COUNT="${EMR_CORE_INSTANCE_COUNT:-2}"
LOG_URI="${EMR_LOG_URI:-${S3_BUCKET}/v1/logs/}"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"

# Parse command line arguments
USE_EXISTING_CLUSTER=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --use-existing-cluster)
            USE_EXISTING_CLUSTER="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --use-existing-cluster <cluster-id>  Use an existing cluster"
            echo "  --help                               Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=============================================="
echo "EMR Spark ETL Job Submission"
echo "=============================================="
echo "S3 Bucket: ${S3_BUCKET}"
echo "JAR Path: ${JAR_PATH}"
echo "Pipeline: ${S3_PIPELINE_PATH} -> ${LOCAL_PIPELINE_PATH}"
echo "Config: ${S3_CONFIG_PATH} -> ${LOCAL_CONFIG_PATH}"
echo "Region: ${REGION}"
echo "=============================================="

# Export region for AWS CLI
export AWS_DEFAULT_REGION="${REGION}"

# Create and upload bootstrap script
BOOTSTRAP_SCRIPT_CONTENT='#!/bin/bash
set -e
echo "Copying config files from S3 to local filesystem..."
mkdir -p /home/hadoop/scripts
aws s3 cp s3://samir-apache-spark-demo/v1/pipelines/pipeline_fileRead-fileWrite.xml /home/hadoop/pipeline.xml
aws s3 cp s3://samir-apache-spark-demo/v1/conf/application-cloud.conf /home/hadoop/application-cloud.conf
aws s3 cp --recursive s3://samir-apache-spark-demo/v1/scripts/ /home/hadoop/scripts/
chmod 644 /home/hadoop/pipeline.xml /home/hadoop/application-cloud.conf
find /home/hadoop/scripts -type f -exec chmod 644 {} \;
find /home/hadoop/scripts -type d -exec chmod 755 {} \;
echo "Config files copied successfully!"
ls -la /home/hadoop/
'

BOOTSTRAP_S3_PATH="${S3_BUCKET}/v1/bootstrap/copy-configs.sh"
echo "Uploading bootstrap script to ${BOOTSTRAP_S3_PATH}..."
echo "$BOOTSTRAP_SCRIPT_CONTENT" | aws s3 cp - "$BOOTSTRAP_S3_PATH"

# Build the step arguments - use LOCAL paths for config files
STEP_ARGS="--class,com.qwshen.etl.Launcher,--master,yarn,--deploy-mode,${DEPLOY_MODE},--driver-memory,${DRIVER_MEMORY},--executor-memory,${EXECUTOR_MEMORY},--conf,spark.hadoop.fs.s3a.endpoint.region=us-east-2,--conf,spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com,${JAR_PATH},--pipeline-def,${LOCAL_PIPELINE_PATH},--application-conf,${LOCAL_CONFIG_PATH}"

if [ -n "$USE_EXISTING_CLUSTER" ]; then
    CLUSTER_ID="$USE_EXISTING_CLUSTER"
    echo "Using existing cluster: ${CLUSTER_ID}"
    echo "Note: Bootstrap action only runs during cluster creation."
    echo "Make sure config files exist at ${LOCAL_PIPELINE_PATH} and ${LOCAL_CONFIG_PATH}"
    
    # Submit step to existing cluster
    STEP_ID=$(aws emr add-steps \
        --cluster-id "${CLUSTER_ID}" \
        --steps "Type=Spark,Name=${JOB_NAME},ActionOnFailure=CONTINUE,Args=[${STEP_ARGS}]" \
        --query 'StepIds[0]' \
        --output text)
    
    echo "Step submitted: ${STEP_ID}"
else
    echo ""
    echo "Creating EMR Cluster with bootstrap action..."
    echo "Cluster Name: ${CLUSTER_NAME}"
    echo "Release: ${RELEASE_LABEL}"
    echo "Master: ${MASTER_INSTANCE_TYPE}"
    echo "Core: ${CORE_INSTANCE_COUNT} x ${CORE_INSTANCE_TYPE}"
    echo "Auto-terminate: enabled"
    echo "=============================================="
    
    # Create cluster with bootstrap action and step
    CLUSTER_ID=$(aws emr create-cluster \
        --name "${CLUSTER_NAME}" \
        --release-label "${RELEASE_LABEL}" \
        --applications Name=Spark Name=Hadoop \
        --instance-groups "[{\"Name\":\"Master\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${MASTER_INSTANCE_TYPE}\",\"InstanceCount\":1},{\"Name\":\"Core\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${CORE_INSTANCE_TYPE}\",\"InstanceCount\":${CORE_INSTANCE_COUNT}}]" \
        --configurations '[{"Classification":"spark-defaults","Properties":{"spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem","spark.hadoop.fs.s3a.aws.credentials.provider":"com.amazonaws.auth.InstanceProfileCredentialsProvider","spark.hadoop.fs.s3a.endpoint":"s3.us-east-2.amazonaws.com","spark.hadoop.fs.s3a.endpoint.region":"us-east-2","spark.hadoop.fs.s3a.fast.upload":"true","spark.hadoop.fs.s3a.connection.maximum":"100"}},{"Classification":"core-site","Properties":{"fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem","fs.s3a.aws.credentials.provider":"com.amazonaws.auth.InstanceProfileCredentialsProvider","fs.s3a.endpoint":"s3.us-east-2.amazonaws.com","fs.s3a.endpoint.region":"us-east-2"}}]' \
        --bootstrap-actions "[{\"Path\":\"${BOOTSTRAP_S3_PATH}\",\"Name\":\"CopyConfigFiles\"}]" \
        --log-uri "${LOG_URI}" \
        --auto-terminate \
        --use-default-roles \
        --steps "Type=Spark,Name=${JOB_NAME},ActionOnFailure=TERMINATE_CLUSTER,Args=[${STEP_ARGS}]" \
        --query 'ClusterId' \
        --output text)
    
    echo "Cluster created: ${CLUSTER_ID}"
    
    # Get step ID
    sleep 5
    STEP_ID=$(aws emr list-steps --cluster-id "${CLUSTER_ID}" --query 'Steps[0].Id' --output text)
    echo "Step ID: ${STEP_ID}"
fi

echo ""
echo "Monitoring step status..."

# Monitor step status
while true; do
    STEP_STATUS=$(aws emr describe-step \
        --cluster-id "${CLUSTER_ID}" \
        --step-id "${STEP_ID}" \
        --query 'Step.Status.State' \
        --output text 2>/dev/null || echo "UNKNOWN")
    
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Step status: ${STEP_STATUS}"
    
    case "$STEP_STATUS" in
        "COMPLETED")
            echo ""
            echo "=============================================="
            echo "Job completed successfully!"
            echo "=============================================="
            echo "Output location: ${S3_BUCKET}/v1/output/"
            echo "Logs location: ${LOG_URI}${CLUSTER_ID}/steps/${STEP_ID}/"
            echo "=============================================="
            exit 0
            ;;
        "FAILED"|"CANCELLED")
            echo ""
            echo "=============================================="
            echo "Job failed or was cancelled!"
            echo "=============================================="
            echo "Check logs at: ${LOG_URI}${CLUSTER_ID}/steps/${STEP_ID}/"
            exit 1
            ;;
        "UNKNOWN")
            # Cluster may have terminated, check cluster status
            CLUSTER_STATUS=$(aws emr describe-cluster --cluster-id "${CLUSTER_ID}" --query 'Cluster.Status.State' --output text 2>/dev/null || echo "UNKNOWN")
            if [ "$CLUSTER_STATUS" = "TERMINATED" ]; then
                REASON=$(aws emr describe-cluster --cluster-id "${CLUSTER_ID}" --query 'Cluster.Status.StateChangeReason.Code' --output text 2>/dev/null)
                if [ "$REASON" = "ALL_STEPS_COMPLETED" ]; then
                    echo ""
                    echo "=============================================="
                    echo "Job completed successfully!"
                    echo "=============================================="
                    echo "Output location: ${S3_BUCKET}/v1/output/"
                    exit 0
                else
                    echo "Cluster terminated with reason: $REASON"
                    exit 1
                fi
            fi
            sleep 30
            ;;
        *)
            sleep 30
            ;;
    esac
done
