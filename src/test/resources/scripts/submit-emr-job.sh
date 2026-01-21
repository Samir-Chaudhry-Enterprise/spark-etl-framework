#!/bin/bash
# EMR Job Submission Script for pipeline_fileRead-fileWrite.xml
#
# This script submits the Spark ETL job to EMR using the --files option
# to distribute config files (required because the framework's FileChannel
# doesn't support S3 URLs directly).
#
# Usage:
#   ./submit-emr-job.sh --use-existing-cluster <cluster-id>
#   ./submit-emr-job.sh --create-cluster

set -e

# Configuration
S3_BUCKET="s3://samir-apache-spark-demo"
JOB_NAME="spark-etl-fileRead-fileWrite"

# S3 paths for artifacts
JAR_PATH="${S3_BUCKET}/v1/lib/spark-etl-framework.jar"
PIPELINE_PATH="${S3_BUCKET}/v1/pipelines/pipeline_fileRead-fileWrite.xml"
CONFIG_PATH="${S3_BUCKET}/v1/conf/application-cloud.conf"
SQL_SCRIPT_PATH="${S3_BUCKET}/v1/scripts/transform-user-train.sql"

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
CREATE_CLUSTER=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --use-existing-cluster)
            USE_EXISTING_CLUSTER="$2"
            shift 2
            ;;
        --create-cluster)
            CREATE_CLUSTER="true"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --use-existing-cluster <cluster-id>  Submit job to an existing cluster"
            echo "  --create-cluster                     Create a new cluster and submit job"
            echo "  --help                               Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate arguments
if [ -z "$USE_EXISTING_CLUSTER" ] && [ -z "$CREATE_CLUSTER" ]; then
    echo "Error: Must specify either --use-existing-cluster <cluster-id> or --create-cluster"
    exit 1
fi

echo "=============================================="
echo "EMR Spark ETL Job Submission"
echo "=============================================="
echo "S3 Bucket: ${S3_BUCKET}"
echo "JAR Path: ${JAR_PATH}"
echo "Pipeline: ${PIPELINE_PATH}"
echo "Config: ${CONFIG_PATH}"
echo "SQL Script: ${SQL_SCRIPT_PATH}"
echo "Region: ${REGION}"
echo "=============================================="

# Export region for AWS CLI
export AWS_DEFAULT_REGION="${REGION}"

# Build the step JSON using --files to distribute config files
# Files are referenced by basename only (Spark downloads them to working directory)
STEP_JSON="[{
    \"Type\": \"Spark\",
    \"Name\": \"${JOB_NAME}\",
    \"ActionOnFailure\": \"CONTINUE\",
    \"Args\": [
        \"--class\", \"com.qwshen.etl.Launcher\",
        \"--master\", \"yarn\",
        \"--deploy-mode\", \"${DEPLOY_MODE}\",
        \"--driver-memory\", \"${DRIVER_MEMORY}\",
        \"--executor-memory\", \"${EXECUTOR_MEMORY}\",
        \"--conf\", \"spark.hadoop.fs.s3a.endpoint.region=us-east-2\",
        \"--conf\", \"spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com\",
        \"--files\", \"${PIPELINE_PATH},${CONFIG_PATH},${SQL_SCRIPT_PATH}\",
        \"${JAR_PATH}\",
        \"--pipeline-def\", \"pipeline_fileRead-fileWrite.xml\",
        \"--application-conf\", \"application-cloud.conf\"
    ]
}]"

if [ -n "$USE_EXISTING_CLUSTER" ]; then
    CLUSTER_ID="$USE_EXISTING_CLUSTER"
    echo ""
    echo "Using existing cluster: ${CLUSTER_ID}"
    echo ""
    
    # Submit step to existing cluster
    STEP_ID=$(aws emr add-steps \
        --cluster-id "${CLUSTER_ID}" \
        --steps "${STEP_JSON}" \
        --query 'StepIds[0]' \
        --output text)
    
    echo "Step submitted: ${STEP_ID}"
else
    echo ""
    echo "Creating EMR Cluster..."
    echo "Cluster Name: ${CLUSTER_NAME}"
    echo "Release: ${RELEASE_LABEL}"
    echo "Master: ${MASTER_INSTANCE_TYPE}"
    echo "Core: ${CORE_INSTANCE_COUNT} x ${CORE_INSTANCE_TYPE}"
    echo "Auto-terminate: enabled"
    echo "=============================================="
    
    # Create cluster with step (auto-terminates after job completes)
    CLUSTER_ID=$(aws emr create-cluster \
        --name "${CLUSTER_NAME}" \
        --release-label "${RELEASE_LABEL}" \
        --applications Name=Spark Name=Hadoop \
        --instance-groups "[{\"Name\":\"Master\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${MASTER_INSTANCE_TYPE}\",\"InstanceCount\":1},{\"Name\":\"Core\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${CORE_INSTANCE_TYPE}\",\"InstanceCount\":${CORE_INSTANCE_COUNT}}]" \
        --configurations '[{"Classification":"spark-defaults","Properties":{"spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem","spark.hadoop.fs.s3a.aws.credentials.provider":"com.amazonaws.auth.InstanceProfileCredentialsProvider","spark.hadoop.fs.s3a.endpoint":"s3.us-east-2.amazonaws.com","spark.hadoop.fs.s3a.endpoint.region":"us-east-2","spark.hadoop.fs.s3a.fast.upload":"true","spark.hadoop.fs.s3a.connection.maximum":"100"}},{"Classification":"core-site","Properties":{"fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem","fs.s3a.aws.credentials.provider":"com.amazonaws.auth.InstanceProfileCredentialsProvider","fs.s3a.endpoint":"s3.us-east-2.amazonaws.com","fs.s3a.endpoint.region":"us-east-2"}}]' \
        --log-uri "${LOG_URI}" \
        --auto-terminate \
        --use-default-roles \
        --steps "${STEP_JSON}" \
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
