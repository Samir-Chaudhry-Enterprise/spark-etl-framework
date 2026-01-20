#!/bin/bash
# EMR Job Submission Script for pipeline_fileRead-fileWrite.xml
#
# This script submits the Spark ETL job to an AWS EMR cluster.
#
# Prerequisites:
# - AWS CLI configured with appropriate credentials
# - EMR cluster running and accessible
# - JAR, pipeline, and config files uploaded to S3
#
# Usage:
#   ./submit-emr-job.sh
#
# Environment Variables (required):
#   AWS_ACCESS_KEY_ID - AWS access key
#   AWS_SECRET_ACCESS_KEY - AWS secret key
#
# Configuration:
CLUSTER_ID="${EMR_CLUSTER_ID:-j-OEUS8V7M7YAM}"
S3_BUCKET="s3://samir-apache-spark-demo"
JOB_NAME="spark-etl-fileRead-fileWrite"

# S3 paths
JAR_PATH="${S3_BUCKET}/v1/lib/spark-etl-framework.jar"
PIPELINE_PATH="${S3_BUCKET}/v1/pipelines/pipeline_fileRead-fileWrite.xml"
CONFIG_PATH="${S3_BUCKET}/v1/conf/application-cloud.conf"

# Spark configuration
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="8g"
DEPLOY_MODE="cluster"

echo "=============================================="
echo "EMR Job Submission"
echo "=============================================="
echo "Cluster ID: ${CLUSTER_ID}"
echo "JAR Path: ${JAR_PATH}"
echo "Pipeline: ${PIPELINE_PATH}"
echo "Config: ${CONFIG_PATH}"
echo "=============================================="

# Check if AWS credentials are set
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "ERROR: AWS credentials not set. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
    exit 1
fi

# Check cluster status
echo "Checking cluster status..."
CLUSTER_STATUS=$(aws emr describe-cluster --cluster-id "${CLUSTER_ID}" --query 'Cluster.Status.State' --output text 2>/dev/null)

if [ "$CLUSTER_STATUS" != "WAITING" ] && [ "$CLUSTER_STATUS" != "RUNNING" ]; then
    echo "ERROR: Cluster ${CLUSTER_ID} is not in WAITING or RUNNING state. Current state: ${CLUSTER_STATUS}"
    exit 1
fi

echo "Cluster status: ${CLUSTER_STATUS}"

# Submit the Spark step
echo "Submitting Spark job..."
STEP_ID=$(aws emr add-steps \
    --cluster-id "${CLUSTER_ID}" \
    --steps "Type=Spark,Name=${JOB_NAME},ActionOnFailure=CONTINUE,Args=[--class,com.qwshen.etl.Launcher,--master,yarn,--deploy-mode,${DEPLOY_MODE},--driver-memory,${DRIVER_MEMORY},--executor-memory,${EXECUTOR_MEMORY},--conf,spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider,${JAR_PATH},--pipeline-def,${PIPELINE_PATH},--application-conf,${CONFIG_PATH}]" \
    --query 'StepIds[0]' \
    --output text)

if [ -z "$STEP_ID" ]; then
    echo "ERROR: Failed to submit job"
    exit 1
fi

echo "Step submitted successfully!"
echo "Step ID: ${STEP_ID}"
echo ""

# Monitor step status
echo "Monitoring step status..."
while true; do
    STEP_STATUS=$(aws emr describe-step \
        --cluster-id "${CLUSTER_ID}" \
        --step-id "${STEP_ID}" \
        --query 'Step.Status.State' \
        --output text)
    
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Step status: ${STEP_STATUS}"
    
    case "$STEP_STATUS" in
        "COMPLETED")
            echo ""
            echo "=============================================="
            echo "Job completed successfully!"
            echo "=============================================="
            exit 0
            ;;
        "FAILED"|"CANCELLED")
            echo ""
            echo "=============================================="
            echo "Job failed or was cancelled!"
            echo "Check logs at: ${S3_BUCKET}/logs/${CLUSTER_ID}/steps/${STEP_ID}/"
            echo "=============================================="
            
            # Get failure reason
            FAILURE_REASON=$(aws emr describe-step \
                --cluster-id "${CLUSTER_ID}" \
                --step-id "${STEP_ID}" \
                --query 'Step.Status.FailureDetails.Reason' \
                --output text)
            
            if [ "$FAILURE_REASON" != "None" ]; then
                echo "Failure reason: ${FAILURE_REASON}"
            fi
            
            exit 1
            ;;
        *)
            sleep 30
            ;;
    esac
done
