#!/bin/bash
# EMR Job Submission Script for Cloud Migration
# This script submits the Spark ETL job to an AWS EMR cluster

set -e

# Configuration - Update these values for your environment
CLUSTER_ID="${EMR_CLUSTER_ID:-j-OEUS8V7M7YAM}"
S3_BUCKET="s3://samir-apache-spark-demo"
S3_PREFIX="v1"

# S3 Paths
JAR_PATH="${S3_BUCKET}/${S3_PREFIX}/lib/spark-etl-framework.jar"
PIPELINE_PATH="${S3_BUCKET}/${S3_PREFIX}/pipelines/pipeline_fileRead-fileWrite.xml"
CONFIG_PATH="${S3_BUCKET}/${S3_PREFIX}/conf/application-cloud.conf"

# Job Configuration
JOB_NAME="event-consolidation-cloud"
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="8g"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --jar-path)
            JAR_PATH="$2"
            shift 2
            ;;
        --pipeline)
            PIPELINE_PATH="$2"
            shift 2
            ;;
        --config)
            CONFIG_PATH="$2"
            shift 2
            ;;
        --job-name)
            JOB_NAME="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --cluster-id ID    EMR Cluster ID (default: \$EMR_CLUSTER_ID or j-OEUS8V7M7YAM)"
            echo "  --jar-path PATH    S3 path to the JAR file"
            echo "  --pipeline PATH    S3 path to the pipeline definition"
            echo "  --config PATH      S3 path to the application config"
            echo "  --job-name NAME    Name for the Spark job"
            echo "  --help             Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "EMR Job Submission"
echo "=========================================="
echo "Cluster ID:    ${CLUSTER_ID}"
echo "JAR Path:      ${JAR_PATH}"
echo "Pipeline:      ${PIPELINE_PATH}"
echo "Config:        ${CONFIG_PATH}"
echo "Job Name:      ${JOB_NAME}"
echo "=========================================="

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &>/dev/null; then
    echo "ERROR: AWS CLI is not configured. Please run 'aws configure' first."
    exit 1
fi

# Check if cluster exists and is running
CLUSTER_STATE=$(aws emr describe-cluster --cluster-id "${CLUSTER_ID}" --query 'Cluster.Status.State' --output text 2>/dev/null || echo "NOT_FOUND")

if [[ "${CLUSTER_STATE}" == "NOT_FOUND" ]]; then
    echo "ERROR: Cluster ${CLUSTER_ID} not found."
    exit 1
elif [[ "${CLUSTER_STATE}" != "WAITING" && "${CLUSTER_STATE}" != "RUNNING" ]]; then
    echo "WARNING: Cluster is in state '${CLUSTER_STATE}'. Job may not run immediately."
fi

echo "Cluster state: ${CLUSTER_STATE}"

# Submit the Spark step to EMR
echo ""
echo "Submitting Spark job to EMR..."

STEP_ID=$(aws emr add-steps \
    --cluster-id "${CLUSTER_ID}" \
    --steps "Type=Spark,Name=${JOB_NAME},ActionOnFailure=CONTINUE,Args=[--class,com.qwshen.etl.Launcher,--master,yarn,--deploy-mode,cluster,--driver-memory,${DRIVER_MEMORY},--executor-memory,${EXECUTOR_MEMORY},--conf,spark.yarn.submit.waitAppCompletion=true,${JAR_PATH},--pipeline-def,${PIPELINE_PATH},--application-conf,${CONFIG_PATH}]" \
    --query 'StepIds[0]' \
    --output text)

echo ""
echo "Step submitted successfully!"
echo "Step ID: ${STEP_ID}"
echo ""
echo "To monitor the step:"
echo "  aws emr describe-step --cluster-id ${CLUSTER_ID} --step-id ${STEP_ID}"
echo ""
echo "To view step logs (after completion):"
echo "  aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file <your-key.pem> --command 'yarn logs -applicationId <app-id>'"
echo ""
echo "Or check the S3 logs bucket configured for your cluster."

# Optionally wait for step completion
read -p "Do you want to wait for the step to complete? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Waiting for step to complete..."
    aws emr wait step-complete --cluster-id "${CLUSTER_ID}" --step-id "${STEP_ID}"
    
    STEP_STATE=$(aws emr describe-step --cluster-id "${CLUSTER_ID}" --step-id "${STEP_ID}" --query 'Step.Status.State' --output text)
    echo ""
    echo "Step completed with state: ${STEP_STATE}"
    
    if [[ "${STEP_STATE}" == "COMPLETED" ]]; then
        echo "Job completed successfully!"
        echo "Check output at: ${S3_BUCKET}/${S3_PREFIX}/output/features/"
    else
        echo "Job failed. Check logs for details."
        exit 1
    fi
fi
