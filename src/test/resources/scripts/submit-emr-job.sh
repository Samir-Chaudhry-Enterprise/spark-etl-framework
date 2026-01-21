#!/bin/bash

# EMR Job Submission Script for Spark ETL Framework
# This script supports two modes:
# 1. --use-existing-cluster <cluster-id>: Submit job to an existing EMR cluster
# 2. --create-cluster: Create a new EMR cluster, submit the job, and auto-terminate

set -e

# Configuration
S3_BUCKET="s3://samir-apache-spark-demo"
JAR_PATH="${S3_BUCKET}/v1/lib/spark-etl-framework.jar"
PIPELINE_PATH="${S3_BUCKET}/v1/pipelines/pipeline_fileRead-fileWrite.xml"
CONFIG_PATH="${S3_BUCKET}/v1/conf/application-cloud.conf"
LOG_BUCKET="${S3_BUCKET}/v1/logs"
JOB_NAME="spark-etl-migration"

# EMR Configuration
EMR_RELEASE="emr-6.15.0"
MASTER_INSTANCE_TYPE="m5.xlarge"
CORE_INSTANCE_TYPE="m5.xlarge"
CORE_INSTANCE_COUNT=2
SERVICE_ROLE="EMR_DefaultRole"
INSTANCE_PROFILE="EMR_EC2_DefaultRole"

# Parse command line arguments
MODE=""
CLUSTER_ID=""
AUTO_TERMINATE="true"

while [[ $# -gt 0 ]]; do
    case $1 in
        --use-existing-cluster)
            MODE="existing"
            CLUSTER_ID="$2"
            shift 2
            ;;
        --create-cluster)
            MODE="create"
            shift
            ;;
        --no-auto-terminate)
            AUTO_TERMINATE="false"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --use-existing-cluster <cluster-id>  Submit job to an existing EMR cluster"
            echo "  --create-cluster                     Create a new EMR cluster and submit job"
            echo "  --no-auto-terminate                  Keep cluster running after job completion"
            echo "  --help                               Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -z "$MODE" ]; then
    echo "Error: Please specify either --use-existing-cluster <cluster-id> or --create-cluster"
    exit 1
fi

# Spark submit arguments
SPARK_ARGS="--class,com.qwshen.etl.Launcher,--deploy-mode,cluster,--conf,spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem,--conf,spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,${JAR_PATH},--pipeline-def,${PIPELINE_PATH},--application-conf,${CONFIG_PATH}"

# Function to wait for step completion
wait_for_step() {
    local cluster_id=$1
    local step_id=$2
    local status=""
    
    echo "Waiting for step $step_id to complete..."
    
    while true; do
        status=$(aws emr describe-step --cluster-id "$cluster_id" --step-id "$step_id" --query 'Step.Status.State' --output text)
        
        case $status in
            COMPLETED)
                echo "Step completed successfully!"
                return 0
                ;;
            FAILED|CANCELLED)
                echo "Step $status. Fetching logs..."
                # Get logs from S3
                aws s3 ls "${LOG_BUCKET}/${cluster_id}/steps/${step_id}/" 2>/dev/null || echo "No logs found"
                return 1
                ;;
            *)
                echo "Step status: $status"
                sleep 30
                ;;
        esac
    done
}

# Function to wait for cluster to be ready
wait_for_cluster() {
    local cluster_id=$1
    local status=""
    
    echo "Waiting for cluster $cluster_id to be ready..."
    
    while true; do
        status=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.Status.State' --output text)
        
        case $status in
            WAITING|RUNNING)
                echo "Cluster is ready!"
                return 0
                ;;
            TERMINATED|TERMINATED_WITH_ERRORS)
                echo "Cluster terminated unexpectedly. Status: $status"
                return 1
                ;;
            *)
                echo "Cluster status: $status"
                sleep 30
                ;;
        esac
    done
}

if [ "$MODE" == "existing" ]; then
    echo "Submitting job to existing cluster: $CLUSTER_ID"
    
    # Add step to existing cluster
    STEP_ID=$(aws emr add-steps \
        --cluster-id "$CLUSTER_ID" \
        --steps "Type=Spark,Name=${JOB_NAME},ActionOnFailure=CONTINUE,Args=[${SPARK_ARGS}]" \
        --query 'StepIds[0]' \
        --output text)
    
    echo "Step submitted with ID: $STEP_ID"
    
    # Wait for step completion
    wait_for_step "$CLUSTER_ID" "$STEP_ID"
    
elif [ "$MODE" == "create" ]; then
    echo "Creating new EMR cluster..."
    
    # Build auto-terminate flag
    AUTO_TERM_FLAG=""
    if [ "$AUTO_TERMINATE" == "true" ]; then
        AUTO_TERM_FLAG="--auto-terminate"
    fi
    
    # Create cluster with step
    CLUSTER_ID=$(aws emr create-cluster \
        --name "${JOB_NAME}-cluster" \
        --release-label "$EMR_RELEASE" \
        --applications Name=Spark \
        --instance-groups "[{\"InstanceGroupType\":\"MASTER\",\"InstanceCount\":1,\"InstanceType\":\"${MASTER_INSTANCE_TYPE}\"},{\"InstanceGroupType\":\"CORE\",\"InstanceCount\":${CORE_INSTANCE_COUNT},\"InstanceType\":\"${CORE_INSTANCE_TYPE}\"}]" \
        --service-role "$SERVICE_ROLE" \
        --ec2-attributes "InstanceProfile=${INSTANCE_PROFILE}" \
        --log-uri "${LOG_BUCKET}/" \
        $AUTO_TERM_FLAG \
        --steps "Type=Spark,Name=${JOB_NAME},ActionOnFailure=TERMINATE_CLUSTER,Args=[${SPARK_ARGS}]" \
        --query 'ClusterId' \
        --output text)
    
    echo "Cluster created with ID: $CLUSTER_ID"
    
    # Wait for cluster to be ready (if not auto-terminating)
    if [ "$AUTO_TERMINATE" == "false" ]; then
        wait_for_cluster "$CLUSTER_ID"
    fi
    
    # Get step ID
    STEP_ID=$(aws emr list-steps --cluster-id "$CLUSTER_ID" --query 'Steps[0].Id' --output text)
    
    echo "Step ID: $STEP_ID"
    
    # Wait for step completion
    wait_for_step "$CLUSTER_ID" "$STEP_ID"
fi

echo ""
echo "Job submission complete!"
echo "Cluster ID: $CLUSTER_ID"
echo "Check output at: ${S3_BUCKET}/v1/output/"
