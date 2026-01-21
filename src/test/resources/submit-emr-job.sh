#!/bin/bash

# EMR Job Submission Script for pipeline_fileRead-fileWrite.xml
# Supports both existing cluster and new cluster creation modes

set -e

# Configuration
S3_BUCKET="s3://samir-apache-spark-demo"
AWS_REGION="us-east-2"
JOB_NAME="spark-etl-fileRead-fileWrite"
JAR_PATH="${S3_BUCKET}/v1/lib/spark-etl-framework.jar"
PIPELINE_PATH="${S3_BUCKET}/v1/pipelines/pipeline_fileRead-fileWrite.xml"
CONFIG_PATH="${S3_BUCKET}/v1/conf/application-cloud.conf"
SQL_SCRIPT_PATH="${S3_BUCKET}/v1/scripts/transform-user-train.sql"
LOGS_BUCKET="${S3_BUCKET}/v1/logs"

# EMR cluster configuration for new cluster creation
EMR_RELEASE="emr-6.15.0"
MASTER_INSTANCE_TYPE="m5.xlarge"
CORE_INSTANCE_TYPE="m5.xlarge"
CORE_INSTANCE_COUNT=2

usage() {
    echo "Usage: $0 [--use-existing-cluster <cluster-id>] [--create-cluster] [--auto-terminate]"
    echo ""
    echo "Options:"
    echo "  --use-existing-cluster <cluster-id>  Submit job to an existing EMR cluster"
    echo "  --create-cluster                      Create a new EMR cluster and submit the job"
    echo "  --auto-terminate                      Auto-terminate cluster after job completion (only with --create-cluster)"
    echo ""
    exit 1
}

wait_for_step() {
    local cluster_id=$1
    local step_id=$2
    
    echo "Waiting for step $step_id to complete..."
    
    while true; do
        status=$(aws emr describe-step --cluster-id "$cluster_id" --step-id "$step_id" --region "$AWS_REGION" --query 'Step.Status.State' --output text)
        
        echo "Step status: $status"
        
        case $status in
            COMPLETED)
                echo "Step completed successfully!"
                return 0
                ;;
            FAILED|CANCELLED)
                echo "Step failed or was cancelled. Fetching logs..."
                fetch_logs "$cluster_id" "$step_id"
                return 1
                ;;
            *)
                sleep 30
                ;;
        esac
    done
}

fetch_logs() {
    local cluster_id=$1
    local step_id=$2
    
    echo "Fetching logs from S3..."
    aws s3 ls "${LOGS_BUCKET}/${cluster_id}/steps/${step_id}/" --recursive 2>/dev/null || echo "No logs found yet"
}

submit_to_existing_cluster() {
    local cluster_id=$1
    
    echo "Submitting job to existing cluster: $cluster_id"
    
    # Clear previous output
    echo "Clearing previous output..."
    aws s3 rm "${S3_BUCKET}/v1/output/features/" --recursive 2>/dev/null || true
    
    # Submit the step
    step_output=$(aws emr add-steps \
        --cluster-id "$cluster_id" \
        --region "$AWS_REGION" \
        --steps "[{
            \"Type\": \"Spark\",
            \"Name\": \"${JOB_NAME}\",
            \"ActionOnFailure\": \"CONTINUE\",
            \"Args\": [
                \"--class\", \"com.qwshen.etl.Launcher\",
                \"--master\", \"yarn\",
                \"--deploy-mode\", \"cluster\",
                \"--driver-memory\", \"4g\",
                \"--executor-memory\", \"8g\",
                \"--files\", \"${PIPELINE_PATH},${CONFIG_PATH},${SQL_SCRIPT_PATH}\",
                \"${JAR_PATH}\",
                \"--pipeline-def\", \"pipeline_fileRead-fileWrite.xml\",
                \"--application-conf\", \"application-cloud.conf\"
            ]
        }]")
    
    step_id=$(echo "$step_output" | jq -r '.StepIds[0]')
    echo "Step submitted with ID: $step_id"
    
    # Wait for step completion
    wait_for_step "$cluster_id" "$step_id"
    
    echo ""
    echo "| Metric | Value |"
    echo "|--------|-------|"
    echo "| EMR Cluster ID | $cluster_id |"
    echo "| Step ID | $step_id |"
    echo "| Region | $AWS_REGION |"
    echo "| Output Location | ${S3_BUCKET}/v1/output/features/ |"
}

create_cluster_and_submit() {
    local auto_terminate=$1
    
    echo "Creating new EMR cluster..."
    
    # Clear previous output
    echo "Clearing previous output..."
    aws s3 rm "${S3_BUCKET}/v1/output/features/" --recursive 2>/dev/null || true
    
    local terminate_flag=""
    if [ "$auto_terminate" = "true" ]; then
        terminate_flag="--auto-terminate"
    fi
    
    # Create cluster with step
    cluster_output=$(aws emr create-cluster \
        --name "${JOB_NAME}-cluster" \
        --release-label "$EMR_RELEASE" \
        --region "$AWS_REGION" \
        --applications Name=Spark Name=Hadoop \
        --instance-groups "[
            {\"InstanceGroupType\":\"MASTER\",\"InstanceCount\":1,\"InstanceType\":\"${MASTER_INSTANCE_TYPE}\"},
            {\"InstanceGroupType\":\"CORE\",\"InstanceCount\":${CORE_INSTANCE_COUNT},\"InstanceType\":\"${CORE_INSTANCE_TYPE}\"}
        ]" \
        --use-default-roles \
        --log-uri "${LOGS_BUCKET}/" \
        $terminate_flag \
        --steps "[{
            \"Type\": \"Spark\",
            \"Name\": \"${JOB_NAME}\",
            \"ActionOnFailure\": \"CONTINUE\",
            \"Args\": [
                \"--class\", \"com.qwshen.etl.Launcher\",
                \"--master\", \"yarn\",
                \"--deploy-mode\", \"cluster\",
                \"--driver-memory\", \"4g\",
                \"--executor-memory\", \"8g\",
                \"--files\", \"${PIPELINE_PATH},${CONFIG_PATH},${SQL_SCRIPT_PATH}\",
                \"${JAR_PATH}\",
                \"--pipeline-def\", \"pipeline_fileRead-fileWrite.xml\",
                \"--application-conf\", \"application-cloud.conf\"
            ]
        }]")
    
    cluster_id=$(echo "$cluster_output" | jq -r '.ClusterId')
    echo "Cluster created with ID: $cluster_id"
    
    # Wait for cluster to be ready
    echo "Waiting for cluster to be ready..."
    aws emr wait cluster-running --cluster-id "$cluster_id" --region "$AWS_REGION"
    
    # Get step ID
    steps_output=$(aws emr list-steps --cluster-id "$cluster_id" --region "$AWS_REGION")
    step_id=$(echo "$steps_output" | jq -r '.Steps[0].Id')
    
    echo "Step ID: $step_id"
    
    # Wait for step completion
    wait_for_step "$cluster_id" "$step_id"
    
    echo ""
    echo "| Metric | Value |"
    echo "|--------|-------|"
    echo "| EMR Cluster ID | $cluster_id |"
    echo "| Step ID | $step_id |"
    echo "| Region | $AWS_REGION |"
    echo "| Output Location | ${S3_BUCKET}/v1/output/features/ |"
}

# Parse arguments
CLUSTER_ID=""
CREATE_CLUSTER=false
AUTO_TERMINATE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --use-existing-cluster)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --create-cluster)
            CREATE_CLUSTER=true
            shift
            ;;
        --auto-terminate)
            AUTO_TERMINATE=true
            shift
            ;;
        *)
            usage
            ;;
    esac
done

# Validate arguments
if [ -n "$CLUSTER_ID" ] && [ "$CREATE_CLUSTER" = true ]; then
    echo "Error: Cannot use both --use-existing-cluster and --create-cluster"
    exit 1
fi

if [ -z "$CLUSTER_ID" ] && [ "$CREATE_CLUSTER" = false ]; then
    usage
fi

# Execute
if [ -n "$CLUSTER_ID" ]; then
    submit_to_existing_cluster "$CLUSTER_ID"
else
    create_cluster_and_submit "$AUTO_TERMINATE"
fi

echo ""
echo "Job submission complete!"
