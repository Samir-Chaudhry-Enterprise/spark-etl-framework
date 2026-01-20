#!/bin/bash
# EMR Job Submission Script for pipeline_fileRead-fileWrite.xml
#
# This script creates an EMR cluster with auto-terminate, submits the Spark ETL job,
# monitors execution, and the cluster automatically terminates after the job completes.
#
# Prerequisites:
# - AWS CLI configured with appropriate credentials
# - JAR, pipeline, and config files uploaded to S3
# - EC2 key pair (optional, for SSH access)
# - EMR service role and instance profile configured
#
# Usage:
#   ./submit-emr-job.sh [--use-existing-cluster <cluster-id>]
#
# Options:
#   --use-existing-cluster <cluster-id>  Use an existing cluster instead of creating a new one
#
# Environment Variables (required):
#   AWS_ACCESS_KEY_ID - AWS access key
#   AWS_SECRET_ACCESS_KEY - AWS secret key
#   AWS_DEFAULT_REGION - AWS region (default: us-east-1)
#
# Optional Environment Variables:
#   EMR_CLUSTER_NAME - Name for the EMR cluster (default: spark-etl-migration)
#   EMR_RELEASE_LABEL - EMR release version (default: emr-6.15.0)
#   EMR_MASTER_INSTANCE_TYPE - Master instance type (default: m5.xlarge)
#   EMR_CORE_INSTANCE_TYPE - Core instance type (default: m5.xlarge)
#   EMR_CORE_INSTANCE_COUNT - Number of core instances (default: 2)
#   EMR_LOG_URI - S3 path for EMR logs (default: s3://samir-apache-spark-demo/emr-logs/)
#   EC2_KEY_NAME - EC2 key pair name for SSH access (optional)
#   EMR_SUBNET_ID - Subnet ID for the cluster (optional)
#
set -e

# Configuration
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

# EMR cluster configuration (can be overridden via environment variables)
CLUSTER_NAME="${EMR_CLUSTER_NAME:-spark-etl-migration}"
RELEASE_LABEL="${EMR_RELEASE_LABEL:-emr-6.15.0}"
MASTER_INSTANCE_TYPE="${EMR_MASTER_INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_TYPE="${EMR_CORE_INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_COUNT="${EMR_CORE_INSTANCE_COUNT:-2}"
LOG_URI="${EMR_LOG_URI:-${S3_BUCKET}/emr-logs/}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Parse command line arguments
USE_EXISTING_CLUSTER=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --use-existing-cluster)
            USE_EXISTING_CLUSTER="$2"
            shift 2
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
echo "Pipeline: ${PIPELINE_PATH}"
echo "Config: ${CONFIG_PATH}"
echo "Region: ${REGION}"
echo "=============================================="

# Check if AWS credentials are set
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "ERROR: AWS credentials not set. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
    exit 1
fi

# Export region for AWS CLI
export AWS_DEFAULT_REGION="${REGION}"

# Function to wait for cluster to be ready
wait_for_cluster_ready() {
    local cluster_id=$1
    echo "Waiting for cluster ${cluster_id} to be ready..."
    
    while true; do
        CLUSTER_STATE=$(aws emr describe-cluster \
            --cluster-id "${cluster_id}" \
            --query 'Cluster.Status.State' \
            --output text)
        
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Cluster state: ${CLUSTER_STATE}"
        
        case "$CLUSTER_STATE" in
            "WAITING")
                echo "Cluster is ready!"
                return 0
                ;;
            "RUNNING")
                echo "Cluster is running (bootstrapping may still be in progress)..."
                sleep 30
                ;;
            "STARTING"|"BOOTSTRAPPING")
                sleep 30
                ;;
            "TERMINATED"|"TERMINATED_WITH_ERRORS"|"TERMINATING")
                echo "ERROR: Cluster terminated unexpectedly"
                return 1
                ;;
            *)
                sleep 30
                ;;
        esac
    done
}

# Function to create EMR cluster
create_cluster() {
    echo ""
    echo "=============================================="
    echo "Creating EMR Cluster"
    echo "=============================================="
    echo "Cluster Name: ${CLUSTER_NAME}"
    echo "Release: ${RELEASE_LABEL}"
    echo "Master: ${MASTER_INSTANCE_TYPE}"
    echo "Core: ${CORE_INSTANCE_COUNT} x ${CORE_INSTANCE_TYPE}"
    echo "Auto-terminate: enabled"
    echo "=============================================="
    
    # Build instance groups JSON
    INSTANCE_GROUPS='[
        {
            "Name": "Master",
            "InstanceGroupType": "MASTER",
            "InstanceType": "'${MASTER_INSTANCE_TYPE}'",
            "InstanceCount": 1
        },
        {
            "Name": "Core",
            "InstanceGroupType": "CORE",
            "InstanceType": "'${CORE_INSTANCE_TYPE}'",
            "InstanceCount": '${CORE_INSTANCE_COUNT}'
        }
    ]'
    
    # Build applications JSON
    APPLICATIONS='[
        {"Name": "Spark"},
        {"Name": "Hadoop"}
    ]'
    
    # Build configurations JSON for S3A
    CONFIGURATIONS='[
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
                "spark.hadoop.fs.s3a.fast.upload": "true",
                "spark.hadoop.fs.s3a.connection.maximum": "100"
            }
        },
        {
            "Classification": "core-site",
            "Properties": {
                "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
            }
        }
    ]'
    
    # Build the create-cluster command
    CREATE_CMD="aws emr create-cluster \
        --name '${CLUSTER_NAME}' \
        --release-label '${RELEASE_LABEL}' \
        --applications '${APPLICATIONS}' \
        --instance-groups '${INSTANCE_GROUPS}' \
        --configurations '${CONFIGURATIONS}' \
        --log-uri '${LOG_URI}' \
        --auto-terminate \
        --use-default-roles \
        --query 'ClusterId' \
        --output text"
    
    # Add optional EC2 key name if provided
    if [ -n "$EC2_KEY_NAME" ]; then
        CREATE_CMD="${CREATE_CMD} --ec2-attributes KeyName=${EC2_KEY_NAME}"
    fi
    
    # Add subnet if provided
    if [ -n "$EMR_SUBNET_ID" ]; then
        CREATE_CMD="${CREATE_CMD} --ec2-attributes SubnetId=${EMR_SUBNET_ID}"
    fi
    
    echo "Creating cluster..."
    CLUSTER_ID=$(eval ${CREATE_CMD})
    
    if [ -z "$CLUSTER_ID" ]; then
        echo "ERROR: Failed to create cluster"
        exit 1
    fi
    
    echo "Cluster created: ${CLUSTER_ID}"
    echo ""
    
    # Wait for cluster to be ready
    wait_for_cluster_ready "${CLUSTER_ID}"
    
    echo "${CLUSTER_ID}"
}

# Determine cluster ID
if [ -n "$USE_EXISTING_CLUSTER" ]; then
    CLUSTER_ID="$USE_EXISTING_CLUSTER"
    echo "Using existing cluster: ${CLUSTER_ID}"
    
    # Check cluster status
    CLUSTER_STATUS=$(aws emr describe-cluster \
        --cluster-id "${CLUSTER_ID}" \
        --query 'Cluster.Status.State' \
        --output text 2>/dev/null)
    
    if [ "$CLUSTER_STATUS" != "WAITING" ] && [ "$CLUSTER_STATUS" != "RUNNING" ]; then
        echo "ERROR: Cluster ${CLUSTER_ID} is not in WAITING or RUNNING state. Current state: ${CLUSTER_STATUS}"
        exit 1
    fi
    
    echo "Cluster status: ${CLUSTER_STATUS}"
else
    # Create a new cluster with auto-terminate
    CLUSTER_ID=$(create_cluster)
fi

echo ""
echo "=============================================="
echo "Submitting Spark Job"
echo "=============================================="
echo "Cluster ID: ${CLUSTER_ID}"
echo "Job Name: ${JOB_NAME}"
echo "=============================================="

# Submit the Spark step
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
            echo "Output location: ${S3_BUCKET}/v1/output/features/"
            echo "Logs location: ${LOG_URI}${CLUSTER_ID}/steps/${STEP_ID}/"
            if [ -z "$USE_EXISTING_CLUSTER" ]; then
                echo ""
                echo "Note: Cluster will auto-terminate now that all steps are complete."
            fi
            echo "=============================================="
            exit 0
            ;;
        "FAILED"|"CANCELLED")
            echo ""
            echo "=============================================="
            echo "Job failed or was cancelled!"
            echo "=============================================="
            echo "Check logs at: ${LOG_URI}${CLUSTER_ID}/steps/${STEP_ID}/"
            
            # Get failure reason
            FAILURE_REASON=$(aws emr describe-step \
                --cluster-id "${CLUSTER_ID}" \
                --step-id "${STEP_ID}" \
                --query 'Step.Status.FailureDetails.Reason' \
                --output text)
            
            if [ "$FAILURE_REASON" != "None" ] && [ -n "$FAILURE_REASON" ]; then
                echo "Failure reason: ${FAILURE_REASON}"
            fi
            
            echo "=============================================="
            exit 1
            ;;
        *)
            sleep 30
            ;;
    esac
done
