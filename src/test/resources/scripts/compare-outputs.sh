#!/bin/bash

# Output Comparison Script for Spark ETL Migration
# This script compares on-prem and cloud outputs to validate the migration
# It checks row counts and data values for each partition

set -e

# Configuration
S3_BUCKET="s3://samir-apache-spark-demo"
CLOUD_OUTPUT_PATH="${S3_BUCKET}/v1/output"
ONPREM_OUTPUT_PATH="/tmp/events/features"
TEMP_DIR="/tmp/migration_comparison"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse command line arguments
ONPREM_PATH=""
CLOUD_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --onprem-path)
            ONPREM_PATH="$2"
            shift 2
            ;;
        --cloud-path)
            CLOUD_PATH="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --onprem-path <path>  Path to on-prem output (default: /tmp/events/features)"
            echo "  --cloud-path <path>   S3 path to cloud output (default: s3://samir-apache-spark-demo/v1/output)"
            echo "  --help                Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Use defaults if not specified
ONPREM_PATH="${ONPREM_PATH:-$ONPREM_OUTPUT_PATH}"
CLOUD_PATH="${CLOUD_PATH:-$CLOUD_OUTPUT_PATH}"

echo "=============================================="
echo "Spark ETL Migration Output Comparison"
echo "=============================================="
echo ""
echo "On-prem output path: $ONPREM_PATH"
echo "Cloud output path: $CLOUD_PATH"
echo ""

# Create temp directory for cloud data
rm -rf "$TEMP_DIR"
mkdir -p "$TEMP_DIR/cloud"
mkdir -p "$TEMP_DIR/onprem"

# Download cloud output
echo "Downloading cloud output from S3..."
aws s3 cp --recursive "$CLOUD_PATH" "$TEMP_DIR/cloud/" --quiet

# Copy on-prem output
echo "Copying on-prem output..."
cp -r "$ONPREM_PATH"/* "$TEMP_DIR/onprem/" 2>/dev/null || true

echo ""
echo "=============================================="
echo "Partition Comparison Results"
echo "=============================================="
echo ""

# Print header
printf "%-40s %-15s %-15s %-10s\n" "Partition" "On-Prem Rows" "Cloud Rows" "Status"
printf "%-40s %-15s %-15s %-10s\n" "----------------------------------------" "---------------" "---------------" "----------"

# Track overall status
OVERALL_STATUS="PASS"
TOTAL_ONPREM=0
TOTAL_CLOUD=0

# Function to count rows in CSV files (excluding header)
count_rows() {
    local path=$1
    local count=0
    
    if [ -d "$path" ]; then
        shopt -s nullglob
        for file in "$path"/*.csv; do
            if [ -f "$file" ]; then
                # Count lines minus header
                local file_count=$(($(wc -l < "$file") - 1))
                count=$((count + file_count))
            fi
        done
        shopt -u nullglob
    fi
    
    echo $count
}

# Compare each partition
for gender in "male" "female"; do
    for interested in "0" "1"; do
        partition="gender=${gender}/interested=${interested}"
        
        onprem_partition="$TEMP_DIR/onprem/gender=${gender}/interested=${interested}"
        cloud_partition="$TEMP_DIR/cloud/features/gender=${gender}/interested=${interested}"
        
        onprem_count=$(count_rows "$onprem_partition")
        cloud_count=$(count_rows "$cloud_partition")
        
        TOTAL_ONPREM=$((TOTAL_ONPREM + onprem_count))
        TOTAL_CLOUD=$((TOTAL_CLOUD + cloud_count))
        
        if [ "$onprem_count" -eq "$cloud_count" ]; then
            status="${GREEN}MATCH${NC}"
        else
            status="${RED}MISMATCH${NC}"
            OVERALL_STATUS="FAIL"
        fi
        
        printf "%-40s %-15s %-15s " "$partition" "$onprem_count" "$cloud_count"
        echo -e "$status"
    done
done

echo ""
printf "%-40s %-15s %-15s\n" "----------------------------------------" "---------------" "---------------"
printf "%-40s %-15s %-15s\n" "TOTAL" "$TOTAL_ONPREM" "$TOTAL_CLOUD"

echo ""
echo "=============================================="
echo "Data Value Comparison"
echo "=============================================="
echo ""

# Compare actual data values (excluding process_date which contains runtime values)
compare_data() {
    local onprem_file=$1
    local cloud_file=$2
    local partition=$3
    
    if [ ! -f "$onprem_file" ] || [ ! -f "$cloud_file" ]; then
        echo "  Skipping $partition - files not found"
        return
    fi
    
    # Extract columns to compare (excluding process_date which contains runtime values)
    # Columns: user_id, birthyear, timestamp, process_date, event_id
    # We compare: user_id, birthyear, timestamp, event_id (columns 1,2,3,5)
    # Note: process_date is column 4 and should be excluded
    
    onprem_sorted=$(tail -n +2 "$onprem_file" | cut -d',' -f1,2,3,5 | sort)
    cloud_sorted=$(tail -n +2 "$cloud_file" | cut -d',' -f1,2,3,5 | sort)
    
    if [ "$onprem_sorted" == "$cloud_sorted" ]; then
        echo -e "  $partition: ${GREEN}Data values match${NC}"
    else
        echo -e "  $partition: ${RED}Data values differ${NC}"
        OVERALL_STATUS="FAIL"
        
        # Show differences
        echo "    Differences (first 5):"
        diff <(echo "$onprem_sorted") <(echo "$cloud_sorted") | head -10 || true
    fi
}

# Compare data for each partition
for gender in "male" "female"; do
    for interested in "0" "1"; do
        partition="gender=${gender}/interested=${interested}"
        
        onprem_partition="$TEMP_DIR/onprem/gender=${gender}/interested=${interested}"
        cloud_partition="$TEMP_DIR/cloud/features/gender=${gender}/interested=${interested}"
        
        # Find CSV files
        onprem_file=$(find "$onprem_partition" -name "*.csv" 2>/dev/null | head -1)
        cloud_file=$(find "$cloud_partition" -name "*.csv" 2>/dev/null | head -1)
        
        if [ -n "$onprem_file" ] && [ -n "$cloud_file" ]; then
            compare_data "$onprem_file" "$cloud_file" "$partition"
        else
            echo -e "  $partition: ${YELLOW}Skipped - files not found${NC}"
        fi
    done
done

echo ""
echo "=============================================="
echo "Final Validation Result"
echo "=============================================="
echo ""

if [ "$OVERALL_STATUS" == "PASS" ]; then
    echo -e "${GREEN}VALIDATION PASSED${NC}"
    echo "On-prem and cloud outputs are identical (excluding runtime-generated fields)"
    exit 0
else
    echo -e "${RED}VALIDATION FAILED${NC}"
    echo "On-prem and cloud outputs have differences"
    exit 1
fi
