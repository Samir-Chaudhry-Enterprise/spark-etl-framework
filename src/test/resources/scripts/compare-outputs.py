#!/usr/bin/env python3
"""
Output Comparison Script for On-Prem vs Cloud Migration Validation

This script compares the outputs from on-prem and cloud Spark ETL jobs to ensure
they produce identical results (excluding runtime-generated fields like process_date).

Usage:
    python compare-outputs.py --onprem-path /path/to/onprem/output --cloud-path /path/to/cloud/output

The script will:
1. Compare row counts per partition
2. Compare actual data values (excluding process_date column)
3. Report pass/fail status for each partition
"""

import argparse
import os
import sys
from pathlib import Path


def read_csv_files(directory):
    """Read all CSV files from a directory and return combined data."""
    data = []
    headers = None
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                filepath = os.path.join(root, file)
                partition = os.path.basename(root)
                
                with open(filepath, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        if headers is None:
                            headers = lines[0].strip().split(',')
                        for line in lines[1:]:
                            row = line.strip().split(',')
                            data.append((partition, row))
    
    return headers, data


def get_partitions(directory):
    """Get all partition directories."""
    partitions = {}
    
    for root, dirs, files in os.walk(directory):
        rel_path = os.path.relpath(root, directory)
        if rel_path == '.':
            continue
        
        csv_files = [f for f in files if f.endswith('.csv')]
        if csv_files:
            row_count = 0
            rows = []
            for csv_file in csv_files:
                filepath = os.path.join(root, csv_file)
                with open(filepath, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        for line in lines[1:]:
                            row_count += 1
                            rows.append(line.strip())
            
            partitions[rel_path] = {
                'row_count': row_count,
                'rows': sorted(rows)
            }
    
    return partitions


def filter_process_date(row, headers):
    """Remove process_date column from a row for comparison."""
    if headers and 'process_date' in headers:
        idx = headers.index('process_date')
        parts = row.split(',')
        if idx < len(parts):
            return ','.join(parts[:idx] + parts[idx+1:])
    return row


def compare_outputs(onprem_path, cloud_path):
    """Compare on-prem and cloud outputs."""
    print("=" * 80)
    print("OUTPUT COMPARISON: On-Prem vs Cloud")
    print("=" * 80)
    print(f"\nOn-Prem Path: {onprem_path}")
    print(f"Cloud Path: {cloud_path}")
    print()
    
    onprem_partitions = get_partitions(onprem_path)
    cloud_partitions = get_partitions(cloud_path)
    
    all_partitions = set(onprem_partitions.keys()) | set(cloud_partitions.keys())
    
    if not all_partitions:
        print("WARNING: No partitions found in either output directory.")
        return False
    
    headers = None
    for root, dirs, files in os.walk(onprem_path):
        for f in files:
            if f.endswith('.csv'):
                with open(os.path.join(root, f), 'r') as fp:
                    headers = fp.readline().strip().split(',')
                    break
        if headers:
            break
    
    print("-" * 80)
    print(f"{'Partition':<40} {'On-Prem':<12} {'Cloud':<12} {'Status':<10}")
    print("-" * 80)
    
    all_passed = True
    
    for partition in sorted(all_partitions):
        onprem_data = onprem_partitions.get(partition, {'row_count': 0, 'rows': []})
        cloud_data = cloud_partitions.get(partition, {'row_count': 0, 'rows': []})
        
        onprem_count = onprem_data['row_count']
        cloud_count = cloud_data['row_count']
        
        if onprem_count != cloud_count:
            status = "FAIL (count)"
            all_passed = False
        else:
            onprem_rows = [filter_process_date(r, headers) for r in onprem_data['rows']]
            cloud_rows = [filter_process_date(r, headers) for r in cloud_data['rows']]
            
            if sorted(onprem_rows) == sorted(cloud_rows):
                status = "PASS"
            else:
                status = "FAIL (data)"
                all_passed = False
        
        print(f"{partition:<40} {onprem_count:<12} {cloud_count:<12} {status:<10}")
    
    print("-" * 80)
    
    total_onprem = sum(p['row_count'] for p in onprem_partitions.values())
    total_cloud = sum(p['row_count'] for p in cloud_partitions.values())
    
    print(f"{'TOTAL':<40} {total_onprem:<12} {total_cloud:<12}")
    print()
    
    if all_passed:
        print("VALIDATION RESULT: PASSED - All partitions match")
    else:
        print("VALIDATION RESULT: FAILED - Some partitions do not match")
    
    print("=" * 80)
    
    return all_passed


def main():
    parser = argparse.ArgumentParser(description='Compare on-prem and cloud ETL outputs')
    parser.add_argument('--onprem-path', required=True, help='Path to on-prem output directory')
    parser.add_argument('--cloud-path', required=True, help='Path to cloud output directory')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.onprem_path):
        print(f"ERROR: On-prem path does not exist: {args.onprem_path}")
        sys.exit(1)
    
    if not os.path.exists(args.cloud_path):
        print(f"ERROR: Cloud path does not exist: {args.cloud_path}")
        sys.exit(1)
    
    success = compare_outputs(args.onprem_path, args.cloud_path)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
