#!/bin/bash
# Script: deploy_run_parallel.sh
# Purpose: Deploy the TPC-H database generation script to a list of remote hosts and execute it in parallel.
# Usage: ./deploy_run_parallel.sh
#
# This script:
#   - Copies the local generate_tpch_sf4.sh to each remote machine at /home/cloud/Selectune/scripts/
#   - Executes the script remotely over SSH using the provided SSH key
#   - Runs the tasks in parallel (backgrounded) and waits until all are finished.

# List of remote host IP addresses
hosts=(
    # "192.168.0.62"
    "192.168.0.112"
    # "192.168.0.215"
    # "192.168.0.9"
    # "192.168.0.158"
    # "192.168.0.116"
    # "192.168.0.134"
    # "192.168.0.103"
    # "192.168.0.236"
    # "192.168.0.192"
)

# SSH key for authentication
SSH_KEY="/home/cloud/.ssh/key"

# Local script to deploy and execute (this is your TPC-H generation script)
LOCAL_SCRIPT="/home/cloud/Selectune/scripts/generate_tpch.sh"

# Remote destination path for the script
REMOTE_DEST="/home/cloud/generate_tpch_sf4.sh"

# Ensure the local script exists
./if [ ! -f "$LOCAL_SCRIPT" ]; then
    echo "Error: Local script $LOCAL_SCRIPT not found!"
    exit 1
fi

echo "Deploying and executing the TPC-H generation script in parallel on remote hosts..."

# Loop over all hosts, launching background jobs for each.
for host in "${hosts[@]}"; do
  {
    echo "-----------------------------------------"
    echo "Processing host: $host"

    # Optionally create the destination directory on the remote host:
    ssh -i "$SSH_KEY" "cloud@$host" "mkdir -p /home/cloud/Selectune/scripts/"

    # Copy the local script to the remote host
    echo "Copying script to $host..."
    scp -i "$SSH_KEY" "$LOCAL_SCRIPT" "cloud@$host:$REMOTE_DEST"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to copy the script to $host"
        exit 1
    fi
    echo "Script copied successfully to $host."

    # Execute the script remotely
    echo "Executing script on $host..."
    ssh -i "$SSH_KEY" "cloud@$host" "bash $REMOTE_DEST"
    if [ $? -ne 0 ]; then
        echo "Error: Script execution failed on $host"
    else
        echo "Script executed successfully on $host"
    fi
  } &
done

# Wait for all background jobs to finish before exiting.
wait

echo "Deployment and parallel execution completed on all hosts."
