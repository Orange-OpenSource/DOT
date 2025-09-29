"""
/*
 * Software Name : DOT
 * SPDX-FileCopyrightText: Copyright (c) Orange SA
 * SPDX-License-Identifier: MIT
 *
 * This software is distributed under the MIT license,
 * see the "LICENSE" file for more details
 *
 * Authors: see CONTRIBUTORS.md
 * Software description: DOT: Dynamic Knob Selection and Online Sampling for Automated Database Tuning.
 */
"""

import paramiko
import threading
import sys
import time

# List of remote hosts
# hosts = ["192.168.0.63"]
hosts = [
    "192.168.0.62", "192.168.0.63", "192.168.0.215", "192.168.0.9",
    "192.168.0.158", "192.168.0.116", "192.168.0.134", "192.168.0.97",
    "192.168.0.236", "192.168.0.192"
]
# hosts = ["192.168.0.62", "192.168.0.63", "192.168.0.215" ,"192.168.0.9" , "192.168.0.158"]
# hosts = ["192.168.0.198"]
# SSH key and username (update as needed)
ssh_key_path = "/home/cloud/.ssh/key"
username = "cloud"  # Replace with your actual SSH username

# Commands to run on each remote host.
# Adjust MySQL credentials and tpcc_load options as needed.
commands = [
    # 0. Drop the tpcc100 database if it already exists.
    'mysql -u dbbert -pdbbert -e "DROP DATABASE IF EXISTS tpcc100"',
    # 1. Create the tpcc100 database.
    'mysqladmin -u dbbert -pdbbert create tpcc100',
    # 2. Create tables from create_table.sql.
    'cd /home/cloud/tpcc-mysql && mysql -u dbbert -pdbbert tpcc100 < create_table.sql',
    # 3. Create indexes and foreign keys.
    'cd /home/cloud/tpcc-mysql && mysql -u dbbert -pdbbert tpcc100 < add_fkey_idx.sql',
    # 4. Populate data using tpcc_load (example options).
    #    Adjust hostname:port, dbname, user, password, and warehouse count as required.
    'cd /home/cloud/tpcc-mysql && ./tpcc_load -h127.0.0.1 -d tpcc100 -u dbbert -pdbbert -w 100'
]


def execute_and_log(ssh, host, cmd, log_file):
    """Execute a command on the remote host and write output (stdout/stderr) to the log file in real time."""
    log_file.write(f"\n\n===== Executing: {cmd}\n")
    log_file.flush()
    try:
        # Start the command with a pseudo-TTY so that output streams unbuffered.
        stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
    except Exception as e:
        log_file.write(f"Failed to execute command: {e}\n")
        return

    # Loop until the command finishes.
    while not stdout.channel.exit_status_ready():
        # If there's output on stdout, read and log it.
        if stdout.channel.recv_ready():
            out = stdout.channel.recv(1024).decode('utf-8')
            if out:
                log_file.write(out)
                log_file.flush()
                print(f"[{host}] {out}", end="")
        # If there's output on stderr, read and log it.
        if stderr.channel.recv_ready():
            err = stderr.channel.recv(1024).decode('utf-8')
            if err:
                log_file.write(err)
                log_file.flush()
                print(f"[{host}][stderr] {err}", end="")
        # Short sleep to avoid busy waiting.
        time.sleep(0.1)

    # Read any remaining output after the command has finished.
    remaining_out = stdout.read().decode('utf-8')
    if remaining_out:
        log_file.write(remaining_out)
        log_file.flush()
        print(f"[{host}] {remaining_out}", end="")
    remaining_err = stderr.read().decode('utf-8')
    if remaining_err:
        log_file.write(remaining_err)
        log_file.flush()
        print(f"[{host}][stderr] {remaining_err}", end="")

def run_commands_on_host(host, key):
    log_filename = f"/home/cloud/Selectune/scripts/tpcc_load_logs6/tpcc_load_{host.replace('.', '_')}.log"
    try:
        with open(log_filename, "w") as log_file:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                ssh.connect(hostname=host, username=username, pkey=key)
                log_file.write(f"Connected to {host}\n")
                print(f"\nConnected to {host}")
            except Exception as e:
                log_file.write(f"Connection failed: {e}\n")
                print(f"\n[{host}] Connection failed: {e}")
                return

            # Execute each command sequentially.
            for cmd in commands:
                log_file.write("\n" + "=" * 50 + "\n")
                log_file.write(f"Executing command: {cmd}\n")
                log_file.flush()
                print(f"\n[{host}] Executing: {cmd}")
                execute_and_log(ssh, host, cmd, log_file)
                log_file.write("\nCommand finished.\n")
                log_file.flush()

            ssh.close()
            log_file.write("Connection closed.\n")
            print(f"[{host}] Connection closed. Log written to {log_filename}")
    except Exception as e:
        print(f"[{host}] Error writing log: {e}")

def main():
    try:
        key = paramiko.RSAKey.from_private_key_file(ssh_key_path)
    except Exception as e:
        print(f"Error loading SSH key from {ssh_key_path}: {e}")
        sys.exit(1)

    threads = []
    for host in hosts:
        t = threading.Thread(target=run_commands_on_host, args=(host, key))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("\nAll commands executed on all hosts.")

if __name__ == "__main__":
    main()
