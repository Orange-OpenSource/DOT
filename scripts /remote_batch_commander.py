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
import sys
import threading
import time

# List of remote hosts
hosts = [
    "192.168.0.62", "192.168.0.63", "192.168.0.215", "192.168.0.9",
    "192.168.0.158", "192.168.0.116", "192.168.0.134", "192.168.0.97",
    "192.168.0.236", "192.168.0.192"
]
# hosts = ["192.168.0.198"]
# Path to your SSH private key and SSH username.
ssh_key_path = "/home/cloud/.ssh/key"
username = "cloud"  # Replace with your actual SSH username

def read_from_channel(host, channel):
    """Continuously read output from the remote interactive shell."""
    while True:
        try:
            if channel.recv_ready():
                data = channel.recv(1024).decode('utf-8')
                if data:
                    print(f"\n--- Output from {host} ---\n{data}")
            else:
                time.sleep(0.1)
        except Exception as e:
            print(f"Error reading from {host}: {e}")
            break

def main():
    try:
        key = paramiko.RSAKey.from_private_key_file(ssh_key_path)
    except Exception as e:
        print(f"Error loading SSH key from {ssh_key_path}: {e}")
        sys.exit(1)

    connections = {}  # Maps host -> SSHClient
    channels = {}     # Maps host -> interactive shell channel

    # Establish connections and open an interactive shell on each host.
    for host in hosts:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(hostname=host, username=username, pkey=key)
            print(f"Connected to {host}")
            channel = ssh.invoke_shell()  # Opens an interactive shell session.
            channels[host] = channel
            connections[host] = ssh

            # Start a background thread to read output from this channel.
            thread = threading.Thread(target=read_from_channel, args=(host, channel), daemon=True)
            thread.start()
        except Exception as e:
            print(f"Failed to connect to {host}: {e}")

    if not connections:
        print("No connections were established. Exiting.")
        sys.exit(1)

    print("\nAll available connections established.")
    print("Enter commands to execute on all remote hosts (stateful session).")
    print("For example, you can 'cd tpcc-mysql' and then run other commands in that directory.")
    print("Type 'exit' to quit.\n")

    # Interactive loop: send commands to all interactive shells.
    try:
        while True:
            command = input("Command> ").strip()
            if command.lower() == "exit":
                break
            if not command:
                continue

            # Send the command (plus newline) to each remote shell.
            for host, channel in channels.items():
                channel.send(command + "\n")
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Exiting.")
    finally:
        # Close all SSH connections.
        for host, ssh in connections.items():
            ssh.close()
        print("All connections closed.")

if __name__ == "__main__":
    main()
