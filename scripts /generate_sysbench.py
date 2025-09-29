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

#!/usr/bin/env python3
import subprocess
import sys

def run_command(cmd, capture_output=False):
    """Run a shell command, exit on error."""
    print(f"+ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=capture_output, text=True)
    if result.returncode != 0:
        print("ERROR:", result.stderr or result.stdout, file=sys.stderr)
        sys.exit(result.returncode)
    return result.stdout if capture_output else None

def main():
    # === User-configurable parameters ===
    host        = "192.168.0.101"      # replace with your remote['host']
    user        = "dbbert"
    password    = "dbbert"
    dbname      = "sysbench4"
    tables      = 10
    table_size  = 2000000
    time_budget = 300                     # seconds, adjust as needed
    threads     = 50
    # ====================================

    # 1) Drop & recreate the database
    drop_create_sql = (
        f"DROP DATABASE IF EXISTS `{dbname}`; "
        f"CREATE DATABASE `{dbname}`;"
    )
    run_command([
        "mysql",
        f"--host={host}",
        f"--user={user}",
        f"--password={password}",
        "-e", drop_create_sql
    ])

    # 2) Prepare the schema & data with sysbench
    sb_cmd = [
        "sysbench",                              # the binary
        "--db-driver=mysql",
        f"--mysql-host={host}",
        "--mysql-user=dbbert",
        "--mysql-password=dbbert",
        "--mysql-db=sysbench4",
        "--tables=10",
        "--table-size=2000000",
        # "--report-interval=1",
        # "--threads=50",
        "oltp_read_write",                       # the test script
        "prepare"                                # prepare (populate) the DB
    ]

    run_command(sb_cmd)

    print("\n✅ Database dropped, recreated, and sysbench prepare completed.")

if __name__ == "__main__":
    main()
