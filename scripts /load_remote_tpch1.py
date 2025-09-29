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
"""
Deploy TPCH on a fleet of MySQL hosts, always replacing any existing TPCH
schema. Detailed logs stream to stdout and to deploy_tpch.log.

Requires:  pip install paramiko
"""

import concurrent.futures
import logging
import pathlib
import sys
from logging.handlers import RotatingFileHandler
from typing import List

import paramiko


# ‑‑‑ editable settings ‑‑‑ ----------------------------------------------------
hosts: List[str] = [
    "192.168.0.198"]
# hosts: List[str] = [
#     "192.168.0.62", "192.168.0.63", "192.168.0.215", "192.168.0.9",
#     "192.168.0.158","192.168.0.116", "192.168.0.134", "192.168.0.103" , "192.168.0.236" , "192.168.0.192"]
SSH_USER        = "cloud"
SSH_KEY_PATH    = "/home/cloud/.ssh/key"
REMOTE_DIR      = "/home/cloud/dbbert/tpch/tpchdata"
MYSQL_USER      = "dbbert"
MYSQL_PASSWORD  = "dbbert"
THREADS         = min(10, len(hosts))

LOG_FILE        = "deploy_tpch.log"          # rotating 5 MB × 3 backups
LOG_LEVEL       = logging.INFO
# -----------------------------------------------------------------------------


# logging setup ---------------------------------------------------------------
logger = logging.getLogger("deploy_tpch")
logger.setLevel(LOG_LEVEL)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    datefmt="%H:%M:%S")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)
# -----------------------------------------------------------------------------


def run_cmd(ssh: paramiko.SSHClient, cmd: str, host: str) -> None:
    """Execute `cmd` on `host`, streaming stdout/stderr line‑by‑line."""
    logger.debug("[%s] RUN: %s", host, cmd)
    stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
    # Stream output while command runs
    for line in iter(stdout.readline, ""):
        if line:
            logger.info("[%s] %s", host, line.rstrip())
    for line in iter(stderr.readline, ""):
        if line:
            logger.warning("[%s] %s", host, line.rstrip())
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        raise RuntimeError(f"[{host}] command exited with status {exit_status}")


def install_tpch(ssh: paramiko.SSHClient, host: str) -> None:
    """Drop and reload TPCH."""
    logger.info("[%s] Starting TPCH (re)installation …", host)
    cmds = [
        f"cd {REMOTE_DIR}",
        'echo "Installing TPC-H on MySQL …"',
        'echo "Copying data …"',
        "sudo cp *.tsv /var/lib/mysql-files",
        'echo "Dropping & recreating database …"',
        (f"mysql -u {MYSQL_USER} -p{MYSQL_PASSWORD} "
         f"-e \"DROP DATABASE IF EXISTS tpch; CREATE DATABASE tpch;\""),
        'echo "Creating schema …"',
        f"mysql -u {MYSQL_USER} -p{MYSQL_PASSWORD} tpch < schema.sql",
        'echo "Loading data …"',
        f"mysql -u {MYSQL_USER} -p{MYSQL_PASSWORD} tpch < loadms.sql",
        'echo "Indexing data …"',
        f"mysql -u {MYSQL_USER} -p{MYSQL_PASSWORD} tpch < index.sql",
        'echo "TPCH installation finished ✔"',
    ]
    run_cmd(ssh, " && ".join(cmds), host)


def process_host(host: str) -> str:
    key = pathlib.Path(SSH_KEY_PATH).expanduser()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        logger.info("[%s] Connecting …", host)
        ssh.connect(hostname=host, username=SSH_USER, key_filename=str(key))
        install_tpch(ssh, host)
        result = f"[{host}] ✔ TPCH installed"
    except Exception as exc:
        logger.exception("[%s] error", host)
        result = f"[{host}] ✖ {exc}"
    finally:
        ssh.close()
    return result


def main() -> None:
    logger.info("Deploying to %d hosts using %d thread(s)", len(hosts), THREADS)
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as pool:
        for msg in pool.map(process_host, hosts):
            logger.info(msg)
    logger.info("All done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Cancelled by user.")
