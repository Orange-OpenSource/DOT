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

import threading
import queue
import subprocess
import json
import csv
import os
import re
import time
from datetime import datetime
import mysql.connector
import random

class MySQLDriver:
    # Class-level constants (regexes) if desired
    TPCC_PATTERN = re.compile(r"trx:\s*(\d+)")
    SYSBENCH_PATTERN = re.compile(r"tps:\s+([\d.]+)")
    SYSBENCH_LATENCY_PATTERN = re.compile(r"lat \(ms,95%\):\s+([\d.]+)")
    def __init__(self,
                 remote: dict,
                 ssh_key_path: str = "~/.ssh/key",
                 local_log_dir: str = "../LOGS",
                 local_res_log_dir: str = "../RES_LOGS",
                 remote_mycnf_path: str = "/etc/mysql/my.cnf",
                 is_fixed_ram: int= 0,
                 is_limited_cpu: int = 0,
                 objective_metric: str = "trx", 
                 budget_allocator: int = 0,
                 tpcc_mysql_path: str = "/home/cloud/tpcc-mysql"
                 ):
        """
        Initialize the MySQLDriver with SSH and DB connection details.
        
        :param remote: dict with keys:
                       'host' (str),
                       'remote_user' (str),
                       'db_user' (str),
                       'password' (str),
                       'database' (str),
                       'port' (int).
        :param ssh_key_path: Path to the SSH private key.
        :param local_log_dir: Local directory path for logs.
        :param remote_mycnf_path: Remote path where the .cnf override will be placed.
        """
        self.remote = remote
        self.ssh_key_path = ssh_key_path
        self.local_log_dir = local_log_dir
        self.remote_mycnf_path = remote_mycnf_path
        self.is_fixed_ram = is_fixed_ram
        self.is_limited_cpu = is_limited_cpu
        self.objective_metric = objective_metric
        self.local_res_log_dir = local_res_log_dir
        self.budget_allocator = budget_allocator
        self.best_performance = 0
        self.tpcc_mysql_path = tpcc_mysql_path
        # Make sure the local log directory exists
        os.makedirs(self.local_log_dir, exist_ok=True)

    def now_str(self) -> str:
        """
        Return current date-time string in 'YYYY-MM-DD HH:MM:SS' format.
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _ssh_command(self, cmd: str):
        """
        Runs a shell command on the remote machine via SSH.
        Returns (stdout, stderr, returncode).
        """
        ssh_cmd = [
            "ssh",
            "-i", self.ssh_key_path,
            f"{self.remote['remote_user']}@{self.remote['host']}",
            cmd
        ]
        proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out, err = proc.communicate()
        return out, err, proc.returncode

    def apply_config_and_restart(self, config_dict: dict) -> bool:
        """
        Apply a JSON-formatted config dict to the remote MySQL instance and restart MySQL,
        optionally setting a memory limit (via systemd slice) if self.is_fixed_ram != 0,
        and optionally setting CPUQuota if self.is_limited_cpu != 0.
        """
        print(f"{self.now_str()} [{self.remote['host']}] Applying config")

        # 1) Build remote config content for MySQL
        #    (this is your custom MySQL config, e.g. innodb params)
        if self.is_fixed_ram != 0:
            # set to 80% of the system
            remote_config_content = f"[mysqld]\nskip-log-bin\ninnodb_buffer_pool_size={int(self.is_fixed_ram * 0.8)}M\nnmax_execution_time = 180000\n"
        else:
            remote_config_content = "[mysqld]\nskip-log-bin\nmax_execution_time = 180000\n"

        for knob, val in config_dict.items():
            remote_config_content += f"{knob} = {val}\n"

        temp_cfg_path = "/tmp/my_override.cnf"
        safe_content = remote_config_content.replace("'", "'\\''")

        # 2) Write that config to remote /tmp, then move into place
        echo_cmd = f"echo '{safe_content}' > {temp_cfg_path}"
        _, err, code = self._ssh_command(echo_cmd)
        if code != 0:
            print(f"{self.now_str()} [{self.remote['host']}] Failed to write temp config: {err}")
            return False

        move_cmd = f"sudo mv {temp_cfg_path} {self.remote_mycnf_path}"
        _, err, code = self._ssh_command(move_cmd)
        if code != 0:
            print(f"{self.now_str()} [{self.remote['host']}] Failed to move config to {self.remote_mycnf_path}: {err}")
            return False

        # ----------------------------------------------------------------------
        # MEMORY LIMIT STEPS
        # ----------------------------------------------------------------------
        if self.is_fixed_ram != 0:
            # 1) Create a systemd slice, e.g. /etc/systemd/system/mysql-limit.slice
            #    We'll set MemoryMax to e.g. "2048M" if self.is_fixed_ram=2048
            memory_slice_content = (
                "[Unit]\n"
                "Description=Slice with limited memory for MySQL\n\n"
                "[Slice]\n"
                f"MemoryMax={self.is_fixed_ram}M\n"
            )
            temp_slice_path = "/tmp/mysql-limit.slice"
            safe_slice = memory_slice_content.replace("'", "'\\''")
            echo_slice_cmd = f"echo '{safe_slice}' > {temp_slice_path}"
            _, err, code = self._ssh_command(echo_slice_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to write temp slice file: {err}")
                return False

            move_slice_cmd = "sudo mv /tmp/mysql-limit.slice /etc/systemd/system/mysql-limit.slice"
            _, err, code = self._ssh_command(move_slice_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to move slice file: {err}")
                return False

            # 2) Modify MySQL service to run under "mysql-limit.slice"
            #    We'll do this via a drop-in override file, similar to how you handle CPU.
            #    For example: /etc/systemd/system/mysql.service.d/override_memory.conf
            mem_override_content = "[Service]\nSlice=mysql-limit.slice\n"
            mem_override_temp = "/tmp/override_memory.conf"
            safe_mem_override = mem_override_content.replace("'", "'\\''")
            echo_mem_override_cmd = f"echo '{safe_mem_override}' > {mem_override_temp}"
            _, err, code = self._ssh_command(echo_mem_override_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to write temp memory override: {err}")
                return False

            mkdir_cmd = "sudo mkdir -p /etc/systemd/system/mysql.service.d/"
            _, err, code = self._ssh_command(mkdir_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to create systemd override dir: {err}")
                return False

            move_mem_override_cmd = (
                "sudo mv /tmp/override_memory.conf "
                "/etc/systemd/system/mysql.service.d/override_memory.conf"
            )
            _, err, code = self._ssh_command(move_mem_override_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to install memory override: {err}")
                return False

        else:
            # If we do NOT want a memory limit, remove the slice & override if present
            check_remove_slice = "sudo rm -f /etc/systemd/system/mysql-limit.slice"
            self._ssh_command(check_remove_slice)
            check_remove_mem_override = (
                "sudo rm -f /etc/systemd/system/mysql.service.d/override_memory.conf"
            )
            self._ssh_command(check_remove_mem_override)

        # ----------------------------------------------------------------------
        # CPU LIMIT STEPS (your existing code for CPUAffinity or CPUQuota)
        # ----------------------------------------------------------------------
        if self.is_limited_cpu != 0:
            print("Setting CPUQuota")
            override_content = f"[Service]\nCPUQuota={self.is_limited_cpu}%\n"
            temp_override_path = "/tmp/mysql_override.conf"
            safe_override = override_content.replace("'", "'\\''")
            echo_override_cmd = f"echo '{safe_override}' > {temp_override_path}"
            _, err, code = self._ssh_command(echo_override_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to write temp override config: {err}")
                return False

            mkdir_cmd = "sudo mkdir -p /etc/systemd/system/mysql.service.d/"
            _, err, code = self._ssh_command(mkdir_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to create override directory: {err}")
                return False

            move_override_cmd = (
                f"sudo mv {temp_override_path} /etc/systemd/system/mysql.service.d/override.conf"
            )
            _, err, code = self._ssh_command(move_override_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to move override config: {err}")
                return False

            _, err, code = self._ssh_command("sudo systemctl daemon-reload")
            print("reload systemd daemon for CPUQuota")
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to reload systemd daemon: {err}")
                return False
        else:
            # Remove CPU override if no CPU limit wanted
            check_remove_cmd = (
                "if [ -f /etc/systemd/system/mysql.service.d/override.conf ]; then "
                "sudo rm -f /etc/systemd/system/mysql.service.d/override.conf; fi"
            )
            _, err, code = self._ssh_command(check_remove_cmd)
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to remove override config: {err}")
                return False

            _, err, code = self._ssh_command("sudo systemctl daemon-reload")
            if code != 0:
                print(f"{self.now_str()} [{self.remote['host']}] Failed to reload systemd daemon: {err}")
                return False

        # ----------------------------------------------------------------------
        # 3) Reload systemd & restart MySQL
        # ----------------------------------------------------------------------
        # If we created or removed the slice, let's ensure systemd sees changes
        _, err, code = self._ssh_command("sudo systemctl daemon-reload")
        if code != 0:
            print(f"{self.now_str()} [{self.remote['host']}] Failed to reload systemd daemon: {err}")
            return False

        _, err, code = self._ssh_command("sudo systemctl restart mysql")
        if code != 0:
            print(f"{self.now_str()} [{self.remote['host']}] MySQL restart failed => {err}")
            return False

        print(f"{self.now_str()} [{self.remote['host']}] MySQL restart success")
        return True

    def _execute_single_query(self, cursor, query: str) -> float:
        """
        Execute a single SQL statement and return its execution time in seconds.
        """
        start = time.time()
        cursor.execute(query)
        while cursor.nextset():
            pass
        return round(time.time() - start, 4)

    def execute_olap(self, sql_file_path: str, intermediate_csv=None) -> float:
        """
        Run TPC-H (OLAP) queries with optional time-budget sampling.
        Treat any CREATE VIEW ... DROP VIEW sequence as one unit when sampling.
        Estimate vs. baseline, log true times, and update baseline only on better full runs.
        """
        # Load and group statements
        with open(sql_file_path, 'r') as f:
            parts = [p.strip() for p in f.read().split(';') if p.strip()]
        queries = []
        buffer = []
        in_view = False
        for part in parts:
            low = part.lower()
            if low.startswith('create view'):
                in_view = True; buffer = [part]
            elif in_view:
                buffer.append(part)
                if low.startswith('drop view'):
                    queries.append('; '.join(buffer) + ';')
                    buffer = []; in_view = False
            else:
                queries.append(part + ';')
        if not queries:
            return 0.0

        # Initialize baselines
        if not hasattr(self, 'best_total_time'):
            self.best_total_time = None; self.best_olap_times = {}
        pct = max(0, min(100, getattr(self, 'budget_allocator', 100)))

        # DB connection
        db_cfg = {
            'host': self.remote['host'],
            'user': self.remote['db_user'],
            'password': self.remote['password'],
            'database': self.remote['database'],
            'port': self.remote.get('port', 3306)
        }
        conn = mysql.connector.connect(**db_cfg)
        cursor = conn.cursor()

        sample_total = 0.0; sample_csv = None
        # Sampling
        if 0 < pct < 100:
            count = max(1, int(len(queries) * pct / 100))
            sampled = random.sample(queries, count)
            sample_times = {q: self._execute_single_query(cursor, q) for q in sampled}
            sample_total = round(sum(sample_times.values()), 2)
            # log sample
            if intermediate_csv:
                base, ext = os.path.splitext(intermediate_csv)
                sample_csv = f"{base}_true_execution_time{ext}"
                write_header = not os.path.exists(sample_csv)
                with open(sample_csv, 'a', newline='') as f:
                    w = csv.writer(f)
                    if write_header:
                        w.writerow(['type','time'])
                    w.writerow(['sample', sample_total])
                print(f"Logged sample {sample_total}s -> {sample_csv}")
            # estimate full
            total_best = sum(self.best_olap_times.get(q,0) for q in queries)
            sampled_best = sum(self.best_olap_times.get(q,0) for q in sampled)
            ratio = sample_total / sampled_best if sampled_best else 1
            est = round(total_best * ratio, 2)
            if self.best_total_time is not None and est >= self.best_total_time:
                cursor.close(); conn.close(); return est

        # Full execution
        start = time.time(); full_times = {}
        for q in queries:
            full_times[q] = self._execute_single_query(cursor, q)
        full_total = round(time.time() - start, 2)
        print(f"Full run time: {full_total}s")

        # Update baseline and log combined if improved
        if self.best_total_time is None or full_total < self.best_total_time:
            self.best_total_time = full_total; self.best_olap_times = full_times.copy()
            print(f"New baseline: {full_total}s")
            if sample_csv:
                combined = round(sample_total + full_total, 2)
                with open(sample_csv, 'a', newline='') as f:
                    w = csv.writer(f); w.writerow(['full', combined])
                print(f"Logged full {combined}s -> {sample_csv}")
        else:
            print(f"Baseline {self.best_total_time}s remains best")

        cursor.close(); conn.close()
        return full_total


    # def _execute_tpch_queries(self, sql_file_path: str, conn, cursor):
    #     """
    #     Execute the SQL script from sql_file_path on an open connection,
    #     splitting on ';' if you have multiple statements.
    #     Commits after all statements are executed.
    #     """
    #     time_budget = self.budget_allocator if getattr(self, "budget_allocator", 0) else 100
    #     with open(sql_file_path, 'r') as file:
    #         sql_script = file.read()

    #     for statement in sql_script.split(';'):
    #         statement = statement.strip()
    #         if statement:
    #             cursor.execute(statement)
    #             # Drain remaining results if multi-statement
    #             while cursor.nextset():
    #                 pass

    # def execute_olap(self, sql_file_path: str, intermediate_csv=None) -> float:
    #     """
    #     Connect to remote MySQL from local, run TPC-H queries (OLAP).
    #     :param sql_file_path: Path to the SQL file containing TPC-H queries.
    #     :return: True if successful, False otherwise.
    #     """
    #     total_time = 0
    #     try:
    #         db_config = {
    #             'host': self.remote['host'],
    #             'user': self.remote['db_user'],
    #             'password': self.remote['password'],
    #             'database': self.remote['database'],
    #             'port': self.remote.get('port', 3306)
    #         }
    #         conn = mysql.connector.connect(**db_config)
    #         cursor = conn.cursor()
    #         start_time = time.time()
    #         # Execute the TPC-H queries
    #         self._execute_tpch_queries(sql_file_path, conn, cursor)
    #         total_time = round(time.time() - start_time, 2)
    #         cursor.close()
    #         conn.close()

    #     except mysql.connector.Error as e:
    #         print(f"{self.now_str()} [{self.remote['host']}] TPC-H queries error => {e}")
    #         return 95270

    #     # total_time = round(time.time() - start_time, 2)
    #     print(f"{self.now_str()} [{self.remote['host']}] TPC-H total time: {total_time} seconds")
    #     return total_time
    def _parse_sysbench_log_for_latency(self, log_text: str) -> float:
        """
        Parse sysbench log text to compute average latency (95th percentile in ms)
        from the last 30 lines.
        Returns None if <30 data points are found.
        Returns 95270 if all data points are zero.
        """
        matches = self.SYSBENCH_LATENCY_PATTERN.findall(log_text)
        if len(matches) < 30:
            print(f"{self.now_str()}: Not enough latency data points (<30).")
            return None

        last_30 = list(map(float, matches[-30:]))

        # Remove all zeros
        non_zero_values = [val for val in last_30 if val != 0.0]

        # If all values were zero
        if not non_zero_values:
            return 95270

        avg_latency = sum(non_zero_values) / len(non_zero_values)
        return round(avg_latency, 2)


    def _parse_sysbench_log_for_tps(self, log_text: str) :
        matches = self.SYSBENCH_PATTERN.findall(log_text)
        if not matches:
            print(f"{self.now_str()}: No TPS data found in the log.")
            return None

        tps_values = list(map(float, matches))
        values_to_average = tps_values[-30:] if len(tps_values) >= 30 else tps_values
        avg_tps = sum(values_to_average) / len(values_to_average)
        return round(avg_tps, 2)

    def _parse_tpcc_log_for_trx(self, log_text: str) :
        """
        Parse TPC-C log text to average the "trx:" value from the last 30 occurrences.
        Returns None if no data found.
        """        
        matches = self.TPCC_PATTERN.findall(log_text)
        if not matches:
            print(f"{self.now_str()}: No 'trx:' data found in the log.")
            return None

        trx_values = list(map(int, matches))
        values_to_average = trx_values[-30:] if len(trx_values) >= 30 else trx_values
        avg_trx = sum(values_to_average) / len(values_to_average)
        return round(avg_trx, 2)

   
    def _parse_resource_log_for_averages(self,csv_file_path: str, num_samples: int = 30):
        """
        Parse the resource CSV log, read the last `num_samples` rows,
        and compute average CPU, RAM, and I/O usage.
        
        :param csv_file_path: Path to the CSV file (with header) in the format:
            timestamp,mysqld_cpu_percent,mysqld_ram_percent,disk_util_percent
        :param num_samples: number of trailing samples to average
        :return: (mean_cpu, mean_ram, mean_io) as floats
        """
        # if not os.path.exists(csv_file_path):
        #     print(f"Resource log file not found: {csv_file_path}")
        #     return (0.0, 0.0, 0.0)

        # with open(csv_file_path, "r") as f:
        #     lines = f.read().splitlines()

        # # If there's no data (or only header), return 0s
        # if len(lines) <= 1:
        #     return (0.0, 0.0, 0.0)

        # # lines[0] is the header: "timestamp,mysqld_cpu_percent,mysqld_ram_percent,disk_util_percent"
        # data_lines = lines[1:]  # skip header

        # # Get the last `num_samples` lines
        # if len(data_lines) > num_samples:
        #     data_lines = data_lines[-num_samples:]

        # total_cpu = 0.0
        # total_ram = 0.0
        # total_io  = 0.0
        # count = 0

        # for line in data_lines:
        #     # Each line should look like:
        #     # 2025-04-09 10:32:01,12.34,5.67,3.21
        #     parts = line.split(",")
        #     if len(parts) < 4:
        #         continue
        #     try:
        #         # parts[0] = timestamp
        #         cpu_val = float(parts[1])
        #         ram_val = float(parts[2])
        #         io_val  = float(parts[3])

        #         total_cpu += cpu_val
        #         total_ram += ram_val
        #         total_io  += io_val
        #         count += 1
        #     except ValueError:
        #         pass  # skip malformed lines

        # if count == 0:
        #     return (0.0, 0.0, 0.0)

        # mean_cpu = total_cpu / count
        # mean_ram = total_ram / count
        # mean_io  = total_io  / count
        # return (mean_cpu, mean_ram, mean_io)
        return (0, 0, 0)# deactivate for now



    def execute_oltp(self, benchmark: str = "sysbench") -> tuple[float, float, float, float]:
        """Run one (or possibly two) OLTP benchmarks and return
        (perf_metric, mean_cpu, mean_ram, mean_io)."""

        def _run_once(time_budget: int) -> tuple[float, float, float, float]:
            oltp_cmd = self._build_oltp_command(benchmark, time_budget)
            if oltp_cmd is None:
                return -9527, 0.0, 0.0, 0.0                      # sentinel on bad benchmark name

            log_path, res_path = self._prepare_log_paths(benchmark)
            print(f"{self.now_str()}: [{self.remote['host']}] Running {benchmark} ({time_budget}s) -> {log_path}")
            print(f"benchmark allocator on, benchmark executed for {time_budget} seconds")

            proc, lf, start_time = self._launch_benchmark(oltp_cmd, log_path)
            # self._monitor_remote_metrics(proc, benchmark, res_path, start_time) # deactivate for now

            return self._parse_final_metrics(benchmark, log_path, res_path, proc, lf)

        # ---------- first run (respect budget_allocator or default 90) ----------
        time_budget = self.budget_allocator if getattr(self, "budget_allocator", 0) else 90
        perf, mean_cpu, mean_ram, mean_io = _run_once(time_budget)

        # ---------- update / optional confirmation run ----------
        if perf > getattr(self, "best_performance", float("-inf")):
            # If the first run used a truncated time budget, do a confirming 90-second run
            if self.budget_allocator and time_budget < 90:
                print(f"{self.now_str()}: New best performance {perf:.2f}. Re-running for 90 s to confirm.")
                perf2, mean_cpu2, mean_ram2, mean_io2 = _run_once(90)

                if perf2 > self.best_performance:
                    self.best_performance = perf2
                    # perf, mean_cpu, mean_ram, mean_io = perf2, mean_cpu2, mean_ram2, mean_io2
                return perf2, mean_cpu2, mean_ram2, mean_io2
        return perf, mean_cpu, mean_ram, mean_io



    def _build_oltp_command(self, benchmark: str, time_budget: int):
        if benchmark == "sysbench":
            return [
                "sudo",
                "nice",
                "-n",
                "-10",
                "sysbench",
                "--db-driver=mysql",
                f"--mysql-host={self.remote['host']}",
                "--mysql-user=dbbert",
                "--mysql-password=dbbert",
                "--mysql-db=sysbench4",
                "--tables=10",
                "--table-size=2000000",
                f"--time={time_budget}",
                "--report-interval=1",
                "--threads=50",
                "oltp_read_write",
                "run",
            ]
        if benchmark == "tpcc":
            return (
                f"sudo nice -n -10 "
                f"{self.tpcc_mysql_path}/tpcc_start "
                f"-h{self.remote['host']} -P3306 -dtpcc100 -udbbert -pdbbert "
                f"-w100 -c32 -r10 -l{time_budget} -i1"
            )
        print("benchmark not implemented")
        return None


    def _prepare_log_paths(self, benchmark: str):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(self.local_log_dir, f"{benchmark}_{self.remote['host']}_{ts}.log")
        res_path = os.path.join(self.local_res_log_dir, f"{benchmark}_{self.remote['host']}_{ts}.csv")
        return log_path, res_path


    def _launch_benchmark(self, oltp_cmd, log_path):
        lf = open(log_path, "w")
        if isinstance(oltp_cmd, list):
            proc = subprocess.Popen(oltp_cmd, stdout=lf, stderr=lf, text=True)
        else:
            proc = subprocess.Popen(oltp_cmd, shell=True, stdout=lf, stderr=lf, text=True)
        return proc, lf, time.time()


    def _get_mysqld_usage(self):
        top_out, _, rc = self._ssh_command("top -b -d 1 -n 2 | grep mysqld | tail -n 1")
        cpu = mem = 0.0
        if rc == 0 and top_out:
            for ln in top_out.splitlines():
                if "mysqld" in ln and "COMMAND" not in ln:
                    p = ln.split()
                    if len(p) >= 10:
                        try:
                            cpu, mem = float(p[8]), float(p[9])
                        except ValueError:
                            pass
                    break
        return cpu, mem


    def _get_disk_util(self):
        io_out, _, rc = self._ssh_command("iostat -x -d vda 1 1")
        util = 0.0
        if rc == 0 and io_out:
            for ln in io_out.splitlines():
                if ln.startswith("vda"):
                    try:
                        util = float(ln.split()[-1])
                    except ValueError:
                        pass
                    break
        return util


    def _monitor_remote_metrics(self, proc, benchmark, res_path, start_time):
        # with open(res_path, "w") as csvf:
        #     csvf.write("timestamp,mysqld_cpu_percent,mysqld_ram_percent,disk_util_percent\n")
        #     while proc.poll() is None:
        #         # time limit exceeded → sudo-kill
        #         if benchmark == "tpcc" and time.time() - start_time > 110:
        #             print("TPC-C exceeded 110 seconds, killing process via sudo.")
        #             try:
        #                 subprocess.run(
        #                     ["sudo", "kill", "-9", str(proc.pid)],
        #                     check=True
        #                 )
        #             except subprocess.CalledProcessError as e:
        #                 print(f"Failed to kill PID {proc.pid}: {e}")
        #             break

        #         # otherwise gather metrics
        #         now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #         cpu, mem = self._get_mysqld_usage()
        #         util = self._get_disk_util()
        #         csvf.write(f"{now},{cpu:.2f},{mem:.2f},{util:.2f}\n")
        #         csvf.flush()
        #         time.sleep(1)
        pass # deactivate for now


    def _parse_final_metrics(self, benchmark, log_path, res_path, proc, lf):
        rc = proc.wait()
        lf.close()
        if rc == -9:
            print("tpcc weird behavior, killed")
        if rc != 0:
            print(f"{self.now_str()}: [{self.remote['host']}] {benchmark} failed with code {rc}.")
        with open(log_path) as f:
            log_txt = f.read()
        mean_cpu, mean_ram, mean_io = self._parse_resource_log_for_averages(res_path, 30)
        if benchmark == "sysbench" and self.objective_metric == "trx":
            v = self._parse_sysbench_log_for_tps(log_txt)
            return (v if v is not None else 0, mean_cpu, mean_ram, mean_io)
        if benchmark == "sysbench" and self.objective_metric == "lat":
            v = self._parse_sysbench_log_for_latency(log_txt)
            return (-(v) if v is not None else -999999, mean_cpu, mean_ram, mean_io)
        if benchmark == "tpcc" and self.objective_metric == "trx":
            v = self._parse_tpcc_log_for_trx(log_txt)
            return (v if v is not None else 0, mean_cpu, mean_ram, mean_io)
        if benchmark == "tpcc" and self.objective_metric == "lat":
            print("not yet implemented")
            return (-9527, mean_cpu, mean_ram, mean_io)
        print("something is wrong at execute_oltp in mysqldriver")
        return (-9527, mean_cpu, mean_ram, mean_io)

        


if __name__ == "__main__":
    # Example 'remote' dictionary
    remote_info = {
        'host': "192.168.0.192",
        'remote_user': "cloud",
        'db_user': "dbbert",
        'password': "dbbert",
        'database': "tpch",
        'port': 3306
    }

    # Create an instance of MySQLDriver
    driver = MySQLDriver(
        remote=remote_info,
        ssh_key_path="~/.ssh/key",
        local_log_dir="../LOGS",
        remote_mycnf_path="/etc/mysql/my.cnf"
    )

    # Apply a sample config & restart MySQL
    mem = 15*1024*1024*1024
    config_dcit = {"innodb_buffer_pool_size": mem}
    success = driver.apply_config_and_restart(config_dcit)

    # Run OLAP (TPC-H) if config applied
    if success:
        driver.execute_olap("/home/cloud/GRID_search_sequential_tuning/configs/queries_tpch.sql")

    # Run an OLTP benchmark (e.g. sysbench)
    sysbench_tps = driver.execute_oltp("sysbench")
    print("Sysbench TPS =>", sysbench_tps)

    # Or TPC-C
    tpcc_trx = driver.execute_oltp("tpcc")
    print("TPC-C TRX =>", tpcc_trx)
