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

import sys
import subprocess
import os

def kill_processes_by_term(search_term, force=False):
    """
    Kills all processes that appear in `ps aux` output
    and contain `search_term` in their command line.

    :param search_term: The string to look for (as in grep).
    :param force: If True, uses SIGKILL (kill -9). Otherwise, uses SIGTERM (kill -15).
    :return: A list of PIDs that were terminated.
    """
    try:
        # Run `ps aux` and capture the output
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=True)
        lines = result.stdout.strip().split("\n")
    except subprocess.CalledProcessError as e:
        print(f"Error running ps aux: {e}")
        return []

    # The first line of `ps aux` is headers (USER, PID, %CPU, etc.)
    # We'll skip it and iterate over the rest
    lines = lines[1:]

    terminated_pids = []

    for line in lines:
        # If the line has the search term AND is not just the grep itself,
        # we assume it's a process we want to kill
        if search_term in line and "grep" not in line:
            columns = line.split(None, 10)  # split by whitespace, up to 11 columns total
            # columns[1] should be the PID in the typical ps aux format
            try:
                pid = int(columns[1])
            except (IndexError, ValueError):
                continue  # skip if we can't parse a PID

            # Attempt to kill this process
            try:
                sig = 9 if force else 15  # 9 = SIGKILL, 15 = SIGTERM
                os.kill(pid, sig)
                terminated_pids.append(pid)
            except ProcessLookupError:
                # Process may already have exited
                pass
            except PermissionError:
                # Might need elevated privileges
                print(f"No permission to kill PID {pid}")

    return terminated_pids

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python kill_by_grep.py <search_term> [--force]")
        sys.exit(1)

    # The first argument is the search term
    search_term = sys.argv[1]

    # Optional `--force` argument to use SIGKILL (kill -9)
    force_kill = ("--force" in sys.argv)

    pids = kill_processes_by_term(search_term, force=force_kill)

    if pids:
        print(f"Terminated processes with PIDs: {pids}")
    else:
        print(f"No processes matching '{search_term}' were found.")
