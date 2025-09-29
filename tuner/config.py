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

import os
import sys
import json
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(description="Bayesian Tuner for MySQL")
    parser.add_argument(
        "config_path",
        help="Path to JSON config file"
    )
    parser.add_argument(
        "--continue",
        dest="continue_tuning",
        action="store_true",
        help="Continue from last saved model with additional iterations"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run in debug mode (simulate TPS and skip applying configurations)"
    )
    return parser.parse_args()


def load_config(config_path):
    if not os.path.isfile(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        return json.load(f)