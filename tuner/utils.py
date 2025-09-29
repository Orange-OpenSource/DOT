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
import csv
import json
import random
import numpy as np
from typing import List

def load_config(config_path):
    if not os.path.isfile(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        return json.load(f)

def get_knob_dicts(config_data, top_n, is_random=0):
    full_knob_dict = config_data["knob_dict"]
    print(full_knob_dict)
    if is_random != 1:
        # Simply pick the first top_n keys in order
        tuned_keys = list(full_knob_dict.keys())[:top_n]
        tuned_knob_dict = {k: full_knob_dict[k] for k in tuned_keys}
    else:
        # Randomly pick top_n keys
        print('is random, randomly selecting knobs to tune')
        all_keys = list(full_knob_dict.keys())
        tuned_keys = random.sample(all_keys, top_n)
        print("random selected knobs :", tuned_keys)
        tuned_knob_dict = {k: full_knob_dict[k] for k in tuned_keys}

    return full_knob_dict, tuned_knob_dict, tuned_keys


def load_y_data(csv_path: str):
    print("Loading y data from", csv_path)
    y0 = []
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return y0
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        knob_indices = {name: header.index(name) for name in header[3:]}
        for row in reader:
            try:
                tps = float(row[2])
            except Exception:
                continue
            y0.append(tps)
    print("loaded y0", y0)
    return y0


def load_intermediate_data(csv_path: str, tuned_knob_list: list, normalizer):
    x0, y0 = [], []
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return x0, y0
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        knob_indices = {name: header.index(name) for name in header[3:]}
        for row in reader:
            try:
                tps = float(row[2])
            except Exception:
                continue
            config = {}
            for knob in tuned_knob_list:
                if knob in knob_indices:
                    try:
                        config[knob] = float(row[knob_indices[knob]])
                    except Exception:
                        config[knob] = str(row[knob_indices[knob]])  # boolean fallback
                else:
                    config[knob] = 0.0
                    print("Warining: knob not found in loading")
            try:
                norm_values = normalizer.normalize(config)
            except Exception as e:
                print(f"Warning: Normalization failed for row {row} with error: {e}")
                norm_values = [0.0] * len(tuned_knob_list)
            x0.append(norm_values)
            y0.append(-tps)
        print("loaded X0", x0)
    return x0, y0

def build_search_space(tuned_knob_list):
    from skopt.space import Real
    return [Real(0.0, 1.0, name=k) for k in tuned_knob_list]

def get_combined_config(normalizer, full_knob_dict, frozen_values, best_norm):
    tuned_config = normalizer.denormalize(best_norm)
    combined = {}
    for knob, values in full_knob_dict.items():
        if knob in tuned_config:
            combined[knob] = tuned_config[knob]
        elif knob in frozen_values:
            combined[knob] = frozen_values[knob]
        else:
            combined[knob] = values[1][2]
    return combined
