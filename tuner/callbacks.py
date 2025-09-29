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
import csv
from datetime import datetime

class LoggingCallback:
    def __init__(self, normalizer, models_dir, cfg_name, full_knob_dict,
                 intermediate_csv_file, frozen_values):
        self.normalizer = normalizer
        self.models_dir = models_dir
        self.cfg_name = cfg_name
        self.full_knob_dict = full_knob_dict
        self.intermediate_csv_file = intermediate_csv_file
        self.frozen_values = frozen_values

        if not os.path.exists(self.intermediate_csv_file) or os.path.getsize(self.intermediate_csv_file) == 0:
            with open(self.intermediate_csv_file, "w", newline="") as f:
                writer = csv.writer(f)
                header = ["iteration", "timestamp", "TPS"] + list(self.full_knob_dict.keys())
                writer.writerow(header)

    def __call__(self, res):
        iteration_num = len(res.x_iters)
        current_tps = -res.func_vals[-1]
        current_norm = res.x_iters[-1]
        current_cfg = self.normalizer.denormalize(current_norm)
        now_str = datetime.now().isoformat()

        combined = {}
        for knob, vals in self.full_knob_dict.items():
            if knob in current_cfg:
                combined[knob] = current_cfg[knob]
            elif knob in self.frozen_values:
                combined[knob] = self.frozen_values[knob]
            else:
                combined[knob] = vals[1][2]

        with open(self.intermediate_csv_file, "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                header = ["iteration", "timestamp", "TPS"] + list(self.full_knob_dict.keys())
                writer.writerow(header)
            row = [iteration_num, now_str, current_tps] + [combined[k] for k in self.full_knob_dict.keys()]
            writer.writerow(row)

        return False
