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

import time
import numpy as np

def objective_func(norm_values, driver, normalizer, full_knob_dict,
                   frozen_values, benchmark="sysbench", intermediate_csv=None):
    print('func obj called')
    config_dict = normalizer.denormalize(norm_values)
    for knob, values in full_knob_dict.items():
        if knob not in normalizer.knob_names:
            if knob in frozen_values:
                config_dict[knob] = frozen_values[knob]
            else:
                config_dict[knob] = values[1][2]

    if getattr(driver, 'debug', False):
        tps = np.random.uniform(1000, 2000)
    else:
        success = driver.apply_config_and_restart(config_dict)
        if not success:
            time.sleep(5)
            return 1e9
        time.sleep(2)
        if 'tpch' in benchmark:
            tps = driver.execute_olap(sql_file_path="../benchmark/queries.sql", intermediate_csv= intermediate_csv)
            return tps
        else:
            tps, _, _, _ = driver.execute_oltp(benchmark=benchmark)

    return -tps
