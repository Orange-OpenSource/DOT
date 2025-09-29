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
import sys, json, pickle, base64
from skopt import Optimizer

# ── 1. receive the setup line ─────────────────────────────────────────────
init = json.loads(sys.stdin.readline())
space           = pickle.loads(base64.b64decode(init["space_b64"]))
random_state    = init.get("random_state", None)
n_initial       = init.get("n_initial_points", 10)     # <-- NEW

opt = Optimizer(
    space,
    random_state      = random_state,
    n_initial_points  = n_initial,                      # <-- NEW
)

# warm-start
x0, y0 = init.get("x0"), init.get("y0")
if x0 and y0 and len(x0) == len(y0):
    for xi, yi in zip(x0, y0):
        opt.tell(xi, yi)

# first ask
x_curr = opt.ask()
print(json.dumps(x_curr)); sys.stdout.flush()

# main loop
for line in sys.stdin:
    y_val = float(line)
    opt.tell(x_curr, y_val)
    x_curr = opt.ask()
    print(json.dumps(x_curr)); sys.stdout.flush()
