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

import math
import numpy as np
import random
from scipy.stats import ttest_ind
from typing import List

class IncrementalSupportMask:
    def __init__(self, n_features: int, step: int = 2, initial: int = 4):
        self.n_features = n_features
        self.step = step
        self.selected = initial - step  # so first call bumps to `initial`

    def __call__(self) -> List[bool]:
        self.selected = min(self.selected + self.step, self.n_features)
        mask = [i < self.selected for i in range(self.n_features)]
        return mask

def eliminate_with_scipy_ttest(X, Y, alpha: float) -> List[bool]:
    X = np.asarray(X); Y = np.asarray(Y)
    n_samples, n_features = X.shape
    support_mask: List[bool] = []
    for k in range(n_features):
        vals = X[:, k]
        med = np.median(vals)
        low_mask = vals <= med
        Y_low, Y_high = Y[low_mask], Y[~low_mask]
        if Y_low.size < 2 or Y_high.size < 2:
            support_mask.append(True)
            continue
        _, p_value = ttest_ind(Y_low, Y_high, equal_var=False)
        support_mask.append(p_value <= alpha)
    return support_mask

def update_tuned_knobs(current_tuned_list, full_knob_dict, selection_mask,
                       frozen_values, is_random=False, is_incremental=0, is_SE=0,is_bandit=0, is_TS= 0, is_LRT=0, is_pure_incremental=0, bandit_choice=0):
    current = list(current_tuned_list)
    used = set(current).union(frozen_values.keys())
    keys = list(full_knob_dict.keys())
    updated = False
    element_to_tune = len(current)

    if element_to_tune > selection_mask.count(True):
        updated = True

    if is_incremental == 1 or is_SE == 1:
        print(f"Change search space from {element_to_tune} to {selection_mask.count(True)}")
        element_to_tune = selection_mask.count(True)
    elif is_bandit == 1:
        if selection_mask.count(True) == len(selection_mask):
            print(f"All knobs are important, bandit choice {bandit_choice}, change search space from {element_to_tune} to {element_to_tune + math.floor(bandit_choice * 5)}")
        else:
            print(f"Not all knobs are important, useful knobs length is {selection_mask.count(True) } ,bandit choice {bandit_choice}, change search space from {element_to_tune} to {selection_mask.count(True) + math.floor(bandit_choice * 5)}")
        element_to_tune = selection_mask.count(True) + 5 * bandit_choice
    elif is_TS == 1 or is_LRT == 1: # well they share the same logic
        new_element_to_tune = selection_mask.count(True) +  5 * bandit_choice
        if selection_mask.count(True) == len(selection_mask):
            print(f"All knobs are important, bandit choice {bandit_choice}, change search space from {len(selection_mask)} to {new_element_to_tune}")
        else:
            print(f"Not all knobs are important, useful knobs length is {selection_mask.count(True) } ,bandit choice {bandit_choice}, change search space from {element_to_tune} to {new_element_to_tune}")
        element_to_tune = new_element_to_tune
    elif is_pure_incremental: # well they share the same logic
        new_element_to_tune = selection_mask.count(True) +  5 
        print(f"Pure incremental")
        print(f"Change search space from {element_to_tune} to {new_element_to_tune}")
        element_to_tune = new_element_to_tune   
    else:
        if selection_mask.count(True) == len(selection_mask):
            print(f"All knobs are important, enlarge search space from {element_to_tune} to {math.floor(element_to_tune * 1.5)}")
            element_to_tune = math.floor(element_to_tune * 1.5)
        else:
            print(f"Not all knobs are important, reduce search space from {element_to_tune} to {selection_mask.count(True)}, but add one for exploration {selection_mask.count(True) + 1}")
            element_to_tune = selection_mask.count(True) + 1

    new_list = [k for i, k in enumerate(current) if selection_mask[i] and i < len(current)]

    if is_random:
        while len(new_list) < element_to_tune:
            if len(used) == len(keys):
                break
            for c in random.sample(keys, len(keys)):
                if c not in used:
                    new_list.append(c)
                    used.add(c)
                    updated = True
                    break
    else:
        needed = element_to_tune - len(new_list)
        if needed > 0:
            available = [k for k in keys if k not in used]
            to_add = available[:needed]
            new_list.extend(to_add)
            used.update(to_add)
            updated = bool(to_add)

    print("updated is: ", updated)
    return new_list, updated
