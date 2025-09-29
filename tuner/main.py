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
import os
import sys
import time
import random
import math
import numpy as np

from functools import partial
from datetime import datetime
from skopt import gp_minimize

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import KFold
from sklearn.feature_selection import RFECV

from  config import parse_arguments
from  utils import (
    load_config,
    get_knob_dicts,
    load_intermediate_data,
    load_y_data,
    build_search_space,
    get_combined_config,
)
from knob_selection import (
    IncrementalSupportMask,
    eliminate_with_scipy_ttest,
    update_tuned_knobs,
)
from  callbacks import LoggingCallback
from  objective import objective_func
from  contextualTS import ContextualTS
from  TwoActionTS import TwoActionTS
from  TwoActionLRT import TwoActionLRT
# sys.path.append("/home/cloud/dot/Drivers")
from MySQLDriver import MySQLDriver
from Normalizer import Normalizer
# from ContextualTS import ContextualTS

def setup_driver_and_dirs(config_path, config_data, args):
    cfg_name = os.path.splitext(os.path.basename(config_path))[0]

    results_dir = "../exp_results"
    os.makedirs(results_dir, exist_ok=True)

    intermediate_dir = "../intermediate_points"
    os.makedirs(intermediate_dir, exist_ok=True)

    intermediate_csv = os.path.join(intermediate_dir, f"{cfg_name}.csv")

    driver = MySQLDriver(
        remote=config_data["remote_info"],
        ssh_key_path=config_data["ssh_key_path"],
        local_log_dir=config_data["local_log_dir"],
        remote_mycnf_path=config_data["remote_mycnf_path"],
        is_fixed_ram=config_data.get("is_fixed_ram", 0),
        is_limited_cpu=config_data.get("is_limited_cpu", 0),
        objective_metric=config_data.get("objective_metric", "trx"),
        budget_allocator=config_data.get("budget_allocator", 0),
    )
    if args.debug:
        driver.debug = True
        print("Running in DEBUG mode: skipping configuration application and using simulated TPS.")

    return driver, cfg_name, intermediate_csv


def decide_calls(x0, updated_knobs_num, len_knobs, flags, remaining_iters):
    is_basic, is_low, is_SE, is_incremental, is_bandit, is_TS, is_LRT, is_super_low,is_pure_incremental = flags

    if x0 is None:
        initial_pts = 10
        n_calls = 10 * len_knobs
        if is_super_low:
            n_calls = 5 * len_knobs
            print("intiial low iteration number to {n_calls}")
        if is_SE:
            n_calls = 100
    else:
        initial_pts = 0
        if updated_knobs_num == 0:
            n_calls = 10
        else:
            n_calls = 10 * len_knobs
            if is_low:
                print("iteration number to 10")
                n_calls = updated_knobs_num * 10
            if is_super_low:
                print("super low iteration number to 5")
                n_calls = updated_knobs_num * 5
        if is_SE:
            n_calls = remaining_iters - 100

    if is_basic:
        print("is basic tuner, the calls is equals to total iterations")
        n_calls = remaining_iters

    if is_basic  + is_incremental + is_SE +  is_bandit> 2:
        print("Error: only one of is_basic, is_incremental, or is_SE or is_bandit should be set to 1.")
        sys.exit(1)

    if is_incremental:
        n_calls = 25

    print("itration number :", n_calls)
    return initial_pts, min(n_calls, remaining_iters)


def run_optimization_iteration(
    driver, normalizer, full_knob_dict, frozen_values,
    current_knobs, benchmark, x0, y0,
    initial_pts, n_calls, random_state, models_dir, cfg_name, intermediate_csv, debug=False
):
    callback = LoggingCallback(
        normalizer=normalizer,
        models_dir=models_dir,
        cfg_name=cfg_name,
        full_knob_dict=full_knob_dict,
        intermediate_csv_file=intermediate_csv,
        frozen_values=frozen_values,
    )

    print(f"\nStarting iteration with knobs: {current_knobs}")

    objective = partial(
        objective_func,
        driver=driver,
        normalizer=normalizer,
        full_knob_dict=full_knob_dict,
        frozen_values=frozen_values,
        benchmark=benchmark,
        intermediate_csv=intermediate_csv,
    )
    if not debug:
        result = gp_minimize(
            func=objective,
            dimensions=build_search_space(current_knobs),
            x0=x0,
            y0=y0,
            n_initial_points=initial_pts,
            n_calls=n_calls,
            random_state=random_state,
            verbose=False,
            callback=[callback],
            # initial_point_generator="lhs",
            # getattr(driver, "debug", False)
        )
    else:
        print("debug mode, ultra quick bo optimization")
        result = gp_minimize(
            func=objective,
            dimensions=build_search_space(current_knobs),
            x0=x0,
            y0=y0,
            n_initial_points=initial_pts,
            n_calls=n_calls,
            random_state=random_state,
            verbose=False,
            callback=[callback],
            acq_optimizer="sampling",
            acq_func="EI",
            n_points=5,
            # initial_point_generator="lhs",
            # getattr(driver, "debug", False)
        )

    return result.x_iters, result.func_vals


def feature_selection_cycle(
    X_all, y_all, current_knobs, full_knob_dict, frozen_values,
    flags, selector, is_random, intermediate_csv, bandit=None, n_calls=None
):
    # if len(X_all[0]) <= 2:
    #     return current_knobs, 0, X_all, y_all
    is_basic, is_low, is_SE, is_incremental, is_bandit, is_TS, is_LRT, is_super_low, is_pure_incremental = flags
    bandit_choice = -1
    # RFECV branch
    if not is_SE and not is_incremental and not is_bandit and not is_TS and not is_LRT and not is_pure_incremental:
        n_estimators = 100 + 10 * len(current_knobs)
        rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=None, random_state=42, n_jobs=-1)
        cv = KFold(n_splits=5, shuffle=True, random_state=42)
        rfecv = RFECV(estimator=rf, step=1, cv=cv, scoring="neg_mean_squared_error")
        rfecv.fit(X_all, y_all)

        print("Optimal number of features:", rfecv.n_features_)
        print("Selected features mask:", rfecv.support_)
        print("Feature ranking:", rfecv.ranking_)

        mask = rfecv.support_.tolist()

    # Incremental‐mask branch
    elif is_incremental:
        mask = selector()

    # Sign‐test branch
    elif is_SE:
        mask = eliminate_with_scipy_ttest(X_all, y_all, alpha=0.05)

    elif is_bandit:
        print("enter the contextual TS bandit branch")
        
        n_estimators = 100 + 10 * len(current_knobs)
        rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=None, random_state=42, n_jobs=-1)
        cv = KFold(n_splits=5, shuffle=True, random_state=42)
        rfecv = RFECV(estimator=rf, step=1, cv=cv, scoring="neg_mean_squared_error")
        rfecv.fit(X_all, y_all)

        print("Optimal number of features:", rfecv.n_features_)
        print("Selected features mask:", rfecv.support_)
        print("Feature ranking:", rfecv.ranking_)

        mask = rfecv.support_.tolist()
        
        context = 1 if all(mask) else 0 # contextual bandit
        perf_list = load_y_data(
            intermediate_csv
        )
        # print("in main, the perf_list is ", perf_list)
        # print("in main, the perf_list length is ", len(perf_list))
        # print("max(perf_list[-n_calls:] is", max(perf_list[-n_calls:]))
        # print("max(perf_list[:len(perf_list)-n_calls])) is ", max(perf_list[:len(perf_list)-n_calls]))
        cur_perf =  max(perf_list[-n_calls:])
        best_perf =  max(perf_list[:len(perf_list)-n_calls]) if perf_list[:len(perf_list)-n_calls] else 0
        bandit.update(bandit.reward(cur_perf,best_perf, n_calls))
        print("call bandit select")
        bandit_choice = bandit.select(context)
    elif is_TS or is_LRT:
        print("enter the normal TS bandit branch or LRT branch")
        min_features_to_select = 10 if  is_super_low else 1
        
        n_estimators = 100 + 10 * len(current_knobs)
        rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=None, random_state=42, n_jobs=-1)
        cv = KFold(n_splits=5, shuffle=True, random_state=42)
        # rfecv = RFECV(estimator=rf, step=1, cv=cv, scoring="r2")
        rfecv = RFECV(estimator=rf, step=1, cv=cv, scoring="neg_mean_squared_error", min_features_to_select = min_features_to_select)
        rfecv.fit(X_all, y_all)

        print("Optimal number of features:", rfecv.n_features_)
        print("Selected features mask:", rfecv.support_)
        print("Feature ranking:", rfecv.ranking_)

        mask = rfecv.support_.tolist()
        
        perf_list = load_y_data(
            intermediate_csv
        )

        cur_perf =  max(perf_list[-n_calls:])
        best_perf =  max(perf_list[:len(perf_list)-n_calls]) if perf_list[:len(perf_list)-n_calls] else 0
        bandit.update(bandit.reward(cur_perf,best_perf, n_calls))
        print("call bandit select")
        bandit_choice = bandit.select()
    elif is_pure_incremental:
        print("pure incremental feature selection")
        min_features_to_select = 10 if  is_super_low else 1
        
        n_estimators = 100 + 10 * len(current_knobs)
        rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=None, random_state=42, n_jobs=-1)
        cv = KFold(n_splits=5, shuffle=True, random_state=42)
        # rfecv = RFECV(estimator=rf, step=1, cv=cv, scoring="r2")
        rfecv = RFECV(estimator=rf, step=1, cv=cv, scoring="neg_mean_squared_error", min_features_to_select = min_features_to_select)
        rfecv.fit(X_all, y_all)

        print("Optimal number of features:", rfecv.n_features_)
        print("Selected features mask:", rfecv.support_)
        print("Feature ranking:", rfecv.ranking_)
        bandit_choice = 0
        mask = rfecv.support_.tolist()
        
    else:
        print("Error: No feature selection method selected.")
        sys.exit(1)


    print("min_features_to_select is ", min_features_to_select)
    new_knobs, updated = update_tuned_knobs(
        current_knobs,
        full_knob_dict,
        mask,
        frozen_values,
        is_random=is_random,
        is_incremental=is_incremental,
        is_SE=is_SE,
        is_bandit=is_bandit,
        is_TS=is_TS,
        is_LRT=is_LRT,
        is_pure_incremental=is_pure_incremental,
        bandit_choice=bandit_choice,
        
    )

    if updated:
        kind = "RFECV" if (not is_SE and not is_incremental and not is_bandit) else "SE"
        print(f"Updating tuned knobs {kind}:")
        print("  Old tuned knobs:", current_knobs)
        print("  New tuned knobs:", new_knobs)

        best_idx = np.argmin(y_all)
        normalizer = Normalizer({k: full_knob_dict[k] for k in current_knobs})
        best_cfg = get_combined_config(normalizer, full_knob_dict, frozen_values, X_all[best_idx])

        for k in current_knobs:
            if k not in new_knobs:
                frozen_values[k] = best_cfg[k]
                print("frozen_values: ", frozen_values)

        updated_num = max(len(set(new_knobs) - set(current_knobs)),0)
        print("update tuned knobs num ", updated_num)

        new_x0, new_y0 = load_intermediate_data(
            intermediate_csv,
            new_knobs,
            Normalizer({k: full_knob_dict[k] for k in new_knobs})
        )
        return new_knobs, updated_num, new_x0, new_y0

    return current_knobs, 0, X_all, y_all


def main():
    args = parse_arguments()
    config_data = load_config(args.config_path)

    full_knob_dict, _, tuned_keys = get_knob_dicts(
        config_data,
        top_n=config_data["top_n"],
        is_random=config_data.get("is_random", 0),
    )

    driver, cfg_name, intermediate_csv = setup_driver_and_dirs(
        args.config_path, config_data, args
    )

    total_iterations = config_data.get("bayes_opt_settings", {}).get("n_calls", 30)
    random_state    = config_data.get("bayes_opt_settings", {}).get("random_state", 0)
    flags = (
        config_data.get("is_basic", 0),
        config_data.get("is_low", 0),
        config_data.get("is_SE", 0),
        config_data.get("is_incremental", 0),
        config_data.get("is_bandit", 0),
        config_data.get("is_TS", 0),
        config_data.get("is_LRT", 0),
        config_data.get("is_super_low", 0),
        config_data.get("is_pure_incremental", 0),
    )
    is_random = config_data.get("is_random", 0)

    selector = None
    if flags[3] and not flags[2]:
        selector = IncrementalSupportMask(n_features=len(full_knob_dict), step=2, initial=4)

    if os.path.exists(intermediate_csv) and os.path.getsize(intermediate_csv) > 0:
        try:
            x0, y0 = load_intermediate_data(
                intermediate_csv,
                tuned_keys,
                Normalizer({k: full_knob_dict[k] for k in tuned_keys})
            )
            if not x0 or not y0:
                x0, y0 = None, None
        except Exception as e:
            print(f"Warning: Could not load intermediate data due to error: {e}. Using no historical data.")
            x0, y0 = None, None
    else:
        x0, y0 = None, None
    
    bandit = None
    is_bandit = config_data.get("is_bandit", 0)
    is_TS = config_data.get("is_TS", 0)
    is_LRT = config_data.get("is_LRT", 0)
    if is_bandit:
        bandit = ContextualTS()
    if is_TS:
        print("initialize TS")
        bandit = TwoActionTS()
    if is_LRT:
        print("initialize LRT")
        bandit = TwoActionLRT()

    overall_iters = 0
    current_knobs = tuned_keys
    frozen_values = {}
    best_result = None
    updated_knobs_num = 0
  
    while overall_iters < total_iterations:
        initial_pts, n_calls = decide_calls(
            x0,
            updated_knobs_num,
            len(current_knobs),
            flags,
            total_iterations - overall_iters,
        )

        X_all, y_all = run_optimization_iteration(
            driver=driver,
            normalizer=Normalizer({k: full_knob_dict[k] for k in current_knobs}),
            full_knob_dict=full_knob_dict,
            frozen_values=frozen_values,
            current_knobs=current_knobs,
            benchmark=config_data.get("benchmark", "sysbench"),
            x0=x0,
            y0=y0,
            initial_pts=initial_pts,
            n_calls=n_calls,
            random_state=random_state,
            models_dir="../exp_models",
            cfg_name=cfg_name,
            intermediate_csv=intermediate_csv,
            debug = getattr(driver, "debug", False),
        )

        overall_iters += n_calls

        current_knobs, updated_knobs_num, x0, y0 = feature_selection_cycle(
            X_all, y_all,
            current_knobs,
            full_knob_dict,
            frozen_values,
            flags,
            selector,
            is_random,
            intermediate_csv,       # ← now passed here
            bandit =  bandit if bandit else None,
            n_calls = n_calls,
        )
        best_idx = np.argmin(y0)
        best_tps = -y0[best_idx]
        print(f"\n=== Best TPS so far: {best_tps:.2f} ")
        best_result = (
            best_tps,
            get_combined_config(
                Normalizer({k: full_knob_dict[k] for k in current_knobs}),
                full_knob_dict,
                frozen_values,
                x0[best_idx],
            ),
        )

    if best_result:
        best_tps, best_cfg = best_result
        print("\n=== FINAL RESULTS ===")
        print(f"Best TPS observed: {best_tps:.2f}")
        print("Best config:")
        for k, v in best_cfg.items():
            print(f"  {k} = {v}")

        config_json = str(best_cfg).replace("'", '"')
        print("TUNING DONE")


if __name__ == "__main__":
    main()
