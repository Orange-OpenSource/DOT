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

class TwoActionLRT:
    def __init__(self):
        """
        Two-action likelihood-ratio test without exploration.

        Actions:
          0 = decrease (down)
          1 = increase (up)
        """
        print("Initialize 2-action Likelihood-Ratio Test Bandit")
        self.success = [1.0, 1.0]  # Start with smoothing
        self.failure = [1.0, 1.0]
        self._last_action = None

    def select(self) -> int:
        """
        Choose action deterministically using likelihood-ratio test based on observed successes/failures.

        Returns:
        --------
        action: int (0 or 1)
        """
        # Compute empirical success rates
        success_rates = [
            self.success[i] / (self.success[i] + self.failure[i]) for i in range(2)
        ]

        # Compute likelihood ratio
        likelihood_ratio = math.log((success_rates[1] + 1e-8) / (success_rates[0] + 1e-8))

        # Decision: action 1 if LR positive, else action 0
        action = 1 if likelihood_ratio > 0 else 0

        print(f"LRT: success_rates={success_rates}, likelihood_ratio={likelihood_ratio:.4f}, chosen action={action}")

        self._last_action = action
        return action

    def update(self, reward: float):
        """
        Update success and failure counts for the previously chosen action.

        Parameters:
        -----------
        reward: float (0 or 1)
        """
        if self._last_action is None:
            print("No previous select() call—skipping update")
            return

        action = self._last_action
        self.success[action] += reward
        self.failure[action] += (1.0 - reward)

        print(f"Updating action {action} with reward {reward}")
        print("New success counts:", self.success)
        print("New failure counts:", self.failure)

        self._last_action = None

    @staticmethod
    def reward(cur_perf: float, best_perf: float, n_calls: int, threshold: float = 0.001) -> float:
        """
        Binary reward based on per-step improvement exceeding threshold.

        Returns:
        --------
        reward: float (0 or 1)
        """
        improvement_ratio = (cur_perf - best_perf) / (best_perf if best_perf != 0 else 1.0)
        improvement_per_step = improvement_ratio / n_calls
        reward_val = 1.0 if improvement_per_step > threshold else 0.0

        print(f"cur_perf: {cur_perf}, best_perf: {best_perf}, improvement_per_step: {improvement_per_step}, reward: {reward_val}")
        return reward_val
