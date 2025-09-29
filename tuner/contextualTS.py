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

import random
from scipy.stats import beta
import math

class ContextualTS:
    def __init__(self, epsilon: float = 0.1):
        """
        Contextual Thompson Sampling with ε-greedy exploration.

        Parameters
        ----------
        epsilon : float
            Probability of taking a random action instead of TS.
        """
        print("initialize bandit")
        self.epsilon = epsilon
        # contexts 0 and 1, actions 0=no_add, 1=add_5
        self.alpha = {0: [1.0, 1.0], 1: [1.0, 1.0]}
        self.beta  = {0: [1.0, 1.0], 1: [1.0, 1.0]}
        self._last_context = None
        self._last_action  = None

    def select(self, context: int) -> int:
        """
        Choose an action for the given context.
        With prob ε pick random; else do Thompson Sampling.
        Memorizes (context, action) for update().
        """
        # ε-greedy: random exploration
        if random.random() < self.epsilon:
            action = random.choice([0, 1])
            print(f"ε-explore: randomly chosen action {action}")
        else:
            # Thompson Sampling
            samples = [
                beta.rvs(self.alpha[context][a], self.beta[context][a])
                for a in (0, 1)
            ]
            action = 0 if samples[0] >= samples[1] else 1
            print(f"TS-sample: θ_samples={samples}, chosen action {action}")

        # memorize for update()
        self._last_context = context
        self._last_action  = action
        return action

    def update(self, reward: float):
        """
        Update the posterior for the last-chosen (context, action)
        using the provided reward ∈ [0,1].
        """
        print('updating with reward:', reward)
        if self._last_context is None or self._last_action is None:
            print("No previous select() call—skipping update")
            return

        c = self._last_context
        a = self._last_action

        # Bayesian update: treat reward as fractional “success”
        self.alpha[c][a] += reward
        self.beta[c][a]  += (1.0 - reward)

        # clear last choice
        self._last_context = None
        self._last_action  = None

        print('updated alpha:', self.alpha)
        print('updated beta:', self.beta)
        print("done updating")

    @staticmethod
    def reward(cur_perf: float, best_perf: float, n_calls: int, scale: float = 500.0) -> float:
        """
        Sigmoidal reward based on per-step improvement.
        """
        print("calculating reward")
        print("cur_perf:", cur_perf)
        print("best_perf:", best_perf)
        print("n_calls:", n_calls)
        try:
            improvement_ratio = (cur_perf - best_perf) / best_perf
        except ZeroDivisionError:
            improvement_ratio = 1.0
        print("improvement_ratio:", improvement_ratio)
        improvement_per_step = improvement_ratio / n_calls
        # steep sigmoid centered at 0.1% per call
        reward_val = 1.0 / (1.0 + math.exp(-scale * (improvement_per_step - 0.001)))
        print("reward:", reward_val)
        return reward_val
