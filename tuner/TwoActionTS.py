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

class TwoActionTS:
    def __init__(self, epsilon: float = 0.1):
        """
        Two-action Thompson Sampling with ε-greedy exploration.

        Actions:
          0 = decrease (down)
          1 = increase (up)

        Parameters
        ----------
        epsilon : float
            Probability of taking a random action instead of TS.
        """
        print("initialize 2-action bandit")
        self.epsilon = epsilon
        # α and β for each of the 2 actions
        self.alpha = [1.0, 1.0]
        self.beta  = [1.0, 1.0]
        self._last_action = None

    def select(self) -> int:
        """
        Choose an action:
          - With probability ε: pick uniformly at random.
          - Else: sample θ_i ~ Beta(α[i],β[i]) for i=0,1 and pick argmax.

        Remembers action for the subsequent update().
        """
        if random.random() < self.epsilon:
            action = random.randrange(2)
            print(f"ε-explore: randomly chosen action {action}")
        else:
            θ = [beta.rvs(self.alpha[i], self.beta[i]) for i in range(2)]
            action = max(range(2), key=lambda i: θ[i])
            print(f"TS-sample: θ_samples={θ}, chosen action {action}")

        self._last_action = action
        return action

    def update(self, reward: float):
        """
        Update the Beta posterior for the last-chosen action using reward ∈ [0,1].
        """
        if self._last_action is None:
            print("No previous select() call—skipping update")
            return

        a = self._last_action
        print(f"updating action {a} with reward {reward}")
        self.alpha[a] += reward
        self.beta[a]  += (1.0 - reward)
        self._last_action = None
        print("new α:", self.alpha)
        print("new β:", self.beta)


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
