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

class Normalizer:
    def __init__(self, knob_dict: dict):
        """
        Initializes the Normalizer with a dictionary defining each MySQL knob and its value range.
        """
        self.knob_dict = knob_dict
        # Store knobs in a stable order so the i-th normalized value corresponds to the i-th knob
        self.knob_names = list(knob_dict.keys())

    def denormalize(self, normalized_values: list) -> dict:
        """
        Convert a list of normalized values (range 0-1) back to their original type/range.
        """
        if len(normalized_values) != len(self.knob_names):
            raise ValueError(
                f"Input list length {len(normalized_values)} does not match "
                f"knob count {len(self.knob_names)}."
            )

        config_dict = {}
        for i, knob in enumerate(self.knob_names):
            knob_type, (min_val, max_val, default_val) = self.knob_dict[knob]
            norm_val = normalized_values[i]

            if knob_type == "integer":
                # Integer knob => use min-max scaling + round
                real_value = min_val + norm_val * (max_val - min_val)
                real_value = round(real_value)
                # dummy fix for precision errors produced during normalization 
                if real_value >= max_val:
                    real_value = max_val
                if real_value <= min_val:
                    real_value = min_val

            elif knob_type == "boolean":
                # Boolean knob => threshold rule
                # If normalized < 0.5 => min_val, else => max_val
                real_value = min_val if norm_val < 0.5 else max_val

            else:
                raise NotImplementedError(f"Unsupported knob type: {knob_type}")

            config_dict[knob] = real_value

        return config_dict

    def normalize(self, config: dict) -> list:
        """
        Convert a configuration dictionary with real values into a list of normalized values (range 0-1).
        """
        normalized_values = []
        for knob in self.knob_names:
            if knob not in config:
                raise ValueError(f"Missing knob '{knob}' in configuration dictionary.")
            real_value = config[knob]
            knob_type, (min_val, max_val, default_val) = self.knob_dict[knob]

            if knob_type == "integer":
                if max_val == min_val:
                    raise ValueError(f"For knob '{knob}', max_val equals min_val.")
                norm_val = (real_value - min_val) / (max_val - min_val)

            elif knob_type == "boolean":
                if real_value == min_val:
                    norm_val = 0.0
                elif real_value == max_val:
                    norm_val = 1.0
                else:
                    raise ValueError(
                        f"For boolean knob '{knob}', value must equal either {min_val} or {max_val}."
                    )

            else:
                raise NotImplementedError(f"Unsupported knob type: {knob_type}")

            normalized_values.append(norm_val)

        return normalized_values

    def get_default_normalized_values(self) -> list:
        """
        Returns a list of normalized values (range 0-1) corresponding to the default values
        for each knob as specified in the knob_dict.
        """
        # Build a configuration dictionary from the default values for each knob.
        default_config = {
            knob: self.knob_dict[knob][1][2]  # Extract the default value
            for knob in self.knob_names
        }
        # Use the normalize method to compute the normalized values.
        return self.normalize(default_config)


if __name__ == "__main__":
    knob_dict = {
        "table_open_cache":         ["integer", [4000, 524288, 4000]],
        "innodb_buffer_pool_size":  ["integer", [134217728, 16106127360, 134217728]],
        "max_heap_table_size":      ["integer", [16384, 16106127360, 16777216]],
        "transaction_prealloc_size":["integer", [1024, 131072, 4096]],
        "transaction_alloc_block_size": ["integer", [1024, 131072, 8192]],
        "innodb_random_read_ahead": ["boolean", ["ON", "OFF", "OFF"]],
    }

    normalizer = Normalizer(knob_dict)

    # Get the normalized values based on the default values.
    default_normalized = normalizer.get_default_normalized_values()
    print("Default Normalized Values:")
    for knob_name, norm_val in zip(normalizer.knob_names, default_normalized):
        print(f"  {knob_name}: {norm_val}")

    # Denormalize the default normalized values to see the original configuration.
    default_denormalized = normalizer.denormalize(default_normalized)
    print("\nDenormalized Values for the Default Normalized Values:")
    for knob_name, value in default_denormalized.items():
        print(f"  {knob_name}: {value}")
