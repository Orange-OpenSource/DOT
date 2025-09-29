
## DOT: Dynamic Knob Selection and Online Sampling for Automated Database Tuning

### Prerequisites
1. **Database**: Install MySQL on your target machine. For example, on Ubuntu:
   ```bash
   sudo apt update
   sudo apt install mysql-server

2. **User Setup**: Create a user for DOT with login `dot` and password `dot`:

   ```sql
   CREATE USER 'dot'@'%' IDENTIFIED BY 'dot';
   GRANT ALL PRIVILEGES ON *.* TO 'dot'@'%';
   FLUSH PRIVILEGES;
   ```

### Running Experiments

1. **Configure Benchmark**

   * Place your benchmark scripts in the `scripts/` folder.
   * Adjust the access patterns and workload parameters in `exp_config.json`.

2. **Custom Benchmarks (Optional)**

   * To integrate custom benchmarks, edit `MySQLDriver.py`:

     * Replace the argument parser as needed.
     * Update the `execute_benchmark` commands to match your setup.

3. **SSH Access**

   * Ensure the tuner machine can SSH into your DBMS host without a password vis ssh key:

     ```bash
     ssh user@<dbms-host>
     ```
   * Update SSH key paths in `exp_config.json` under `ssh` settings.

4. **Tuning Configuration**

   * Open your JSON config file (e.g., `configs_example/dot_config.json`).
   * Define knobs and their ranges in the `knob_dict`:

     ```json
     "knob_dict": {
       "innodb_buffer_pool_size": [min, max, default],
       ....
     }
     ```
   * Choose tuning situation:

     * `"strategy": "basic = 1"` for plain Bayesian Optimization
     * `"strategy": "lrt = 1"` for DOT with Likelihood-Ratio Testing
     * `"strategy": "ts = 1"` for DOT with Thompson Sampling
     * `"strategy": "budget = XX"` for DOT with different benchmark budget
     * `"with prior or not": "random = 1 or 0"` for a ordered knob list or not
     
   * Set benchmarking budget, adaptation settings, and other parameters as needed.

5. **Run DOT**

   ```bash
   python main.py configs/dot_config.json
   # For debug mode:
   python main.py configs/dot_config.json --debug
   ```


6. **Dependency**

* Python: 3.10 and requirements
* TPCC: https://github.com/Percona-Lab/tpcc-mysql
* Sysbench: https://github.com/akopytov/sysbench

---
*For more details and examples, see the `examples/` directory.*


