# HBDB vs CockroachDB Stress Test Suite

This suite compares **HBDB** (your deterministic Python implementation) vs **CockroachDB**.

## Prerequisites

1.  **CockroachDB**: Must be installed and running.
    ```bash
    # Mac
    brew install cockroach
    cockroach start-single-node --insecure --background
    ```

2.  **Python Dependencies**:
    ```bash
    pip install psycopg2-binary
    ```
    (Note: HBDB runs without this, but the comparison runner needs it).

## Running the Benchmark

Run the `benchmarks.run` module from the project root:

```bash
# Test HBDB (High Contention Transfer)
python3 -m benchmarks.run --db hbdb --workload transfer --threads 4 --duration 30

# Test CockroachDB (High Contention Transfer)
python3 -m benchmarks.run --db crdb --workload transfer --threads 4 --duration 30
```

## Workloads

*   `transfer`: High contention. Updates account balances.
    *   **Goal**: Demonstrate HBDB's ability to handle conflicts without retries.
    *   **Metric**: Throughput (ops/sec) under high concurrency.
*   `kv`: Low contention. Random reads/writes.
    *   **Goal**: Establish baseline performance difference (Go vs Python).

## Expected Results

| Metric | CockroachDB | HBDB |
| :--- | :--- | :--- |
| **High Contention** | Many retry errors (lower eff. throughput) | **Stable high throughput** (Aria win) |
| **Low Contention** | **Extremely high** (Compiled Go) | Moderate (Interpreted Python) |
