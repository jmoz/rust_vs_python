## Results

**1.58x speed increase** using a Rust threadpool compared to a standard synchronised loop in Python.

```
Benchmark 1: ./target/release/rusttest
  Time (mean ± σ):      1.712 s ±  0.033 s    [User: 9.350 s, System: 1.625 s]
  Range (min … max):    1.674 s …  1.788 s    10 runs

Benchmark 2: ../python_polars/.venv/bin/python ../python_polars/main.py
  Time (mean ± σ):      2.698 s ±  0.048 s    [User: 7.518 s, System: 1.226 s]
  Range (min … max):    2.640 s …  2.774 s    10 runs

Summary
  ./target/release/rusttest ran
    1.58 ± 0.04 times faster than ../python_polars/.venv/bin/python ../python_polars/main.py
```