# Metrics Schema (`results/metrics.csv`)

Each row is an aggregated timing/result for one query × format × factor combo. Columns:

| column            | type/units   | meaning |
|-------------------|--------------|---------|
| `query`           | string       | Logical query id (`q1_filter_agg`, `q1_nosarg`, `q2_projection`, `q3_scan_group`). |
| `format`          | string       | Storage format (`parquet`, `csv`, `ndjson`). |
| `selectivity`     | float        | Fraction of rows matched (as quantile cut), populated for Q1 variants; `null` for queries without a filter. |
| `row_group_mb`    | float (MB)   | Target Parquet row-group size; `null` for row formats. |
| `compression`     | string       | Parquet compression (`NONE`, `ZSTD`); `null` for row formats. |
| `column_mode`     | string       | `narrow` (few columns) or `wide` (many columns) for projection query; `null` otherwise. |
| `runs`            | int          | Number of repetitions aggregated into the percentile stats (from `runs_per_case`). |
| `p50_ms`          | float (ms)   | Median runtime across the `runs`. |
| `p95_ms`          | float (ms)   | 95th percentile runtime across the `runs`. |
| `bytes_scanned`   | float (bytes)| Bytes read from storage (DuckDB-reported), `null` when not available. |
| `rowgroups_total` | float        | Total Parquet row groups in the file(s), `null` for row formats or when not recorded. |
| `rowgroups_scanned` | float      | Row groups actually read, for estimating skip rate; `null` for row formats or when not recorded. |
| `pushdown_hit`    | bool         | Whether DuckDB reported predicate pushdown; `null` when not applicable. |
| `sorted`          | bool         | Whether data was sorted by filter key to improve clustering; `null` for row formats without the concept. |
| `cols_projected`  | float        | Number of columns selected (projection query), `null` otherwise. |

Notes:
- Numeric columns may appear as floats due to CSV parsing; treat them as ints where appropriate (`runs`, row-group counts, `cols_projected`).
- Skip-rate ≈ `1 - (rowgroups_scanned / rowgroups_total)` when both fields are present.
