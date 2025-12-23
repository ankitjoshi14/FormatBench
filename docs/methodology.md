# Methodology (Small Factor Matrix)

- **Formats:** Parquet (variants), CSV, NDJSON.
- **Factors:**
  - **Selectivity:** ~1%, 10%, 50% (cut as quantiles on `l_shipdate`).
  - **Parquet Row-Group Size:** 8MB, 32MB, 128MB (approx via rows/group).
  - **Parquet Compression:** NONE, ZSTD.
- **Queries:**
  - **Q1 filter+agg** with three selectivities.
  - **Q1 non-sargable** (`CAST(l_shipdate AS VARCHAR) LIKE '1996-%'`) to disable pushdown.
  - **Q2 projection (narrow vs wide)** to show pruning vs parse costs.
  - **Q3 scan+group** baseline.
- **Metrics:** P50/P95 (ms), bytes scanned (approx), row-groups scanned vs total (Parquet only), pushdown hit flag.
- **Environment:** DuckDB (Python), single node, pinned version; machine specs recorded in `results/PROVENANCE.json`.