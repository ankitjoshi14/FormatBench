import json, time, math
from pathlib import Path

import duckdb as dd
import pandas as pd
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RESULTS = ROOT / "results"
PROF = ROOT / "profiling"
RESULTS.mkdir(exist_ok=True, parents=True)
PROF.mkdir(exist_ok=True, parents=True)

CFG = yaml.safe_load((ROOT / "config.yaml").read_text())

def percentile(vals, p):
    vals = sorted(vals)
    if not vals: return None
    k = (len(vals)-1) * (p/100.0)
    f = math.floor(k); c = math.ceil(k)
    if f == c: return vals[int(k)]
    return vals[f] + (vals[c]-vals[f]) * (k - f)

def resolve_path(path_str: str) -> Path:
    """Return absolute path for stored manifest entries (accepts relative or absolute)."""
    p = Path(path_str)
    return p if p.is_absolute() else (ROOT / p)

def source_expr(fmt, pv=None):
    if fmt == "parquet":
        p = resolve_path(pv["full_path"])
        return f"read_parquet('{p.as_posix()}')"
    if fmt == "csv":
        return f"read_csv_auto('{(DATA / 'lineitem.csv').as_posix()}')"
    if fmt == "ndjson":
        return f"read_json_auto('{(DATA / 'lineitem.ndjson').as_posix()}')"
    raise ValueError(fmt)

# --- Parquet bytes & groups via Parquet metadata (not DuckDB profiling) ---

def parquet_total_bytes_and_groups(parquet_path: Path):
    pf = pq.ParquetFile(parquet_path)
    total_groups = pf.metadata.num_row_groups
    total_bytes = sum(pf.metadata.row_group(i).total_byte_size for i in range(total_groups))
    return total_bytes, total_groups

def parquet_bytes_and_groups_for_cutoff(parquet_path: Path, cutoff_date_str: str):
    """
    Estimate scanned bytes and row-groups using Parquet row-group statistics
    for column 'l_shipdate'. A group is scanned for predicate (l_shipdate < cutoff)
    unless MIN(l_shipdate) >= cutoff.
    """
    pf = pq.ParquetFile(parquet_path)
    total_groups = pf.metadata.num_row_groups
    total_bytes = 0
    scanned_bytes = 0
    scanned_groups = 0

    arrow_schema = pf.schema_arrow
    col_index = arrow_schema.get_field_index("l_shipdate")
    if col_index == -1:
        col_index = None

    cutoff = pd.to_datetime(cutoff_date_str).date()

    def _to_date(val):
        try:
            if val is None: return None
            if isinstance(val, bytes): val = val.decode("utf-8")
            if isinstance(val, (int, float)):
                return (pd.Timestamp("1970-01-01") + pd.to_timedelta(int(val), unit="D")).date()
            return pd.to_datetime(val).date()
        except Exception:
            return None

    for i in range(total_groups):
        rg = pf.metadata.row_group(i)
        rg_bytes = rg.total_byte_size
        total_bytes += rg_bytes

        if col_index is None:
            scanned_bytes += rg_bytes; scanned_groups += 1; continue

        col = rg.column(col_index)
        stats = col.statistics
        if stats is None:
            scanned_bytes += rg_bytes; scanned_groups += 1; continue

        mind = _to_date(getattr(stats, "min", None))
        # Predicate: l_shipdate < cutoff ⇒ skip when MIN >= cutoff
        if mind is not None and mind >= cutoff:
            continue
        else:
            scanned_bytes += rg_bytes; scanned_groups += 1

    return scanned_bytes, total_bytes, scanned_groups, total_groups

# --- Cutoffs from a file (constants-only quantile calls) ---

def compute_selectivity_cutoffs(con: dd.DuckDBPyConnection, fracs, sample_path: str):
    """
    Compute DATE cutoffs from a file (Parquet/CSV). Returns {fraction: 'YYYY-MM-DD'}.
    DuckDB requires quantile percentiles to be constants; loop in Python.
    """
    sample_path = str(sample_path)
    src = f"read_parquet('{sample_path}')" if sample_path.endswith(".parquet") \
          else f"read_csv_auto('{sample_path}')"
    out = {}
    for f in fracs:
        q = con.execute(
            f"SELECT strftime(quantile_cont(l_shipdate, {float(f)}), '%Y-%m-%d') FROM {src}"
        ).fetchone()[0]
        out[float(f)] = q
    return out

# --- Date expression helper for NDJSON vs others ---

def shipdate_expr_for(fmt: str) -> str:
    # NDJSON may store epoch-ms (BIGINT) or ISO strings; normalize to DATE
    if fmt == "ndjson":
        return ("CASE WHEN typeof(l_shipdate) = 'BIGINT' "
                "THEN CAST(to_timestamp(l_shipdate::DOUBLE / 1000.0) AS DATE) "
                "ELSE CAST(l_shipdate AS DATE) END")
    return "l_shipdate"

# --- Queries ---

def sql_q1(src, shipdate_expr, cutoff_date_str):
    return f"SELECT SUM(l_extendedprice) FROM {src} WHERE {shipdate_expr} < DATE '{cutoff_date_str}'"

def sql_q1_nosarg(src, shipdate_expr, year_str):
    return f"""
        SELECT SUM(l_extendedprice) FROM {src}
        WHERE strftime({shipdate_expr}, '%Y-%m-%d') LIKE '{year_str}-%'
    """

def sql_q2_narrow(src):
    return f"SELECT l_orderkey, l_partkey, l_extendedprice FROM {src};"

def sql_q2_wide(src):
    return f"""
        SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
               l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
               l_shipdate, l_commitdate
        FROM {src};
    """

def sql_q3_scan_group(src):
    return f"""
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity) AS sum_qty,
               SUM(l_extendedprice) AS sum_base_price,
               COUNT(*) AS cnt
        FROM {src}
        GROUP BY l_returnflag, l_linestatus;
    """
        
def run_query_with_profile(con, sql, profile_path: Path):
    con.execute("PRAGMA enable_profiling='json';")
    con.execute("PRAGMA profiling_mode='detailed';")
    con.execute(f"PRAGMA profiling_output='{profile_path.as_posix()}';")
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    t1 = time.perf_counter()
    return (t1 - t0) * 1000.0

def main():
    con = dd.connect(database=":memory:")

    # Load manifest, prefer sorted variants for each (row_group_mb, compression)
    pv = json.loads((RESULTS / "parquet_variants.json").read_text())
    # Use ALL variants (sorted and unsorted)
    parquet_variants = list(pv)

    # Compute cutoffs using a sorted Parquet file if present
    selectivities = CFG["selectivities"]
    sample_path = next((resolve_path(p["full_path"]) for p in pv if p.get("sorted")),
                       (resolve_path(pv[0]["full_path"]) if pv else (DATA / "lineitem.csv")))
    cutoffs = compute_selectivity_cutoffs(con, selectivities, sample_path)
    year_for_nosarg = "1996"

    rows = []

    # --- Parquet variants ---
    for pv_info in parquet_variants:
        rg_mb = pv_info["row_group_mb_target"]
        codec = pv_info["compression"]
        sorted_flag = pv_info.get("sorted", False)
        src = source_expr("parquet", pv=pv_info)
        parquet_path = resolve_path(pv_info["full_path"])
        ship = shipdate_expr_for("parquet")

        # Q1: selective filter at 1/10/50%
        for sel in selectivities:
            prof_dir = PROF / f"parquet_rg{rg_mb}_{codec.lower()}_sel{int(sel*100)}"
            prof_dir.mkdir(parents=True, exist_ok=True)
            times = []
            for r in range(CFG["runs_per_case"]):
                prof = prof_dir / f"run{r}.json"
                ms = run_query_with_profile(con, sql_q1(src, ship, cutoffs[sel]), prof)
                times.append(ms)
            p50, p95 = percentile(times, 50), percentile(times, 95)
            sb, tb, sg, tg = parquet_bytes_and_groups_for_cutoff(parquet_path, cutoffs[sel])
            rows.append({
                "query": "q1_filter_agg",
                "format": "parquet",
                "selectivity": sel,
                "row_group_mb": rg_mb,
                "compression": codec,
                "column_mode": "n/a",
                "runs": CFG["runs_per_case"],
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
                "bytes_scanned": sb,
                "rowgroups_total": tg,
                "rowgroups_scanned": sg,
                "pushdown_hit": True if sg is not None and tg is not None and sg < tg else None,
                "sorted": sorted_flag
            })

        # Q1 non-sargable (force full scan)
        prof_dir = PROF / f"parquet_rg{rg_mb}_{codec.lower()}_nosarg"
        prof_dir.mkdir(parents=True, exist_ok=True)
        times = []
        for r in range(CFG["runs_per_case"]):
            prof = prof_dir / f"run{r}.json"
            ms = run_query_with_profile(con, sql_q1_nosarg(src, ship, year_for_nosarg), prof)
            times.append(ms)
        p50, p95 = percentile(times, 50), percentile(times, 95)
        tb, tg = parquet_total_bytes_and_groups(parquet_path)
        rows.append({
            "query": "q1_nosarg",
            "format": "parquet",
            "selectivity": None,
            "row_group_mb": rg_mb,
            "compression": codec,
            "column_mode": "n/a",
            "runs": CFG["runs_per_case"],
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "bytes_scanned": tb,
            "rowgroups_total": tg,
            "rowgroups_scanned": tg,
            "pushdown_hit": False,
            "sorted": sorted_flag
        })

        # Q2 narrow / wide projection
        for label, sql_builder, cols in [("narrow", sql_q2_narrow, 3), ("wide", sql_q2_wide, 12)]:
            prof_dir = PROF / f"parquet_rg{rg_mb}_{codec.lower()}_q2_{label}"
            prof_dir.mkdir(parents=True, exist_ok=True)
            times = []
            for r in range(CFG["runs_per_case"]):
                prof = prof_dir / f"run{r}.json"
                ms = run_query_with_profile(con, sql_builder(src), prof)
                times.append(ms)
            p50, p95 = percentile(times, 50), percentile(times, 95)
            sb, tb, sg, tg = None, None, None, None  # not applicable
            rows.append({
                "query": "q2_projection",
                "format": "parquet",
                "selectivity": None,
                "row_group_mb": rg_mb,
                "compression": codec,
                "column_mode": label,
                "runs": CFG["runs_per_case"],
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
                "bytes_scanned": None,
                "rowgroups_total": tg,
                "rowgroups_scanned": sg,
                "pushdown_hit": None,
                "cols_projected": cols,
                "sorted": sorted_flag
            })

        # Q3 scan+group
        prof_dir = PROF / f"parquet_rg{rg_mb}_{codec.lower()}_q3"
        prof_dir.mkdir(parents=True, exist_ok=True)
        times = []
        for r in range(CFG["runs_per_case"]):
            prof = prof_dir / f"run{r}.json"
            ms = run_query_with_profile(con, sql_q3_scan_group(src), prof)
            times.append(ms)
        p50, p95 = percentile(times, 50), percentile(times, 95)
        rows.append({
            "query": "q3_scan_group",
            "format": "parquet",
            "selectivity": None,
            "row_group_mb": rg_mb,
            "compression": codec,
            "column_mode": "n/a",
            "runs": CFG["runs_per_case"],
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "bytes_scanned": None,
            "rowgroups_total": None,
            "rowgroups_scanned": None,
            "pushdown_hit": None,
            "sorted": sorted_flag
        })

    # --- Row formats: CSV, NDJSON (no pruning; bytes ~ file size) ---
    row_formats = ["csv", "ndjson"]
    for fmt in CFG["formats"]:
        if fmt not in row_formats: continue
        file_path = DATA / ("lineitem.csv" if fmt == "csv" else "lineitem.ndjson")
        file_size = file_path.stat().st_size
        src = source_expr(fmt)
        ship = shipdate_expr_for(fmt)

        # Q1 with three selectivities
        for sel in selectivities:
            prof_dir = PROF / f"{fmt}_sel{int(sel*100)}"
            prof_dir.mkdir(parents=True, exist_ok=True)
            times = []
            for r in range(CFG["runs_per_case"]):
                prof = prof_dir / f"run{r}.json"
                ms = run_query_with_profile(con, sql_q1(src, ship, cutoffs[sel]), prof)
                times.append(ms)
            p50, p95 = percentile(times, 50), percentile(times, 95)
            rows.append({
                "query": "q1_filter_agg",
                "format": fmt,
                "selectivity": sel,
                "row_group_mb": None,
                "compression": None,
                "column_mode": "n/a",
                "runs": CFG["runs_per_case"],
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
                "bytes_scanned": file_size,
                "rowgroups_total": None,
                "rowgroups_scanned": None,
                "pushdown_hit": False
            })

        # Q2 projection narrow / wide
        for label, sql_builder, cols in [("narrow", sql_q2_narrow, 3), ("wide", sql_q2_wide, 12)]:
            prof_dir = PROF / f"{fmt}_q2_{label}"
            prof_dir.mkdir(parents=True, exist_ok=True)
            times = []
            for r in range(CFG["runs_per_case"]):
                prof = prof_dir / f"run{r}.json"
                ms = run_query_with_profile(con, sql_builder(src), prof)
                times.append(ms)
            p50, p95 = percentile(times, 50), percentile(times, 95)
            rows.append({
                "query": "q2_projection",
                "format": fmt,
                "selectivity": None,
                "row_group_mb": None,
                "compression": None,
                "column_mode": label,
                "runs": CFG["runs_per_case"],
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
                "bytes_scanned": file_size,
                "rowgroups_total": None,
                "rowgroups_scanned": None,
                "pushdown_hit": False,
                "cols_projected": cols
            })

        # Q3 scan+group
        prof_dir = PROF / f"{fmt}_q3"
        prof_dir.mkdir(parents=True, exist_ok=True)
        times = []
        for r in range(CFG["runs_per_case"]):
            prof = prof_dir / f"run{r}.json"
            ms = run_query_with_profile(con, sql_q3_scan_group(src), prof)
            times.append(ms)
        p50, p95 = percentile(times, 50), percentile(times, 95)
        rows.append({
            "query": "q3_scan_group",
            "format": fmt,
            "selectivity": None,
            "row_group_mb": None,
            "compression": None,
            "column_mode": "n/a",
            "runs": CFG["runs_per_case"],
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "bytes_scanned": file_size,
            "rowgroups_total": None,
            "rowgroups_scanned": None,
            "pushdown_hit": False
        })

    df = pd.DataFrame(rows)
    df.to_csv(RESULTS / "metrics.csv", index=False)
    print("Saved metrics to results/metrics.csv and profiles to profiling/")

if __name__ == "__main__":
    main()
