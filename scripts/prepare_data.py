import os, json, hashlib, platform, subprocess
from pathlib import Path

import duckdb as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
try:
    import psutil
except ImportError:  # Optional dependency; handled gracefully
    psutil = None

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RESULTS = ROOT / "results"
DOCS = ROOT / "docs"
DATA.mkdir(exist_ok=True, parents=True)
RESULTS.mkdir(exist_ok=True, parents=True)
DOCS.mkdir(exist_ok=True, parents=True)

CFG = yaml.safe_load((ROOT / "config.yaml").read_text())

def sha256_first_mb(path: Path, mb: int = 2) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read(mb * 1024 * 1024))
    return h.hexdigest()

def ensure_tpch(con: dd.DuckDBPyConnection, scale: float):
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={scale});")

def emit_csv_json(con: dd.DuckDBPyConnection):
    csv = DATA / "lineitem.csv"
    ndjson = DATA / "lineitem.ndjson"
    if not csv.exists():
        con.execute(f"COPY lineitem TO '{csv.as_posix()}' (FORMAT CSV, HEADER, DELIMITER ',');")
    if not ndjson.exists():
        df = con.execute("SELECT * FROM lineitem;").fetchdf()
        # Write dates as ISO strings so comparisons work without BIGINT casts
        df.to_json(ndjson, orient="records", lines=True, date_format="iso", date_unit="s")

def estimate_rows_per_group(sample_file: Path, table_rows: int, target_mb: int) -> int:
    """
    Heuristic rows/group independent of compressed size, tuned so pruning is observable.
    """
    mapping = {
        1: 50_000,   2: 100_000,
        4: 200_000,  8: 400_000,
        16: 800_000, 32: 1_600_000,
        64: 3_200_000, 128: 4_800_000,
    }
    rows = mapping.get(int(target_mb), max(50_000, table_rows // 50))
    return max(10_000, min(rows, table_rows))

def write_parquet_variants(con: dd.DuckDBPyConnection):
    # Materialize Arrow table once (Table, not streaming reader)
    at = con.execute("SELECT * FROM lineitem;").fetch_arrow_table()
    num_rows = at.num_rows
    # Sorted variant to make pruning visible on l_shipdate
    try:
        at_sorted = at.sort_by([("l_shipdate", "ascending")])
    except Exception:
        at_sorted = at

    variants = []
    # Write one baseline parquet to exist for estimation logs (not strictly required)
    baseline = DATA / "lineitem_baseline.parquet"
    if not baseline.exists():
        pq.write_table(at, baseline.as_posix(), compression="zstd")

    for rg_mb in CFG["parquet_variants"]["row_group_mb"]:
        for codec in CFG["parquet_variants"]["compression"]:
            rows_per_group = estimate_rows_per_group(baseline, num_rows, rg_mb)

            # Unsorted
            out = DATA / f"lineitem.parquet.rg{rg_mb}mb.{codec.lower()}.parquet"
            if not out.exists():
                pq.write_table(
                    at,
                    out.as_posix(),
                    compression=None if codec == "NONE" else codec.lower(),
                    row_group_size=rows_per_group,
                )
            meta = pq.ParquetFile(out).metadata
            variants.append({
                "path": out.name,
                "full_path": out.relative_to(ROOT).as_posix(),
                "rows_per_group": rows_per_group,
                "row_groups_total": meta.num_row_groups,
                "compression": codec,
                "row_group_mb_target": rg_mb,
                "sha256_first2mb": sha256_first_mb(out),
                "sorted": False,
            })

            # Sorted
            out_sorted = DATA / f"lineitem.sorted.parquet.rg{rg_mb}mb.{codec.lower()}.parquet"
            if not out_sorted.exists():
                pq.write_table(
                    at_sorted,
                    out_sorted.as_posix(),
                    compression=None if codec == "NONE" else codec.lower(),
                    row_group_size=rows_per_group,
                )
            meta_s = pq.ParquetFile(out_sorted).metadata
            variants.append({
                "path": out_sorted.name,
                "full_path": out_sorted.relative_to(ROOT).as_posix(),
                "rows_per_group": rows_per_group,
                "row_groups_total": meta_s.num_row_groups,
                "compression": codec,
                "row_group_mb_target": rg_mb,
                "sha256_first2mb": sha256_first_mb(out_sorted),
                "sorted": True,
            })

    (RESULTS / "parquet_variants.json").write_text(json.dumps(variants, indent=2))

def write_provenance(con: dd.DuckDBPyConnection):
    def cpu_brand():
        # Prefer platform; fall back to sysctl on macOS for a readable model string
        brand = (platform.processor() or "").strip()
        if brand:
            return brand
        try:
            out = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True)
            out = out.strip()
            if out:
                return out
        except Exception:
            pass
        return None

    def total_ram_bytes():
        # Prefer psutil if present; otherwise use sysconf
        if psutil is not None:
            try:
                return int(psutil.virtual_memory().total)
            except Exception:
                pass
        try:
            pages = os.sysconf("SC_PHYS_PAGES")
            page_size = os.sysconf("SC_PAGE_SIZE")
            return int(pages * page_size)
        except Exception:
            return None

    prov = {
        "duckdb_version": con.execute("SELECT version()").fetchone()[0],
        "os": os.uname().sysname,
        "release": os.uname().release,
        "hostname": platform.node(),
        "cpu_brand": cpu_brand(),
        "logical_cores": os.cpu_count(),
        "physical_cores": psutil.cpu_count(logical=False) if psutil is not None else None,
        "ram_bytes": total_ram_bytes(),
        "python_version": platform.python_version(),
    }
    for p in ["lineitem.csv", "lineitem.ndjson"]:
        fp = DATA / p
        if fp.exists():
            prov[p] = {"size_bytes": fp.stat().st_size, "sha256_first2mb": sha256_first_mb(fp)}
    (RESULTS / "PROVENANCE.json").write_text(json.dumps(prov, indent=2))

def main():
    con = dd.connect(database=":memory:")
    ensure_tpch(con, CFG["dataset"]["tpch_scale"])
    emit_csv_json(con)
    write_parquet_variants(con)
    write_provenance(con)
    print("Prepared: CSV, NDJSON, Parquet variants. See results/ for manifests & provenance.")

if __name__ == "__main__":
    main()
