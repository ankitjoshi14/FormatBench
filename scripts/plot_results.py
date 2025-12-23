import os
from pathlib import Path
import pandas as pd

# Ensure matplotlib and font caches are writable to avoid warnings in sandboxed envs
ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".mplconfig"))
os.environ.setdefault("XDG_CACHE_HOME", str(ROOT / ".mplconfig"))

import matplotlib.pyplot as plt

RESULTS = ROOT / "results"
PLOTS = ROOT / "plots"
PLOTS.mkdir(exist_ok=True, parents=True)

df = pd.read_csv(RESULTS / "metrics.csv")

# -------------------------------
# 1) Bytes vs selectivity (Q1 only)
# -------------------------------
# 1) Bytes vs selectivity (Q1 only)
sub = df[(df["query"] == "q1_filter_agg")].copy()

# Two parquet subsets at the smallest available row_group_mb
parq_all = sub[sub["format"] == "parquet"].copy()
plt.figure()

# Parquet (sorted)
parq_s = parq_all[parq_all.get("sorted", False) == True]
if not parq_s.empty:
    min_rg_s = parq_s["row_group_mb"].dropna().min()
    ps = parq_s[parq_s["row_group_mb"] == min_rg_s]
    g = ps.groupby("selectivity", as_index=False)["bytes_scanned"].median()
    plt.plot(g["selectivity"]*100, g["bytes_scanned"]/1e6, marker="o", label=f"parquet (sorted, {int(min_rg_s)}MB)")

# Parquet (unsorted)
parq_u = parq_all[parq_all.get("sorted", False) == False]
if not parq_u.empty:
    min_rg_u = parq_u["row_group_mb"].dropna().min()
    pu = parq_u[parq_u["row_group_mb"] == min_rg_u]
    g = pu.groupby("selectivity", as_index=False)["bytes_scanned"].median()
    plt.plot(g["selectivity"]*100, g["bytes_scanned"]/1e6, marker="o", label=f"parquet (unsorted, {int(min_rg_u)}MB)")

# Row formats
csv = sub[sub["format"]=="csv"]
if not csv.empty:
    g = csv.groupby("selectivity", as_index=False)["bytes_scanned"].median()
    plt.plot(g["selectivity"]*100, g["bytes_scanned"]/1e6, marker="o", label="csv")

ndj = sub[sub["format"]=="ndjson"]
if not ndj.empty:
    g = ndj.groupby("selectivity", as_index=False)["bytes_scanned"].median()
    plt.plot(g["selectivity"]*100, g["bytes_scanned"]/1e6, marker="o", label="ndjson")

plt.xlabel("Selectivity (%)")
plt.ylabel("Bytes scanned (MB)")
plt.title("Bytes Scanned vs Selectivity (Q1 filter)")
plt.legend()
plt.tight_layout()
plt.savefig(PLOTS / "fig_bytes_vs_selectivity.png")
plt.close()

# --------------------------------
# 2) P95 vs selectivity (Q1 filter)
# --------------------------------
plt.figure()

if not parq_s.empty:
    g = ps.groupby("selectivity", as_index=False)["p95_ms"].median()
    plt.plot(g["selectivity"]*100, g["p95_ms"], marker="o", label=f"parquet (sorted, {int(min_rg_s)}MB)")
if not parq_u.empty:
    g = pu.groupby("selectivity", as_index=False)["p95_ms"].median()
    plt.plot(g["selectivity"]*100, g["p95_ms"], marker="o", label=f"parquet (unsorted, {int(min_rg_u)}MB)")

if not csv.empty:
    g = csv.groupby("selectivity", as_index=False)["p95_ms"].median()
    plt.plot(g["selectivity"]*100, g["p95_ms"], marker="o", label="csv")
if not ndj.empty:
    g = ndj.groupby("selectivity", as_index=False)["p95_ms"].median()
    plt.plot(g["selectivity"]*100, g["p95_ms"], marker="o", label="ndjson")

plt.xlabel("Selectivity (%)")
plt.ylabel("Latency P95 (ms)")
plt.title("P95 Latency vs Selectivity (Q1 filter)")
plt.legend()
plt.tight_layout()
plt.savefig(PLOTS / "fig_p95_vs_selectivity.png")
plt.close()

# ----------------------------------------------------------
# 3) Skip-rate vs row-group size (Parquet only, Q1 @ 10%)
# ----------------------------------------------------------
parq10 = sub[(sub["format"]=="parquet") & (sub["selectivity"].round(2)==0.10)].copy()
if not parq10.empty and "sorted" in parq10.columns:
    grp = parq10.groupby(["sorted","row_group_mb"], as_index=False).agg(
        rowgroups_total=("rowgroups_total", "max"),
        rowgroups_scanned=("rowgroups_scanned", "max"),
    ).dropna(subset=["rowgroups_total","rowgroups_scanned"])
    if not grp.empty:
        grp["skip_rate"] = 1.0 - (grp["rowgroups_scanned"] / grp["rowgroups_total"]).clip(upper=1.0)
        pivot = grp.pivot(index="row_group_mb", columns="sorted", values="skip_rate").fillna(0.0)
        pivot.columns = ["unsorted" if c is False else "sorted" for c in pivot.columns]
        ax = pivot.plot(kind="bar")
        ax.set_xlabel("Parquet Row-Group Size (MB)")
        ax.set_ylabel("Skip Rate (estimated)")
        ax.set_title("Parquet Row-Group Skip Rate @ ~10% Selectivity")
        ax.legend(title="parquet order", labels=["unsorted","sorted"])

        # If unsorted skips are zero, annotate so the contrast is obvious
        if "unsorted" in pivot.columns:
            handles, labels = ax.get_legend_handles_labels()
            # First legend handle corresponds to unsorted bars (False column)
            if handles and hasattr(handles[0], "patches") and handles[0].patches:
                unsorted_color = handles[0].patches[0].get_facecolor()
            else:
                unsorted_color = "C0"
            for i, mb in enumerate(pivot.index):
                ax.text(i - 0.2, 0.02, "0%", color=unsorted_color,
                        fontsize=8, ha="center", va="bottom")
        plt.tight_layout()
        plt.savefig(PLOTS / "fig_skiprate_vs_rowgroup.png")
        plt.close()

print("Wrote plots/fig_*.png")
