#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot: Polemic vs Archivalization (threshold >= 7, min_n=3)
Outputs:
- outputs/fig_polemic_vs_archival_threshold7.png
- outputs/table_yearly_polemic_vs_archival_threshold7.csv
- outputs/table_yearly_polemic_vs_archival_threshold7.xlsx (if openpyxl available)
- outputs/table_yearly_strong_archival_top_refs_threshold7.csv
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "outputs"
OUTDIR.mkdir(parents=True, exist_ok=True)

CANDIDATE_INPUTS = [
    ROOT / "data_raw" / "events_raw_4d_working.csv",
    ROOT / "data_raw" / "events_raw_4d_enriched_geo.csv",
    ROOT / "data_raw" / "events_raw_4d_enriched_city.csv",
    ROOT / "data_raw" / "events_raw_4d_enriched.csv",
    ROOT / "data_raw" / "events_raw.csv",
]

THRESH = 7.0
MIN_N = 3

OUT_FIG = OUTDIR / "fig_polemic_vs_archival_threshold7.png"
OUT_CSV = OUTDIR / "table_yearly_polemic_vs_archival_threshold7.csv"
OUT_XLSX = OUTDIR / "table_yearly_polemic_vs_archival_threshold7.xlsx"
OUT_TOPREFS = OUTDIR / "table_yearly_strong_archival_top_refs_threshold7.csv"


def pick_input() -> Path:
    for p in CANDIDATE_INPUTS:
        if p.exists() and p.is_file():
            return p
    raise FileNotFoundError(
        "No input CSV found. Checked:\n" + "\n".join(str(p) for p in CANDIDATE_INPUTS)
    )


def ensure_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df))
    return pd.to_numeric(df[col], errors="coerce")


def excel_export(df: pd.DataFrame, out_path: Path) -> bool:
    try:
        import openpyxl  # noqa: F401

        df.to_excel(out_path, index=False)
        return True
    except Exception:
        note = OUTDIR / "NOTE_excel_export_failed.txt"
        note.write_text(
            "Excel export failed. Install openpyxl: python -m pip install openpyxl\n",
            encoding="utf-8",
        )
        return False


def main():
    inp = pick_input()
    df = pd.read_csv(inp).fillna("")

    # which archival score to use
    if "archivalization_score_v2" in df.columns:
        arch_col = "archivalization_score_v2"
    elif "archivalization_score" in df.columns:
        arch_col = "archivalization_score"
    else:
        raise KeyError("Need archivalization_score_v2 or archivalization_score.")

    # sanity checks
    for c in ["year", "source", "polemic_score"]:
        if c not in df.columns:
            raise KeyError(f"Missing required column: {c}")

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["polemic_score"] = ensure_numeric(df, "polemic_score")
    df["arch_score"] = ensure_numeric(df, arch_col)

    # keep only valid rows
    df = df.dropna(subset=["year", "polemic_score", "arch_score"]).copy()
    df["year"] = df["year"].astype(int)

    df["is_strong_archival"] = (df["arch_score"] >= THRESH).astype(int)

    # yearly aggregates
    yearly = (
        df.groupby("year", as_index=False)
        .agg(
            n_events=("year", "size"),
            strong_archival_share=("is_strong_archival", "mean"),
            mean_polemic_score=("polemic_score", "mean"),
            mean_arch_score=("arch_score", "mean"),
        )
        .sort_values("year")
    )
    yearly["strong_archival_share_pct"] = yearly["strong_archival_share"] * 100.0
    yearly["ok"] = (yearly["n_events"] >= MIN_N).astype(int)

    yearly.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    xlsx_ok = excel_export(yearly, OUT_XLSX)

    # top refs: per ok-year, list items with arch_score >= THRESH
    top_rows = []
    ok_years = yearly.loc[yearly["ok"] == 1, "year"].tolist()

    for y in ok_years:
        sub = df[(df["year"] == y) & (df["arch_score"] >= THRESH)].copy()
        if sub.empty:
            continue

        # normalize col names for output
        sub = sub.rename(columns={"arch_score": "archival_score_used"})

        # keep only some cols if present
        keep = ["year"]
        if "event_id" in sub.columns:
            keep.append("event_id")
        if "title" in sub.columns:
            keep.append("title")
        keep += ["archival_score_used", "polemic_score", "source"]

        sub = sub[keep].sort_values(
            ["archival_score_used", "polemic_score"], ascending=[False, False]
        ).head(10)

        # ensure 'year' is the first column (WITHOUT insert)
        cols = ["year"] + [c for c in sub.columns if c != "year"]
        sub = sub[cols]

        top_rows.append(sub)

    if top_rows:
        top_df = pd.concat(top_rows, ignore_index=True)
    else:
        top_df = pd.DataFrame(
            columns=["year", "event_id", "title", "archival_score_used", "polemic_score", "source"]
        )

    top_df.to_csv(OUT_TOPREFS, index=False, encoding="utf-8-sig")

    # plot only ok years
    plot_df = yearly[yearly["ok"] == 1].copy()
    if plot_df.empty:
        raise RuntimeError(f"No years satisfy min_n={MIN_N}.")

    x = plot_df["year"].astype(int).tolist()
    y_share = plot_df["strong_archival_share_pct"].tolist()
    y_polemic = plot_df["mean_polemic_score"].tolist()
    y_n = plot_df["n_events"].tolist()

    fig, ax = plt.subplots(figsize=(11.5, 6.2))

    l1 = ax.plot(x, y_share, marker="o", label=f"Strong archival share (>= {THRESH:g})")
    ax.set_ylabel(f"Strong archival share (%) (arch_score >= {THRESH:g})")

    ax2 = ax.twinx()
    l2 = ax2.plot(x, y_polemic, marker="s", label="Mean polemic_score")
    ax2.set_ylabel("Mean polemic_score")

    ax3 = ax.twinx()
    ax3.spines["right"].set_position(("axes", 1.08))
    ax3.set_frame_on(True)
    ax3.patch.set_visible(False)
    l3 = ax3.plot(x, y_n, linestyle="--", marker="o", label="n_events (year)")
    ax3.set_ylabel("n_events (year)")

    ax.set_title(f"Polemic vs. Archivalization (Threshold >= {THRESH:g}, min_n={MIN_N})")
    ax.set_xlabel("year")

    # x-axis ticks: only actual plotted years
    years_ticks = sorted(set(int(v) for v in x))
    ax.set_xticks(years_ticks)
    ax.set_xticklabels([str(v) for v in years_ticks], rotation=35, ha="right")
    ax.set_xlim(min(years_ticks) - 1, max(years_ticks) + 1)

    lines = l1 + l2 + l3
    labels = [ln.get_label() for ln in lines]
    ax.legend(lines, labels, loc="upper left")

    plt.tight_layout()
    plt.savefig(OUT_FIG, dpi=200)
    plt.close(fig)

    print("Done. Wrote:")
    print(f" - {OUT_CSV}")
    if xlsx_ok:
        print(f" - {OUT_XLSX}")
    else:
        print(" - Excel export skipped (see outputs/NOTE_excel_export_failed.txt)")
    print(f" - {OUT_FIG}")
    print(f" - {OUT_TOPREFS}")
    print()
    print(f"Note: Only years with n_events >= {MIN_N} are plotted; all years are kept in CSV with column 'ok'.")


if __name__ == "__main__":
    main()