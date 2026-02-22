#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INP = ROOT / "data_raw" / "events_raw_4d_working.csv"
OUTDIR = ROOT / "outputs"
OUTDIR.mkdir(parents=True, exist_ok=True)

KEY_COLS = [
    "year", "city", "geo_scope", "geo_evidence",
    "institution_level", "polemic_score", "archivalization_score_v2", "semantic_noise"
]

def missing_ratio(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return float("nan")
    s = df[col].astype(str).fillna("").str.strip()
    return float((s == "").mean())

def main() -> None:
    if not INP.exists():
        raise FileNotFoundError(f"Missing input: {INP}")

    df = pd.read_csv(INP).fillna("")
    n_rows, n_cols = len(df), len(df.columns)

    rows = []
    rows.append({"metric": "n_records", "value": n_rows})
    rows.append({"metric": "n_fields", "value": n_cols})

    for c in KEY_COLS:
        if c in df.columns:
            rows.append({"metric": f"missing_ratio:{c}", "value": round(missing_ratio(df, c), 6)})

    if "geo_evidence" in df.columns:
        vc = df["geo_evidence"].astype(str).fillna("").str.strip().replace({"": "(empty)"}).value_counts()
        for k, v in vc.items():
            rows.append({"metric": f"geo_evidence_count:{k}", "value": int(v)})

    if "geo_scope" in df.columns:
        vc = df["geo_scope"].astype(str).fillna("").str.strip().replace({"": "(empty)"}).value_counts()
        for k, v in vc.items():
            rows.append({"metric": f"geo_scope_count:{k}", "value": int(v)})

    out = pd.DataFrame(rows)

    out_csv = OUTDIR / "table_data_audit.csv"
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")

    out_md = OUTDIR / "table_data_audit.md"
    md_lines = ["| metric | value |", "|---|---:|"]
    for _, r in out.iterrows():
        md_lines.append(f"| {r['metric']} | {r['value']} |")
    out_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    out_tex = OUTDIR / "table_data_audit.tex"
    tex = [
        "\\begin{tabular}{ll}",
        "\\hline",
        "Metric & Value \\\\",
        "\\hline",
    ]
    for _, r in out.iterrows():
        m = str(r["metric"]).replace("_", "\\_")
        v = str(r["value"]).replace("_", "\\_")
        tex.append(f"{m} & {v} \\\\")
    tex += ["\\hline", "\\end{tabular}"]
    out_tex.write_text("\n".join(tex) + "\n", encoding="utf-8")

    print("Done. Wrote:")
    print(" -", out_csv)
    print(" -", out_md)
    print(" -", out_tex)

if __name__ == "__main__":
    main()
