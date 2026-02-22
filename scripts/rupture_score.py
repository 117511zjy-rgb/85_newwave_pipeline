#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _root() -> Path:
    # .../85_newwave_pipeline/scripts/rupture_score.py -> root is parents[1]
    return Path(__file__).resolve().parents[1]


def _to_float(s: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default).astype(float)


def _scol(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype(str).fillna("").str.strip()
    return pd.Series([""] * len(df), index=df.index)


def _ensure_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "year" in df.columns:
        y = pd.to_numeric(df["year"], errors="coerce")
    elif "date" in df.columns:
        y = pd.to_numeric(df["date"].astype(str).str.slice(0, 4), errors="coerce")
    else:
        raise ValueError("Input must contain `year` or `date`.")
    df["year"] = y
    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)
    return df


def _noise01(df: pd.DataFrame) -> pd.Series:
    """
    semantic_noise_01 in [0,1]
    - if semantic_noise_score exists: score/10
    - if semantic_noise flag exists: {0,1}
    - if both: flag * (score/10)
    """
    flag_col = "semantic_noise"
    score_col = "semantic_noise_score"

    if flag_col in df.columns:
        flag = (
            df[flag_col].astype(str).fillna("").str.strip()
            .isin(["1", "True", "true", "TRUE", "yes", "YES", "y", "Y"])
            .astype(float)
        )
    else:
        flag = pd.Series(0.0, index=df.index)

    if score_col in df.columns:
        score01 = (_to_float(df[score_col], 0.0) / 10.0).clip(0.0, 1.0)
    else:
        score01 = pd.Series(0.0, index=df.index)

    if (flag_col in df.columns) and (score_col in df.columns):
        out = flag * score01
    elif score_col in df.columns:
        out = score01
    elif flag_col in df.columns:
        out = flag
    else:
        out = pd.Series(0.0, index=df.index)

    return out.fillna(0.0).astype(float)


def _O01(df: pd.DataFrame) -> pd.Series:
    """
    O = archivalization / objectification strength (0..1)
    base: archivalization_score_v2 else archivalization_score, /10
    bonus: geo evidence/scope slightly increases O (more grounded)
    """
    if "archivalization_score_v2" in df.columns:
        base = _to_float(df["archivalization_score_v2"], 0.0)
    elif "archivalization_score" in df.columns:
        base = _to_float(df["archivalization_score"], 0.0)
    else:
        base = pd.Series(0.0, index=df.index)

    base01 = (base / 10.0).clip(0.0, 1.0)

    geo_ev = _scol(df, "geo_evidence")
    geo_sc = _scol(df, "geo_scope")

    bonus_ev = geo_ev.map({"A_exact": 0.06, "B_infer": 0.03}).fillna(0.0).astype(float)
    bonus_sc = ((geo_sc != "") & (geo_sc != "unknown")).astype(float) * 0.03

    return (base01 + bonus_ev + bonus_sc).clip(0.0, 1.0).astype(float)


def _R01(df: pd.DataFrame) -> pd.Series:
    """
    R = rupture / polemic intensity (0..1)
    polemic_score/10 + 0.45*semantic_noise_01
    """
    if "polemic_score" in df.columns:
        pole01 = (_to_float(df["polemic_score"], 0.0) / 10.0).clip(0.0, 1.0)
    else:
        pole01 = pd.Series(0.0, index=df.index)

    noise01 = _noise01(df)
    return (pole01 + 0.45 * noise01).clip(0.0, 1.0).astype(float)


def _S01(df: pd.DataFrame) -> pd.Series:
    """
    S = structural opacity / missingness (0..1)
    higher -> harder to ground (missing city/theme/org/actors or unknown geo)
    """
    city = _scol(df, "city")
    thcl = _scol(df, "themes_cluster")
    actors = _scol(df, "actors")
    orgs = _scol(df, "organizations")
    geo_sc = _scol(df, "geo_scope")
    geo_ev = _scol(df, "geo_evidence")

    miss_city = (city == "").astype(float)
    miss_thcl = (thcl == "").astype(float)
    miss_orgs = (orgs == "").astype(float)
    miss_actors = (actors == "").astype(float)
    unk_sc = ((geo_sc == "") | (geo_sc == "unknown")).astype(float)
    unk_ev = ((geo_ev == "") | (geo_ev == "C_unknown")).astype(float)

    s = (
        0.22 * miss_city
        + 0.18 * miss_thcl
        + 0.14 * miss_orgs
        + 0.10 * miss_actors
        + 0.18 * unk_sc
        + 0.18 * unk_ev
    ).clip(0.0, 1.0)

    return s.astype(float)


def main():
    ROOT = _root()
    DATA = ROOT / "data_raw" / "events_raw_4d_working.csv"
    OUTDIR = ROOT / "outputs"
    OUTDIR.mkdir(parents=True, exist_ok=True)

    if not DATA.exists():
        raise FileNotFoundError(
            f"Missing input: {DATA}\n"
            f"Tip: confirm you have data_raw/events_raw_4d_working.csv in the project root:\n"
            f"  {ROOT}"
        )

    df = pd.read_csv(DATA).fillna("")
    df = _ensure_year(df)

    df["semantic_noise_01"] = _noise01(df)
    df["O"] = _O01(df)
    df["R"] = _R01(df)
    df["S"] = _S01(df)

    grp = df.groupby("year", as_index=False).agg(
        n=("year", "size"),
        O_mean=("O", "mean"),
        R_mean=("R", "mean"),
        S_mean=("S", "mean"),
        noise_mean=("semantic_noise_01", "mean"),
    )

    denom = (grp["O_mean"] + grp["R_mean"] + grp["S_mean"]).replace(0, np.nan)
    grp["O_share"] = (grp["O_mean"] / denom).fillna(0.0)
    grp["R_share"] = (grp["R_mean"] / denom).fillna(0.0)
    grp["S_share"] = (grp["S_mean"] / denom).fillna(0.0)

    out_table = OUTDIR / "table_rupture_components_yearly.csv"
    grp.to_csv(out_table, index=False, encoding="utf-8-sig")

    years = sorted(grp["year"].unique().tolist())

    # means plot
    fig1 = plt.figure(figsize=(10, 4.8))
    ax1 = plt.gca()
    ax1.plot(grp["year"], grp["O_mean"], marker="o", label="O_mean")
    ax1.plot(grp["year"], grp["R_mean"], marker="o", label="R_mean")
    ax1.plot(grp["year"], grp["S_mean"], marker="o", label="S_mean")
    ax1.plot(grp["year"], grp["noise_mean"], marker="o", linestyle="--", label="noise_mean")
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Mean (0..1)")
    ax1.set_title("Rupture components (means) by year")
    ax1.legend(loc="best")
    ax1.set_xticks(years)
    ax1.tick_params(axis="x", labelrotation=35)
    plt.tight_layout()
    out_fig1 = OUTDIR / "fig_rupture_components_means.png"
    plt.savefig(out_fig1, dpi=200)
    plt.close(fig1)

    # shares plot
    fig2 = plt.figure(figsize=(10, 4.8))
    ax2 = plt.gca()
    ax2.plot(grp["year"], grp["O_share"], marker="o", label="O_share")
    ax2.plot(grp["year"], grp["R_share"], marker="o", label="R_share")
    ax2.plot(grp["year"], grp["S_share"], marker="o", label="S_share")
    ax2.set_xlabel("Year")
    ax2.set_ylabel("Share within (O+R+S)")
    ax2.set_title("Rupture components (shares) by year")
    ax2.legend(loc="best")
    ax2.set_xticks(years)
    ax2.tick_params(axis="x", labelrotation=35)
    plt.tight_layout()
    out_fig2 = OUTDIR / "fig_rupture_components_shares.png"
    plt.savefig(out_fig2, dpi=200)
    plt.close(fig2)

    print("Done.")
    print(" -", out_table)
    print(" -", out_fig1)
    print(" -", out_fig2)


if __name__ == "__main__":
    main()
