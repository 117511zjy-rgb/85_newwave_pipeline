# scripts/space_gini_rolling3.py
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

IN_PATH = "data_raw/events_raw_4d_working.csv"
OUTDIR = Path("outputs")
OUTDIR.mkdir(exist_ok=True)

OUT_TABLE = OUTDIR / "table_4d_space_city_gini_rolling3_A_exact.csv"
OUT_FIG = OUTDIR / "fig_4d_space_city_gini_rolling3_A_exact.png"

MIN_N_EVENTS = 3  # 阈值=3

def gini_from_counts(counts):
    x = np.array(counts, dtype=float)
    if len(x) == 0:
        return np.nan
    if np.all(x == 0):
        return np.nan
    n = len(x)
    mean = x.mean()
    if mean == 0:
        return np.nan
    diff_sum = np.abs(x.reshape(-1, 1) - x.reshape(1, -1)).sum()
    return float(diff_sum / (2 * (n ** 2) * mean))

def main():
    df = pd.read_csv(IN_PATH).fillna("")

    # year
    if "year" not in df.columns:
        if "date" in df.columns:
            df["year"] = df["date"].astype(str).str[:4]
        else:
            raise ValueError("No year/date column found.")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    if "city" not in df.columns:
        df["city"] = ""
    if "geo_evidence" not in df.columns:
        df["geo_evidence"] = "C_unknown"

    # only A_exact
    d = df[df["geo_evidence"].astype(str).str.strip() == "A_exact"].copy()
    d = d[d["year"].notna()].copy()
    d["year"] = d["year"].astype(int)
    d["city"] = d["city"].astype(str).str.strip()

    years = sorted(d["year"].unique().tolist())
    if not years:
        print("No A_exact rows found.")
        return

    rows = []
    for y in years:
        w = d[(d["year"] >= y - 1) & (d["year"] <= y + 1)].copy()
        n_events = len(w)

        w_city = w[w["city"].astype(str).str.strip() != ""].copy()
        city_counts = w_city["city"].value_counts().to_dict()
        n_cities = len(city_counts)
        counts = list(city_counts.values())

        g = gini_from_counts(counts) if n_cities >= 1 else np.nan
        degenerate_one_city = int(n_cities == 1)

        effective_gini = np.nan if degenerate_one_city == 1 else g

        rows.append({
            "center_year": y,
            "window": f"{y-1},{y},{y+1}",
            "n_events": n_events,
            "n_events_with_city": int(len(w_city)),
            "n_cities": n_cities,
            "gini_city": g,
            "degenerate_one_city": degenerate_one_city,
            "effective_gini": effective_gini,
            "top_city": (max(city_counts, key=city_counts.get) if n_cities > 0 else ""),
            "top_city_share": (round(max(counts)/sum(counts), 4) if n_cities > 0 else np.nan),
        })

    out = pd.DataFrame(rows)
    out_f = out[out["n_events"] >= MIN_N_EVENTS].copy().reset_index(drop=True)

    out_f.to_csv(OUT_TABLE, index=False, encoding="utf-8-sig")
    print("Wrote:", OUT_TABLE)
    print("Rows:", len(out_f))

    # --- Plot with right axis for n_events ---
    x = out_f["center_year"].astype(int).to_numpy()
    n_events = out_f["n_events"].astype(int).to_numpy()
    eff = out_f["effective_gini"].to_numpy()
    raw = out_f["gini_city"].to_numpy()
    deg = out_f["degenerate_one_city"].astype(int).to_numpy()

    fig, ax1 = plt.subplots()

    # non-degenerate line
    mask_nd = (deg == 0) & (~np.isnan(eff))
    ax1.plot(x[mask_nd], eff[mask_nd], marker="o", linestyle="-", label="effective_gini (>=2 cities)")

    # degenerate markers (one city)
    mask_d = (deg == 1) & (~np.isnan(raw))
    if mask_d.any():
        ax1.scatter(x[mask_d], raw[mask_d], marker="x", label="degenerate_one_city (raw gini=0)")

    ax1.set_title(f"Space: City concentration (Gini) - 3yr rolling (A_exact, min_n={MIN_N_EVENTS})")
    ax1.set_xlabel("Center year")
    ax1.set_ylabel("Gini (city concentration)")
    ax1.tick_params(axis="x", rotation=45)

    # right axis: n_events
    ax2 = ax1.twinx()
    ax2.plot(x, n_events, linestyle="--", marker=".", label="n_events (window)", alpha=0.8)
    ax2.set_ylabel("n_events in 3-year window")

    # combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="best")

    fig.tight_layout()
    fig.savefig(OUT_FIG, dpi=200)
    plt.close(fig)
    print("Wrote:", OUT_FIG)

if __name__ == "__main__":
    main()