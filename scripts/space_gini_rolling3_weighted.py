import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

IN_PATH = "data_raw/events_raw_4d_working.csv"  # 你现在 pipeline 的工作文件
OUT_DIR = Path("outputs")
OUT_DIR.mkdir(exist_ok=True)

# 输出
OUT_CSV = OUT_DIR / "table_4d_space_city_gini_rolling3_weighted.csv"
OUT_FIG = OUT_DIR / "fig_4d_space_city_gini_3yr_weighted.png"

# 参数
WINDOW = 3
MIN_N = 3
USE_EVIDENCE = {"A_exact", "B_infer"}  # 加权版：A_exact + B_infer 都参与
W_BY_EVIDENCE = {"A_exact": 1.0, "B_infer": 0.6, "C_unknown": 0.0}


def gini_from_weights(w: np.ndarray) -> float:
    """
    加权Gini（城市集中度）。
    w 是每个城市的“权重事件数”（>=0）。
    返回 0..1。
    """
    w = np.asarray(w, dtype=float)
    w = w[w >= 0]
    if w.size == 0:
        return np.nan
    if np.allclose(w.sum(), 0.0):
        return np.nan
    w_sorted = np.sort(w)
    n = w_sorted.size
    cumw = np.cumsum(w_sorted)
    # Gini = 1 - 2 * sum_i (cumw_i / (n * total)) + 1/n
    total = w_sorted.sum()
    g = 1 - 2 * np.sum(cumw) / (n * total) + 1 / n
    # 数值边界
    return float(max(0.0, min(1.0, g)))


def year_int(x):
    s = str(x)[:4]
    if s.isdigit():
        return int(s)
    return None


def main():
    df = pd.read_csv(IN_PATH).fillna("")
    if "year" in df.columns:
        df["year"] = df["year"].apply(year_int)
    else:
        df["year"] = df["date"].apply(year_int)

    # 必要列检查
    need_cols = ["year", "city", "geo_evidence"]
    for c in need_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}. You have columns: {df.columns.tolist()}")

    # 过滤证据等级
    df["geo_evidence"] = df["geo_evidence"].astype(str).str.strip()
    df = df[df["geo_evidence"].isin(USE_EVIDENCE)].copy()

    # 权重
    df["w_geo"] = df["geo_evidence"].map(W_BY_EVIDENCE).fillna(0.0).astype(float)

    # 去掉无城市
    df["city"] = df["city"].astype(str).str.strip()
    df = df[df["city"] != ""].copy()

    # 年范围
    years = sorted([y for y in df["year"].dropna().unique().tolist() if isinstance(y, (int, np.integer))])
    if not years:
        raise RuntimeError("No valid years found after filtering. Check year parsing and filters.")

    min_year, max_year = min(years), max(years)

    rows = []
    x_centers = []
    gini_eff = []  # 有效(>=2城市)的gini
    x_deg = []     # 退化(==1城市)点
    n_events = []

    # rolling window center years
    for center in range(min_year, max_year + 1):
        start = center - (WINDOW // 2)
        end = center + (WINDOW // 2)

        wdf = df[(df["year"] >= start) & (df["year"] <= end)].copy()
        n = len(wdf)
        if n < MIN_N:
            continue

        # 每个城市的加权事件数
        city_w = wdf.groupby("city")["w_geo"].sum().sort_values(ascending=False)
        n_cities = int(city_w.shape[0])

        # gini：如果只有一个城市，raw gini=0（但作为“退化”标记）
        if n_cities >= 2:
            g = gini_from_weights(city_w.values)
            gini_eff.append(g)
            x_centers.append(center)
        elif n_cities == 1:
            g = 0.0
            x_deg.append(center)
            # 也把它记到主序列里（方便输出表格），但图上用叉号表示
            gini_eff.append(g)
            x_centers.append(center)
        else:
            continue

        n_events.append(n)

        rows.append(
            {
                "center_year": center,
                "window_start": start,
                "window_end": end,
                "n_events": n,
                "n_cities": n_cities,
                "top_city": city_w.index[0] if n_cities >= 1 else "",
                "top_city_weight": float(city_w.iloc[0]) if n_cities >= 1 else 0.0,
                "gini_weighted": float(g),
            }
        )

    out = pd.DataFrame(rows).sort_values("center_year").reset_index(drop=True)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("Wrote:", OUT_CSV)

    # --- PLOT ---
    fig, ax = plt.subplots(figsize=(10, 6))

    # 主线：gini
    ax.plot(x_centers, gini_eff, marker="o", label=f"gini_weighted (A_exact+B_infer, min_n={MIN_N})")

    # 退化点（只有1个城市）：叉号放在0处
    if len(x_deg) > 0:
        ax.scatter(x_deg, [0] * len(x_deg), marker="x", label="degenerate_one_city (raw gini=0)")

    ax.set_title(f"Space: City concentration (Gini) - 3yr rolling (A_exact + B_infer, min_n={MIN_N})")
    ax.set_xlabel("Center year")
    ax.set_ylabel("Gini (city concentration)")

    # 右轴：窗口内事件数
    ax2 = ax.twinx()
    ax2.plot(x_centers, n_events, linestyle="--", marker="o", label="n_events (3yr window)")
    ax2.set_ylabel("n_events in 3-year window")

    # 合并 legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper center")

    # x轴稀疏刻度（最多10个）
    all_years = sorted(set(int(v) for v in x_centers))
    min_y, max_y = min(all_years), max(all_years)
    max_ticks = 10
    span = max_y - min_y
    if span <= max_ticks:
        step = 1
    else:
        step = max(1, int(round(span / (max_ticks - 1))))
    start_tick = (min_y // step) * step
    xticks = list(range(start_tick, max_y + 1, step))
    ax.set_xticks(xticks)
    ax.tick_params(axis="x", labelrotation=35)

    plt.tight_layout()
    plt.savefig(OUT_FIG, dpi=180)
    print("Wrote:", OUT_FIG)


if __name__ == "__main__":
    main()