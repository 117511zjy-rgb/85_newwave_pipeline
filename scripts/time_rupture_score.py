import re
import pandas as pd
from pathlib import Path

IN_PATH = "data_raw/events_raw_4d_working.csv"
OUT_PATH = "data_raw/events_raw_4d_working.csv"   # 覆盖写回：追加字段
OUT_YEARLY = Path("outputs") / "table_time_rupture_yearly.csv"
Path("outputs").mkdir(exist_ok=True)

# 你之前用的断点（可随时改）
BREAKPOINTS = [1978, 1985, 1989, 2000, 2010, 2020]
BP_WINDOW = 1  # ±1年

# 关键词：争议/扭转/再叙述/“档案化”
KW_POLEMIC = [
    "争议","风波","批评","批判","质疑","论战","冲突","禁","封","审查","整肃","清理","整顿","政治",
    "转向","扭转","再叙述","重写","重新","回望","回放","回顾","复盘","再评价","反思","修正",
]
KW_ARCHIVE = [
    "档案","文献","资料","年表","图录","数据库","回顾展","文献展","档案展","研究","整理","收录","编纂",
    "博物馆","美术馆","艺术馆","馆藏","收藏","捐赠","策展","策划","展史",
]

# tech channel：如果你的字段名不同也没关系，脚本会自动兼容
TECH_HINT = {
    "database": 0.20,
    "retrospective_web": 0.12,
    "social_media": 0.10,
    "unknown": 0.00,
}

# institution shift：把“unknown->明确”当作一种结构化信号（档案化/机构化）
INST_HINT = {
    "museum": 0.15,
    "platform": 0.12,
    "media": 0.10,
    "market": 0.08,
    "unknown": 0.00,
}

def year_int(x):
    s = str(x).strip()
    if not s:
        return None
    m = re.match(r"^(\d{4})", s)
    return int(m.group(1)) if m else None

def near_breakpoint(y: int | None) -> int:
    if y is None:
        return 0
    return int(any(abs(y - bp) <= BP_WINDOW for bp in BREAKPOINTS))

def text_hit(text: str, keywords: list[str]) -> int:
    t = str(text)
    return sum(1 for w in keywords if w and (w in t))

def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def main():
    df = pd.read_csv(IN_PATH).fillna("")

    # 兼容字段名
    col_year = pick_col(df, ["year", "Year", "y"])
    col_date = pick_col(df, ["date", "Date"])
    col_title = pick_col(df, ["title", "Title"])
    col_themes = pick_col(df, ["themes", "themes_cluster", "Themes"])
    col_source = pick_col(df, ["source", "source_domain_real", "Source"])
    col_noise = pick_col(df, ["semantic_noise_score", "semantic_noise", "noise_score"])
    col_inst = pick_col(df, ["institution_level", "inst_level"])
    col_tech = pick_col(df, ["tech_channel", "diffusion_channel", "channel"])

    # year
    if col_year is None:
        # 用 date 推 year
        df["year"] = df[col_date].apply(year_int) if col_date else ""
        col_year = "year"

    def compute_row(r):
        y = year_int(r.get(col_year, ""))

        title = r.get(col_title, "")
        themes = r.get(col_themes, "")
        src = r.get(col_source, "")

        text = f"{title} {themes} {src}"

        # (1) breakpoint 信号
        s_bp = 0.35 if near_breakpoint(y) else 0.0

        # (2) polemic/archival 词汇信号
        pole = text_hit(text, KW_POLEMIC)   # 计数
        arch = text_hit(text, KW_ARCHIVE)

        # 映射到 0~0.25 / 0~0.20（饱和）
        s_pole = min(pole, 4) / 4 * 0.25
        s_arch = min(arch, 5) / 5 * 0.20

        # (3) 语义噪点：如果你已有 semantic_noise_score（通常>0），做归一映射
        s_noise = 0.0
        if col_noise:
            try:
                nv = float(str(r.get(col_noise, "")).strip() or 0.0)
                # 经验映射：0~10 -> 0~0.20
                s_noise = clamp01(nv / 10.0) * 0.20
            except Exception:
                s_noise = 0.0

        # (4) tech / institution 结构化信号
        s_tech = 0.0
        if col_tech:
            tv = str(r.get(col_tech, "unknown")).strip() or "unknown"
            s_tech = TECH_HINT.get(tv, 0.0)

        s_inst = 0.0
        if col_inst:
            iv = str(r.get(col_inst, "unknown")).strip() or "unknown"
            s_inst = INST_HINT.get(iv, 0.0)

        # 合成：上限 1.0
        score = clamp01(s_bp + s_pole + s_arch + s_noise + s_tech + s_inst)

        # 写清楚理由（论文里很好用）
        why = []
        if s_bp > 0: why.append("near_breakpoint")
        if pole > 0: why.append(f"polemic_kw={pole}")
        if arch > 0: why.append(f"archival_kw={arch}")
        if s_noise > 0: why.append("semantic_noise")
        if s_tech > 0: why.append(f"tech={str(r.get(col_tech,''))}")
        if s_inst > 0: why.append(f"inst={str(r.get(col_inst,''))}")
        why = ";".join(why) if why else "none"

        return pd.Series({
            "rupture_score": round(score, 4),
            "rupture_bp": round(s_bp, 4),
            "rupture_polemic": round(s_pole, 4),
            "rupture_archival": round(s_arch, 4),
            "rupture_semantic_noise": round(s_noise, 4),
            "rupture_tech": round(s_tech, 4),
            "rupture_institution": round(s_inst, 4),
            "rupture_why": why,
        })

    add = df.apply(compute_row, axis=1)
    for c in add.columns:
        df[c] = add[c]

    # 写回 working 表
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    # 年度汇总
    ycol = "year"
    d2 = df.copy()
    d2[ycol] = d2[ycol].apply(year_int)
    d2 = d2.dropna(subset=[ycol])
    yearly = (
        d2.groupby(ycol)
        .agg(
            n_events=("rupture_score","size"),
            rupture_mean=("rupture_score","mean"),
            rupture_p90=("rupture_score", lambda s: float(pd.Series(s).quantile(0.9))),
            polemic_mean=("rupture_polemic","mean"),
            archival_mean=("rupture_archival","mean"),
            noise_mean=("rupture_semantic_noise","mean"),
        )
        .reset_index()
        .sort_values(ycol)
    )
    yearly.to_csv(OUT_YEARLY, index=False, encoding="utf-8-sig")

    print("Done:")
    print(" - updated:", OUT_PATH)
    print(" - yearly:", OUT_YEARLY)
    print("rupture_score unique:", df["rupture_score"].nunique())
    print(yearly.tail(10).to_string(index=False))

if __name__ == "__main__":
    main()
