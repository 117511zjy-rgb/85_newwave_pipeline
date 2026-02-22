import re
import pandas as pd
from pathlib import Path

IN_PATH = "data_raw/events_raw_4d_working.csv"
OUT_PATH = "data_raw/events_raw_4d_working.csv"
OUT_DIR = Path("outputs")
OUT_DIR.mkdir(exist_ok=True)

BREAKPOINTS = [1978, 1985, 1989, 2000, 2010, 2020]

KW_CONCEPT = [
    "前卫","实验","现代性","启蒙","主体","人民","国家","意识形态","体制","制度",
    "市场","全球化","当代","新潮","先锋","批评","争论","危机","断裂","转向",
    "叙事","再叙述","回顾","年表","文献","档案","历史","组织","结社","社群","网络"
]
KW_GENRE = [
    "访谈","对谈","评论","研讨","论文","专题","讲座","座谈","论坛","纪要","年表","数据库","图录","文献"
]
SRC_HINT = [
    "artlinkart","数据库","年表","专题","访谈","评论","研讨","journal","paper","thesis","forum","lecture"
]

def year_int(s):
    s = str(s)[:4]
    return int(s) if re.match(r"^\d{4}$", s) else None

def near_breakpoint(y, window=1):
    if y is None:
        return 0
    return int(any(abs(y-bp) <= window for bp in BREAKPOINTS))

def score_row(title, themes, source, y):
    text = " ".join([str(title), str(themes)])
    src = str(source)
    s = 0.0

    for w in KW_CONCEPT:
        if w and w in text:
            s += 1.0

    for w in KW_GENRE:
        if w and (w in text or w in src):
            s += 1.2

    for w in SRC_HINT:
        if w and w.lower() in src.lower():
            s += 0.8

    s += 2.0 * near_breakpoint(y, window=1)
    return s

def main():
    df = pd.read_csv(IN_PATH).fillna("")
    if "semantic_noise" not in df.columns:
        df["semantic_noise"] = 0
    if "notes" not in df.columns:
        df["notes"] = ""

    scores = []
    for _, r in df.iterrows():
        y = year_int(r.get("year",""))
        s = score_row(r.get("title",""), r.get("themes",""), r.get("source",""), y)
        scores.append(s)
    df["semantic_noise_score"] = scores

    topn = 10 if len(df) >= 10 else len(df)
    thresh = df["semantic_noise_score"].nlargest(topn).min() if topn > 0 else 999
    df["semantic_noise"] = (df["semantic_noise_score"] >= thresh).astype(int)

    df.loc[df["semantic_noise"]==1, "notes"] = df.loc[df["semantic_noise"]==1, "notes"].astype(str) + \
        " | semantic_noise=1 by rule(top10 score);"

    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    audit = df.sort_values("semantic_noise_score", ascending=False).head(30)
    audit[["event_id","year","title","event_type","source","semantic_noise_score","semantic_noise"]].to_csv(
        OUT_DIR / "table_semantic_noise_audit_top30.csv", index=False, encoding="utf-8-sig"
    )

    print("Done. Marked semantic_noise by rule. Wrote outputs/table_semantic_noise_audit_top30.csv")
    print("Top10 threshold:", thresh)

if __name__ == "__main__":
    main()
