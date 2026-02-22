import re
import urllib.parse as up
from pathlib import Path

import pandas as pd

IN_PATH = "data_raw/events_raw_4d_working.csv"
OUT_PATH = "data_raw/events_raw_4d_working.csv"
OUT_AUDIT = Path("outputs") / "table_archivalization_v2_audit.csv"
OUT_AUDIT_TOP = Path("outputs") / "table_archivalization_v2_top30.csv"
Path("outputs").mkdir(exist_ok=True)

def decode_duckduckgo(u: str) -> str:
    u = str(u or "")
    if "duckduckgo.com/l/?" not in u:
        return u
    try:
        qs = up.parse_qs(up.urlparse(u).query)
        real = qs.get("uddg", [u])[0]
        return up.unquote(real)
    except Exception:
        return u

def real_domain(u: str) -> str:
    u = decode_duckduckgo(u)
    try:
        return up.urlparse(u).netloc.lower()
    except Exception:
        return ""

def path_str(u: str) -> str:
    u = decode_duckduckgo(u)
    try:
        return (up.urlparse(u).path or "").lower()
    except Exception:
        return ""

def text_blob(title, source, event_type):
    return f"{title} {source} {event_type}".lower()

DOMAIN_STRONG = {
    "aaa.org.hk": 4.0,
    "www.artda.cn": 4.0,
    "www.artlinkart.com": 2.0,
}
DOMAIN_MEDIUM = {
    "www.cafamuseum.org": 2.5,
    "www.cafa.com.cn": 2.0,
    "www.redbrickartmuseum.org": 2.0,
    "www.artforum.com.cn": 1.5,
    "news.ifeng.com": 1.0,
    "www.sohu.com": 0.5,
}
KW_STRONG = {
    "档案": 3.0, "文献": 3.0, "年表": 3.0, "数据库": 3.0, "archive": 3.0,
    "行为档案": 3.5, "人物档案": 3.5,
    "文献展": 3.0, "文献展览": 3.0,
    "回放": 2.0, "合集": 1.5, "回顾": 1.5,
    "研究": 2.5, "节选": 2.0,
    "策展学": 2.0, "建立": 1.0,
}
PATH_KW = {
    "archives": 2.5,
    "archive": 2.5,
    "collections": 2.0,
    "collection": 2.0,
    "workhistory": 2.0,
    "about": 1.0,
    "overview": 0.8,
    "print": 1.5,
    ".pdf": 2.0,
}
TYPE_BONUS = {"other": 0.6, "exhibition": 0.2}

def archivalization_v2(title, source, event_type):
    dom = real_domain(source)
    pth = path_str(source)
    t = text_blob(title, source, event_type)

    score = 0.0
    score += DOMAIN_STRONG.get(dom, 0.0)
    score += DOMAIN_MEDIUM.get(dom, 0.0)
    score += TYPE_BONUS.get(str(event_type).lower(), 0.0)

    for k, w in KW_STRONG.items():
        if k.lower() in t:
            score += w

    for k, w in PATH_KW.items():
        if k in pth:
            score += w

    score = min(score, 10.0)
    return round(score, 2), dom, pth

def main():
    df = pd.read_csv(IN_PATH).fillna("")
    scores, doms, paths = [], [], []
    for _, r in df.iterrows():
        s, d, p = archivalization_v2(r.get("title",""), r.get("source",""), r.get("event_type",""))
        scores.append(s); doms.append(d); paths.append(p)

    df["archivalization_score_v2"] = scores
    df["source_domain_real"] = doms
    df["source_path_real"] = paths

    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    audit = (df.groupby("source_domain_real")["archivalization_score_v2"]
               .agg(n="count", mean="mean", p90=lambda x: x.quantile(0.9))
               .reset_index()
               .sort_values(["mean","n"], ascending=False))
    audit.to_csv(OUT_AUDIT, index=False, encoding="utf-8-sig")

    top = (df.sort_values("archivalization_score_v2", ascending=False)
             .loc[:, ["event_id","year","title","event_type","source_domain_real","archivalization_score_v2","source"]]
             .head(30))
    top.to_csv(OUT_AUDIT_TOP, index=False, encoding="utf-8-sig")

    print("Done. Wrote:")
    print(" -", OUT_PATH)
    print(" -", OUT_AUDIT)
    print(" -", OUT_AUDIT_TOP)
    print("Top v2 scores:")
    print(top[["event_id","archivalization_score_v2"]].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
