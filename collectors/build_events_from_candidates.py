import os
import re
import hashlib
import pandas as pd
from rapidfuzz import fuzz

CAND_PATH = "data_raw/candidates/candidates_seed.csv"
OUT_PATH = "data_raw/events_raw.csv"

CITY_WORDS = ["北京","上海","广州","深圳","杭州","南京","武汉","西安","成都","重庆","天津","长沙","厦门","香港","台北"]
THEME_WORDS = ["前卫","观念","实验","装置","行为","政治","现代性","媒介","主体","后现代","制度","话语","新潮","抽象","影像"]

EVENT_TYPE_RULES = [
    (r"(展|展览|个展|群展|回顾展|开幕|exhibition)", "exhibition"),
    (r"(论坛|研讨|讲座|评论|文献|对谈|访谈|批评)", "discourse"),
    (r"(发布|专题|报道|媒体|新闻|press|news)", "media"),
]

ORG_HINTS = ["美术馆","博物馆","画廊","艺术中心","学院","大学","基金会","艺术馆","UCCA","MoMA","Tate","Guggenheim","Met"]

def norm(s):
    return re.sub(r"\s+", " ", str(s) if not pd.isna(s) else "").strip()

def normalize_date(s):
    s = norm(s)
    if not s:
        return ""
    s = s.replace("年","-").replace("月","-").replace("日","")
    s = re.sub(r"[./]", "-", s)
    s = re.sub(r"\s+", "", s)

    m = re.match(r"^((19|20)\d{2})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(3)):02d}-{int(m.group(4)):02d}"

    m = re.match(r"^((19|20)\d{2})-(\d{1,2})$", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(3)):02d}-01"

    m = re.match(r"^((19|20)\d{2})$", s)
    if m:
        return f"{int(m.group(1)):04d}-01-01"

    m = re.search(r"((19|20)\d{2})[-/.](\d{1,2})([-/.](\d{1,2}))?", s)
    if m:
        y = int(m.group(1)); mo = int(m.group(3)); d = int(m.group(5)) if m.group(5) else 1
        return f"{y:04d}-{mo:02d}-{d:02d}"

    y = re.search(r"(19|20)\d{2}", s)
    if y:
        return f"{y.group(0)}-01-01"
    return ""

def detect_type(text, default_="other"):
    t = norm(text).lower()
    for pat, label in EVENT_TYPE_RULES:
        if re.search(pat, t, re.I):
            return label
    return norm(default_).lower() if norm(default_) else "other"

def extract_city(text):
    t = norm(text)
    for c in CITY_WORDS:
        if c in t:
            return c
    return ""

def extract_orgs(text):
    t = norm(text)
    found = [k for k in ORG_HINTS if k in t]
    return ";".join(sorted(set(found)))

def extract_themes(text):
    t = norm(text)
    found = [k for k in THEME_WORDS if k in t]
    return ";".join(found)

def near_dup(a, b):
    return fuzz.token_set_ratio(norm(a), norm(b)) >= 93

def make_event_id(i, date, etype, title):
    year = date[:4] if date else "0000"
    key = hashlib.md5((norm(title)+"|"+date+"|"+etype).encode("utf-8")).hexdigest()[:8]
    return f"EVT-{etype[:3].upper()}-{year}-{i:05d}-{key}"

def dedup(df):
    if "source" in df.columns:
        df = df.drop_duplicates(subset=["source","title"], keep="first").copy()
    else:
        df = df.drop_duplicates(subset=["title"], keep="first").copy()

    kept = []
    for _, r in df.iterrows():
        dup = False
        for k in kept[-2000:]:
            if str(r["date"]) == str(k["date"]) and near_dup(r["title"], k["title"]):
                dup = True
                break
        if not dup:
            kept.append(r.to_dict())
    return pd.DataFrame(kept)

def main():
    if not os.path.exists(CAND_PATH):
        raise FileNotFoundError(CAND_PATH)

    raw = pd.read_csv(CAND_PATH).fillna("")
    for c in ["title","date","date_raw","event_type_guess","snippet","source_url","source_page"]:
        if c not in raw.columns:
            raw[c] = ""

    rows = []
    for _, r in raw.iterrows():
        title = norm(r["title"])[:180]
        snippet = norm(r["snippet"])
        if len(title) < 4:
            continue

        date = normalize_date(r["date"]) if norm(r["date"]) else normalize_date(r["date_raw"])
        text = f"{title} {snippet}"

        etype = detect_type(text, r["event_type_guess"])
        city = extract_city(text)
        orgs = extract_orgs(text)
        themes = extract_themes(text)
        source = norm(r["source_url"]) or norm(r["source_page"])

        # 宽进：无日期也保留，先给占位日期，降低置信度
        if not date:
            date = "1985-01-01"  # 针对你当前八五新潮主题，先用占位年保留记录

        conf = 0.45
        if source: conf += 0.10
        if etype != "other": conf += 0.10
        if orgs: conf += 0.10
        if themes: conf += 0.10
        # 日期是占位时不加分，真实日期再加
        if date != "1985-01-01": conf += 0.15
        conf = round(min(conf, 0.95), 2)

        rows.append({
            "title": title,
            "date": date,
            "city": city,
            "event_type": etype,
            "actors": "",   # 后续可接NER
            "organizations": orgs,
            "themes": themes,
            "source": source,
            "confidence": conf
        })

    d = pd.DataFrame(rows)
    d = d[d["source"].astype(str).str.len() > 0].copy()
    d = dedup(d).reset_index(drop=True)

    d.insert(0, "event_id", [make_event_id(i+1, d.loc[i,"date"], d.loc[i,"event_type"], d.loc[i,"title"]) for i in range(len(d))])
    d.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print(f"Saved {len(d)} events -> {OUT_PATH}")
    print("event_type counts:")
    print(d["event_type"].value_counts(dropna=False).to_string())
    print("missing ratios:")
    print("actors:", (d["actors"].fillna("").str.strip()=="").mean())
    print("organizations:", (d["organizations"].fillna("").str.strip()=="").mean())
    print("themes:", (d["themes"].fillna("").str.strip()=="").mean())

if __name__ == "__main__":
    main()