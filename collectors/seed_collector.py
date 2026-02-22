import os
import re
import time
import hashlib
from datetime import datetime, timezone
from urllib.parse import quote, urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

OUT_DIR = "data_raw/candidates"
HTML_DIR = "data_raw/html"
LOG_DIR = "logs"
OUT_PATH = os.path.join(OUT_DIR, "candidates_seed.csv")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 你可以继续加站点
SEED_SITES = [
    "https://ucca.org.cn/exhibitions/",
]

# 分页模板（{page} 会被替换）
PAGINATED_SITES = [
    {"template": "https://ucca.org.cn/exhibitions/page/{page}/", "start": 1, "end": 20},
]

# 关键词检索后缀
QUERY_SUFFIX = " 艺术 展览"

def log_error(tag: str, target: str, err: Exception):
    with open(os.path.join(LOG_DIR, "crawl_errors.log"), "a", encoding="utf-8") as f:
        f.write(f"[{tag}] {target}\t{repr(err)}\n")

def fetch(url: str, timeout: int = 20) -> str:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def save_html(url: str, html: str) -> str:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    path = os.path.join(HTML_DIR, f"{h}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path

def normalize_date(date_raw: str) -> str:
    if not date_raw:
        return ""
    s = str(date_raw).replace("年", "-").replace("月", "-").replace("日", "")
    s = re.sub(r"[./]", "-", s)
    try:
        dt = dtparser.parse(s, fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def detect_event_type(text: str) -> str:
    t = "other"
    if re.search(r"(展|展览|exhibition|开幕|回顾展)", text, re.I):
        t = "exhibition"
    elif re.search(r"(论坛|研讨|讲座|评论|文献|话语)", text, re.I):
        t = "discourse"
    elif re.search(r"(媒体|采访|发布|专题)", text, re.I):
        t = "media"
    return t

def extract_candidates_from_html(page_url: str, html: str):
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # 通用候选块选择器（可继续加）
    items = soup.select(
        "article, .post, .news-item, li, .item, .entry, "
        ".exhibition-item, .card, .result, .search-result, .archive-item"
    )

    if not items:
        items = [soup]

    for it in items:
        text = " ".join(it.stripped_strings)
        if len(text) < 30:
            continue

        # 日期抽取：2023-11-05 / 2023.11.05 / 2023年11月5日 / 2023-11
        m = re.search(r"((19|20)\d{2}[-/.年]\d{1,2}([-/.\s月]\d{1,2})?)", text)
        date_raw = m.group(1) if m else ""

        # 标题优先
        title_tag = it.find(["h1", "h2", "h3", "a", "strong"])
        title = title_tag.get_text(strip=True) if title_tag else text[:80]
        title = re.sub(r"\s+", " ", title).strip()

        # 链接
        a = it.find("a", href=True)
        link = urljoin(page_url, a["href"]) if a else page_url

        rows.append({
            "candidate_id": hashlib.md5((title + link).encode("utf-8")).hexdigest()[:16],
            "title": title,
            "date_raw": date_raw,
            "date": normalize_date(date_raw),
            "event_type_guess": detect_event_type(text),
            "snippet": text[:600],
            "source_url": link,
            "source_page": page_url,
            "crawl_time": datetime.now(timezone.utc).isoformat(),
        })

    return rows

def crawl_one_url(url: str, tag: str = "seed"):
    try:
        html = fetch(url)
        save_html(url, html)
        rows = extract_candidates_from_html(url, html)
        return rows
    except Exception as e:
        log_error(tag, url, e)
        return []

def crawl_paginated(template: str, start: int, end: int, sleep_sec: float = 0.8):
    all_rows = []
    for page in range(start, end + 1):
        url = template.format(page=page)
        rows = crawl_one_url(url, tag="page")
        all_rows.extend(rows)
        time.sleep(sleep_sec)
    return all_rows

def crawl_queries(keywords, sleep_sec: float = 1.0):
    all_rows = []
    for kw in keywords:
        q = f"{kw}{QUERY_SUFFIX}"
        query_url = f"https://duckduckgo.com/html/?q={quote(q)}"
        rows = crawl_one_url(query_url, tag="query")
        for r in rows:
            r["query"] = kw
        all_rows.extend(rows)
        time.sleep(sleep_sec)
    return all_rows

def load_keywords(path="config/keywords.txt"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing keywords file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        kws = [x.strip() for x in f if x.strip()]
    return kws

def merge_and_save(new_rows, out_path=OUT_PATH):
    df_new = pd.DataFrame(new_rows)
    if df_new.empty:
        print("No new candidates found in this run.")
        return 0, 0

    # 清洗空值
    for c in ["title", "source_url", "snippet", "source_page", "date_raw", "date", "event_type_guess"]:
        if c not in df_new.columns:
            df_new[c] = ""
        df_new[c] = df_new[c].fillna("").astype(str)

    # 读历史并合并
    if os.path.exists(out_path):
        df_old = pd.read_csv(out_path).fillna("")
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new.copy()

    before = len(df)

    # 去重：title + source_url
    df["dedup_key"] = (df["title"].str.strip().str.lower() + "||" + df["source_url"].str.strip().str.lower())
    df = df.drop_duplicates(subset=["dedup_key"], keep="first").drop(columns=["dedup_key"])

    # 再去重：candidate_id
    if "candidate_id" in df.columns:
        df = df.drop_duplicates(subset=["candidate_id"], keep="first")

    after = len(df)
    added = after - (len(df_old) if os.path.exists(out_path) else 0)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return before, after

def main():
    keywords = load_keywords("config/keywords.txt")
    all_rows = []

    # A) 固定种子页
    for url in SEED_SITES:
        all_rows.extend(crawl_one_url(url, tag="seed"))
        time.sleep(0.5)

    # B) 分页站点
    for p in PAGINATED_SITES:
        all_rows.extend(crawl_paginated(p["template"], p["start"], p["end"], sleep_sec=0.8))

    # C) 搜索关键词
    all_rows.extend(crawl_queries(keywords, sleep_sec=1.0))

    before, after = merge_and_save(all_rows, OUT_PATH)
    print(f"Saved/merged candidates -> {OUT_PATH}")
    print(f"Rows before dedup merge-step: {before}")
    print(f"Rows after dedup: {after}")

if __name__ == "__main__":
    main()