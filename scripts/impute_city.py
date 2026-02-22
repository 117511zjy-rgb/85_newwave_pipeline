import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
import pandas as pd

INP = Path("data_raw/events_raw_4d_enriched.csv")
OUT = Path("data_raw/events_raw_4d_enriched_city.csv")

# 这些域名本质是“平台/数据库/百科/电商/拍卖”，域名不应直接推城市
PLATFORM_DOMAINS = {
    "www.artlinkart.com",
    "book.douban.com",
    "baike.baidu.com",
    "auction.artron.net",
    "zhuanlan.zhihu.com",
    "wikii.one",
    "www.artforum.com.cn",  # 偏媒体平台属性
    "www.sohu.com",
    "art.china.cn",
    "book.kongfz.com",
    "artforum.com.cn",
    "www.artron.net",
}

# 机构/域名 -> 城市（只放“强关联机构站点/馆方站点/艺术家官网”）
DOMAIN_CITY = {
    # 北京
    "ucca.org.cn": "北京",
    "cafa.com.cn": "北京",
    "cafa.edu.cn": "北京",
    "namoc.org": "北京",
    "namoc.cn": "北京",
    "www.redbrickartmuseum.org": "北京",  # 红砖美术馆
    "redbrickartmuseum.org": "北京",
    "www.longmarchspace.com": "北京",     # 长征空间
    "longmarchspace.com": "北京",
    "arthing.org": "北京",                # ARThing（偏北京，可视为媒体/机构混合；暂当北京）
    "www.arthing.org": "北京",

    # 上海
    "psa.org.cn": "上海",

    # 香港
    "aaa.org.hk": "香港",                 # 亚洲艺术文献库
    "www.mplus.org.hk": "香港",
    "mplus.org.hk": "香港",

    # 艺术家/机构个人站点（地理弱，但常带城市线索；先不直接推城）
    # "xubing.com": ???  -> 不强行落城，用文本线索
}

# 机构/关键词 -> 城市（更可靠：标题/机构字段/主题/摘要/来源页）
ORG_CITY_HINTS = {
    # 北京（机构）
    "尤伦斯": "北京",
    "UCCA": "北京",
    "中国美术馆": "北京",
    "中央美术学院": "北京",
    "CAFA": "北京",
    "红砖美术馆": "北京",
    "Red Brick Art Museum": "北京",
    "长征空间": "北京",
    "Long March Space": "北京",
    "今日美术馆": "北京",
    "北京": "北京",

    # 上海
    "上海当代艺术博物馆": "上海",
    "PSA": "上海",
    "上海美术馆": "上海",
    "上海": "上海",

    # 香港
    "亚洲艺术文献库": "香港",
    "AAA": "香港",
    "M+": "香港",
    "香港": "香港",

    # 广州/深圳
    "广东美术馆": "广州",
    "广州美术学院": "广州",
    "广州": "广州",
    "深圳美术馆": "深圳",
    "深圳": "深圳",

    # 重庆/西安/南京/杭州/武汉/成都（预留）
    "四川美术学院": "重庆",
    "重庆": "重庆",
    "西安美术学院": "西安",
    "西安": "西安",
    "南京艺术学院": "南京",
    "南京": "南京",
    "浙江美术学院": "杭州",
    "中国美术学院": "杭州",
    "杭州": "杭州",
    "湖北美术学院": "武汉",
    "武汉": "武汉",
    "成都": "成都",
}

CITY_LIST = [
    "北京","上海","广州","深圳","南京","杭州","武汉","成都","重庆","西安",
    "香港","台北","天津","苏州","厦门","青岛","济南","郑州","长沙","昆明"
]

def norm(x):
    return "" if pd.isna(x) else str(x)

def unwrap_redirect(url: str) -> str:
    u = norm(url).strip()
    if not u:
        return ""
    try:
        pu = urlparse(u)
        if "duckduckgo.com" in pu.netloc and pu.path.startswith("/l/"):
            q = parse_qs(pu.query)
            if "uddg" in q and len(q["uddg"]) > 0:
                return unquote(q["uddg"][0]).strip()
    except Exception:
        pass
    return u

def extract_domain(url: str) -> str:
    u = unwrap_redirect(url)
    try:
        pu = urlparse(u)
        return (pu.netloc or "").lower()
    except Exception:
        return ""

def find_city_in_text(text: str) -> str:
    t = norm(text)
    for c in CITY_LIST:
        if c and c in t:
            return c
    return ""

def best_city_from_hints(title, orgs, themes, snippet, source_page) -> str:
    joined = " ".join([norm(title), norm(orgs), norm(themes), norm(snippet), norm(source_page)])
    # 1) 直接命中城市词
    c = find_city_in_text(joined)
    if c:
        return c
    # 2) 机构词典
    for k, v in ORG_CITY_HINTS.items():
        if k and (k in joined):
            return v
    return ""

def main():
    df = pd.read_csv(INP).fillna("")
    if "city" not in df.columns:
        df["city"] = ""
    if "city_impute_method" not in df.columns:
        df["city_impute_method"] = ""
    if "source_domain_real" not in df.columns:
        df["source_domain_real"] = df.get("source","").apply(extract_domain)
    else:
        # 重新计算一次，避免旧值还是 duckduckgo.com
        df["source_domain_real"] = df.get("source","").apply(extract_domain)

    for i, r in df.iterrows():
        city = norm(r.get("city","")).strip()
        if city:
            if not norm(r.get("city_impute_method","")).strip():
                df.at[i, "city_impute_method"] = "original"
            continue

        title = r.get("title","")
        orgs = r.get("organizations","")
        themes = r.get("themes_raw","") or r.get("themes","")
        snippet = r.get("snippet","")
        source_page = r.get("source_page","")
        src = r.get("source","")
        dom = df.at[i, "source_domain_real"]

        # 1) 文本/机构线索优先
        c = best_city_from_hints(title, orgs, themes, snippet, source_page)
        if c:
            df.at[i, "city"] = c
            df.at[i, "city_impute_method"] = "text_or_org_hint"
            continue

        # 2) 域名线索（仅对“非平台域名”生效）
        if dom and (dom not in PLATFORM_DOMAINS) and (dom in DOMAIN_CITY):
            df.at[i, "city"] = DOMAIN_CITY[dom]
            df.at[i, "city_impute_method"] = "domain_bias"
            continue

    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    city_missing = (df["city"].astype(str).str.strip()=="").mean()
    print("Done:", OUT)
    print("rows:", len(df))
    print("city_missing:", city_missing)
    print("top cities:", df["city"].astype(str).value_counts().head(10).to_dict())
    print("impute methods:", df["city_impute_method"].astype(str).value_counts().to_dict())
    print("top missing domains:", df[df["city"].astype(str).str.strip()==""].get("source_domain_real","").value_counts().head(15).to_dict())

if __name__ == "__main__":
    main()
