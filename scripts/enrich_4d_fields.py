# scripts/enrich_4d_fields.py
# 목적：对 events_raw_4d_working.csv 做回顾性四维字段补全（可审计），降低 unknown
# 输出：
#   data_raw/events_raw_4d_enriched.csv
#   outputs/table_enrich_audit_summary.csv
#
# 运行：
#   python -u scripts/enrich_4d_fields.py

import re
from pathlib import Path
import pandas as pd

IN_PATH = Path("data_raw/events_raw_4d_working.csv")
OUT_PATH = Path("data_raw/events_raw_4d_enriched.csv")
AUDIT_PATH = Path("outputs/table_enrich_audit_summary.csv")
Path("outputs").mkdir(exist_ok=True)

# -------------------------
# 1) 规则库：domain -> source_kind / institution_level / tech_channel
# -------------------------
DOMAIN_RULES = [
    # database / archive-like
    (r"(artlinkart\.com)", "database", "database", "database"),
    (r"(arthing\.org)", "database", "database", "database"),
    (r"(artda\.cn)", "database", "database", "database"),
    (r"(hiart\.cn)", "media", "media", "retrospective_web"),
    (r"(trueart\.com)", "media", "media", "retrospective_web"),

    # institution-ish
    (r"(cafa\.com\.cn)", "institution", "institution", "retrospective_web"),
    (r"(ucca\.org\.cn)", "institution", "museum", "retrospective_web"),

    # encyclopedia / book pages
    (r"(baike\.baidu\.com)", "encyclopedia", "encyclopedia", "retrospective_web"),
    (r"(douban\.com)", "encyclopedia", "encyclopedia", "retrospective_web"),

    # mass media / platforms
    (r"(sohu\.com)", "media_platform", "media", "retrospective_web"),
    (r"(ifeng\.com)", "media_platform", "media", "retrospective_web"),
    (r"(kankanews\.com)", "media_platform", "media", "retrospective_web"),
    (r"(zhihu\.com)", "media_platform", "media", "retrospective_web"),

    # default fallback
    (r".*", "unknown", "unknown", "retrospective_web"),
]

CITY_LIST = [
    "北京","上海","广州","深圳","南京","杭州","武汉","成都","重庆","西安","沈阳","长春","哈尔滨",
    "天津","苏州","无锡","宁波","厦门","福州","青岛","济南","郑州","长沙","南昌","昆明",
    "贵阳","兰州","乌鲁木齐","呼和浩特","南宁","海口","大连","珠海"
]

# 一些“更像材料/影印/馆藏/目录”的迹象：不是原始，但更接近 primary-like
PRIMARY_LIKE_HINTS = [
    "影印","扫描","PDF","期刊","目录","馆藏","档案","年鉴","文献","图录","索引","编年","年表"
]

# -------------------------
# 2) 主题：先做可计算簇（不用追求艺术史完美分类）
# -------------------------
THEME_CLUSTERS = {
    "avantgarde_discourse": ["前卫","先锋","新潮","实验","观念","现代性","激进","反叛"],
    "archivalization": ["文献","档案","回顾","年表","编年","资料","图录","记录","索引"],
    "institutional_exhibition": ["展览","美术馆","艺术中心","策展","馆藏","开幕","展陈"],
    "polemic_criticism": ["批评","争论","论战","访谈","宣言","辩论","反思","批判"],
    "marketization": ["市场","拍卖","画廊","藏家","成交","价格","商业","基金"],
    "media_diffusion": ["媒体","报道","专题","直播","公众号","平台","传播","新闻"],
}

# -------------------------
# 3) 工具函数
# -------------------------
def norm_str(x):
    return "" if pd.isna(x) else str(x)

def pick_first_nonempty(*xs):
    for x in xs:
        x = norm_str(x).strip()
        if x:
            return x
    return ""

def extract_domain(url: str) -> str:
    url = norm_str(url)
    m = re.search(r"https?://([^/]+)/?", url)
    if not m:
        return ""
    return m.group(1).lower()

def apply_domain_rules(url: str):
    d = extract_domain(url)
    for pat, source_kind, institution_level, tech_channel in DOMAIN_RULES:
        if re.search(pat, d):
            return source_kind, institution_level, tech_channel, d
    return "unknown","unknown","retrospective_web", d

def infer_city(text: str):
    text = norm_str(text)
    for c in CITY_LIST:
        if c and c in text:
            return c
    return ""

def mark_primary_like(text: str):
    t = norm_str(text)
    return int(any(h in t for h in PRIMARY_LIKE_HINTS))

def cluster_themes(text: str):
    t = norm_str(text)
    hits = []
    for k, kws in THEME_CLUSTERS.items():
        for w in kws:
            if w in t:
                hits.append(k)
                break
    # 去重保持顺序
    seen = set()
    out = []
    for h in hits:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return ";".join(out)

def audit_ratio(series):
    s = series.fillna("").astype(str).str.strip()
    return float((s == "").mean())

# -------------------------
# 4) 主流程
# -------------------------
def main():
    if not IN_PATH.exists():
        raise FileNotFoundError(f"Missing {IN_PATH}")

    df = pd.read_csv(IN_PATH).fillna("")

    # 兼容：确保这些列存在（不存在就补空）
    must_cols = [
        "event_id","title","date","year","city","event_type","actors","organizations","themes","source","confidence",
        # 可能你已有的4D列（不强制）
        "space_scale","institution_level","tech_channel",
        "semantic_noise","semantic_noise_score",
    ]
    for c in must_cols:
        if c not in df.columns:
            df[c] = ""

    # year：没有就从 date 取
    if df["year"].astype(str).str.strip().eq("").mean() > 0.5:
        df["year"] = df["date"].astype(str).str[:4]
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)

    # 核心文本：用于城市/主题/primary-like判定
    core_text = (
        df["title"].astype(str) + " " +
        df.get("themes", "").astype(str) + " " +
        df.get("organizations", "").astype(str) + " " +
        df.get("event_type", "").astype(str)
    )

    # domain-driven inference
    src = df["source"].astype(str)
    out = src.apply(apply_domain_rules)
    df["source_kind"] = [x[0] for x in out]
    df["institution_level_rule"] = [x[1] for x in out]
    df["tech_channel_rule"] = [x[2] for x in out]
    df["source_domain"] = [x[3] for x in out]

    # source_kind 再升级：如果有“更像影印/档案/目录”的迹象，标记 primary_like
    df["primary_like_flag"] = core_text.apply(mark_primary_like)
    # 你可以在论文里说：primary_like ≠ 原始，但“形式更接近原始”
    # 这里让 source_kind 更细一点：在 database/media 的内部挑出 primary_like
    df["source_kind"] = df.apply(
        lambda r: ("primary_like" if int(r["primary_like_flag"]) == 1 else r["source_kind"]),
        axis=1
    )

    # institution_level：如果原本为空，就用 rule 填
    inst_empty = df["institution_level"].astype(str).str.strip() == ""
    df.loc[inst_empty, "institution_level"] = df.loc[inst_empty, "institution_level_rule"]
    df["institution_level_method"] = "kept"
    df.loc[inst_empty, "institution_level_method"] = "domain_rule"

    # tech_channel：同理
    tc_empty = df["tech_channel"].astype(str).str.strip() == ""
    df.loc[tc_empty, "tech_channel"] = df.loc[tc_empty, "tech_channel_rule"]
    df["tech_channel_method"] = "kept"
    df.loc[tc_empty, "tech_channel_method"] = "domain_rule"

    # city：优先保留已有；否则从文本提
    city_empty = df["city"].astype(str).str.strip() == ""
    df.loc[city_empty, "city"] = core_text[city_empty].apply(infer_city)
    df["city_method"] = "kept"
    df.loc[city_empty, "city_method"] = "heuristic_text"
    # 仍空就 unknown
    df.loc[df["city"].astype(str).str.strip() == "", "city_method"] = "unknown"

    # themes_raw / themes_cluster：保留原 themes，同时生成簇
    df["themes_raw"] = df["themes"].astype(str)
    df["themes_cluster"] = core_text.apply(cluster_themes)

    # space_scale（如果你用到了）：一个很粗但可用的回顾性尺度
    # 规则：有城市=local；多城市/“全国/中国/各地”=national；“国际/全球”=global
    if "space_scale" in df.columns:
        ss_empty = df["space_scale"].astype(str).str.strip() == ""
        txt = core_text.astype(str)
        df.loc[ss_empty, "space_scale"] = "local"
        df.loc[ss_empty & txt.str.contains("全国|中国|各地|巡展", regex=True), "space_scale"] = "national"
        df.loc[ss_empty & txt.str.contains("国际|全球|world|international", regex=True, case=False), "space_scale"] = "global"
        df["space_scale_method"] = "kept"
        df.loc[ss_empty, "space_scale_method"] = "text_rule"

    # 置信度（可选）：如果你的 confidence 都是空，给一个保守的 baseline
    conf_empty = df["confidence"].astype(str).str.strip() == ""
    df.loc[conf_empty, "confidence"] = 0.60
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.60)

    # -------------------------
    # 5) 审计 summary（论文写方法论时很关键）
    # -------------------------
    audit_rows = []
    def add_audit(col):
        audit_rows.append({
            "field": col,
            "missing_ratio": audit_ratio(df[col]),
            "unique_top10": ";".join(df[col].astype(str).value_counts().head(10).index.astype(str).tolist())
        })

    for col in [
        "city","city_method",
        "institution_level","institution_level_method",
        "tech_channel","tech_channel_method",
        "source_kind","source_domain",
        "themes_raw","themes_cluster",
        "space_scale" if "space_scale" in df.columns else None
    ]:
        if col and col in df.columns:
            add_audit(col)

    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(AUDIT_PATH, index=False, encoding="utf-8-sig")

    # 输出 enriched
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print("Done. Wrote:")
    print(f" - {OUT_PATH}")
    print(f" - {AUDIT_PATH}")
    print("\nQuick check:")
    print("rows:", len(df))
    print("city missing:", round(audit_ratio(df["city"]), 3))
    print("institution_level missing:", round(audit_ratio(df["institution_level"]), 3))
    print("themes_cluster missing:", round(audit_ratio(df["themes_cluster"]), 3))

if __name__ == "__main__":
    main()