import re
from pathlib import Path
import pandas as pd

INP = Path("data_raw/events_raw_4d_enriched_city.csv")
OUT = Path("data_raw/events_raw_4d_enriched_geo.csv")
AUD1 = Path("outputs/table_geo_evidence_audit.csv")
AUD2 = Path("outputs/table_geo_scope_audit.csv")
Path("outputs").mkdir(exist_ok=True)

def norm(x):
    return "" if pd.isna(x) else str(x)

def infer_geo_scope(text: str):
    t = norm(text)
    if re.search(r"国际|全球|world|international|香港|台北|海外|M\+|AAA|Hong Kong", t, flags=re.I):
        return "transregional"
    if re.search(r"全国|中国|各地|前卫展|中国/前卫|China/Avant-Garde|展览史|档案展|回顾展", t, flags=re.I):
        return "national"
    if re.search(r"美术馆|艺术中心|空间|馆|开幕|展览", t):
        return "local"
    return "unknown"

def main():
    df = pd.read_csv(INP).fillna("")
    for col in ["geo_evidence", "geo_scope"]:
        if col not in df.columns:
            df[col] = ""

    txt = (
        df.get("title","").astype(str) + " " +
        df.get("organizations","").astype(str) + " " +
        df.get("themes_raw","").astype(str) + " " +
        df.get("source_kind","").astype(str) + " " +
        df.get("institution_level","").astype(str)
    )

    # geo_scope
    empty_scope = df["geo_scope"].astype(str).str.strip() == ""
    df.loc[empty_scope, "geo_scope"] = txt[empty_scope].apply(infer_geo_scope)

    # geo_evidence
    city_nonempty = df.get("city","").astype(str).str.strip() != ""
    text_has_city = txt.apply(lambda s: bool(re.search(r"北京|上海|广州|深圳|南京|杭州|武汉|成都|重庆|西安|香港|台北", norm(s))))
    df["geo_evidence"] = ""  # 重算，避免旧值干扰
    df.loc[city_nonempty | text_has_city, "geo_evidence"] = "A_exact"

    if "city_impute_method" in df.columns:
        bmask = (df["geo_evidence"].astype(str).str.strip() == "") & (df["city_impute_method"] == "domain_bias")
        df.loc[bmask, "geo_evidence"] = "B_inferred"

    df.loc[df["geo_evidence"].astype(str).str.strip() == "", "geo_evidence"] = "C_unknown"

    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    audit1 = df["geo_evidence"].value_counts(dropna=False).rename_axis("geo_evidence").reset_index(name="count")
    audit2 = df["geo_scope"].value_counts(dropna=False).rename_axis("geo_scope").reset_index(name="count")
    audit1.to_csv(AUD1, index=False, encoding="utf-8-sig")
    audit2.to_csv(AUD2, index=False, encoding="utf-8-sig")

    print("Done:", OUT)
    print("geo_evidence:", df["geo_evidence"].value_counts(dropna=False).to_dict())
    print("geo_scope:", df["geo_scope"].value_counts(dropna=False).to_dict())
    print("Audit written:", AUD1, "and", AUD2)

if __name__ == "__main__":
    main()
