import pandas as pd
import urllib.parse as up
from pathlib import Path

IN = Path("outputs/table_archivalization_v2_top30.csv")
OUTDIR = Path("outputs"); OUTDIR.mkdir(exist_ok=True)

def decode_duck(u: str) -> str:
    u = str(u or "")
    if "duckduckgo.com/l/?" not in u:
        return u
    try:
        qs = up.parse_qs(up.urlparse(u).query)
        real = qs.get("uddg", [u])[0]
        return up.unquote(real)
    except Exception:
        return u

df = pd.read_csv(IN).fillna("")
# 生成 [S1]… 编号（按当前顺序）
df["Sref"] = [f"[S{i}]" for i in range(1, len(df)+1)]
df["source_decoded"] = df["source"].apply(decode_duck)

# 表格（短）
table = df[["event_id","year","title","archivalization_score_v2","Sref"]].copy()
table.to_csv(OUTDIR/"table_archivalization_top30_Srefs.csv", index=False, encoding="utf-8-sig")

# 引用清单（长）
refs = df[["Sref","source_domain_real","source_decoded"]].copy()
refs.to_csv(OUTDIR/"refs_archivalization_top30.csv", index=False, encoding="utf-8-sig")

print("Done. Wrote:")
print(" - outputs/table_archivalization_top30_Srefs.csv")
print(" - outputs/refs_archivalization_top30.csv")
