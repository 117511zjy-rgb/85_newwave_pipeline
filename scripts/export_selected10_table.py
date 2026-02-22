import pandas as pd
from pathlib import Path

OUTDIR = Path("outputs")
OUTDIR.mkdir(exist_ok=True)

INFILE = OUTDIR / "table_semantic_noise_selected10.csv"
df = pd.read_csv(INFILE).fillna("")

def short_url(u, n=70):
    u = str(u)
    return u if len(u) <= n else u[:n] + "…"

if "source" in df.columns:
    df["source"] = df["source"].apply(short_url)

# xlsx
df.to_excel(OUTDIR / "table_semantic_noise_selected10.xlsx", index=False)

# markdown (needs tabulate)
(OUTDIR / "table_semantic_noise_selected10.md").write_text(
    df.to_markdown(index=False),
    encoding="utf-8"
)

# latex
(OUTDIR / "table_semantic_noise_selected10.tex").write_text(
    df.to_latex(index=False, escape=True),
    encoding="utf-8"
)

print("Done. Wrote:")
print(" - outputs/table_semantic_noise_selected10.xlsx")
print(" - outputs/table_semantic_noise_selected10.md")
print(" - outputs/table_semantic_noise_selected10.tex")
