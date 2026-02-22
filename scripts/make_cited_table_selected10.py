import re
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

OUTDIR = Path("outputs")
OUTDIR.mkdir(exist_ok=True)

INFILE = OUTDIR / "table_semantic_noise_selected10.csv"

def canonicalize_source(u: str) -> str:
    """Try to extract real target URL from duckduckgo redirect, otherwise return as-is."""
    u = (u or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        if "duckduckgo.com" in (p.netloc or "") and p.path.startswith("/l/"):
            qs = parse_qs(p.query)
            if "uddg" in qs and qs["uddg"]:
                return unquote(qs["uddg"][0])
        return u
    except Exception:
        return u

def build_sources(sources):
    canon = [canonicalize_source(s) for s in sources]
    # stable unique list in appearance order
    seen = {}
    ordered = []
    for s in canon:
        if s not in seen:
            seen[s] = True
            ordered.append(s)
    # map to [S1]...
    label = {s: f"S{i+1}" for i, s in enumerate(ordered)}
    return canon, ordered, label

def df_to_md_with_sources(df, ordered_sources):
    md_table = df.to_markdown(index=False)
    lines = [md_table, "", "Sources:"]
    for i, s in enumerate(ordered_sources, start=1):
        if s:
            lines.append(f"[S{i}] {s}")
        else:
            lines.append(f"[S{i}] (empty)")
    return "\n".join(lines)

def df_to_tex_with_sources(df, ordered_sources):
    # simple safe latex escaping
    def esc(x):
        x = "" if x is None else str(x)
        x = x.replace("\\", r"\textbackslash{}")
        x = x.replace("&", r"\&").replace("%", r"\%").replace("$", r"\$")
        x = x.replace("#", r"\#").replace("_", r"\_").replace("{", r"\{").replace("}", r"\}")
        x = x.replace("~", r"\textasciitilde{}").replace("^", r"\textasciicircum{}")
        return x

    cols = list(df.columns)
    # column align: left for text, right for numeric-like
    align = []
    for c in cols:
        if any(k in c.lower() for k in ["score", "year"]):
            align.append("r")
        else:
            align.append("l")
    colspec = " | ".join(align)

    header = " & ".join(esc(c) for c in cols) + r" \\"
    body = "\n".join(" & ".join(esc(v) for v in row) + r" \\" for row in df.astype(str).values.tolist())

    src_lines = []
    for i, s in enumerate(ordered_sources, start=1):
        src_lines.append(rf"\item [S{i}] {esc(s) if s else '(empty)'}")

    tex = rf"""\begin{{table}}[ht]
\centering
\small
\begin{{tabular}}{{{colspec}}}
\hline
{header}
\hline
{body}
\hline
\end{{tabular}}

\caption{{Selected 10 events with source references.}}
\end{{table}}

\noindent\textbf{{Sources}}
\begin{{description}}
{chr(10).join(src_lines)}
\end{{description}}
"""
    return tex

def main():
    df = pd.read_csv(INFILE).fillna("")
    if "source" not in df.columns:
        raise SystemExit("No 'source' column found in input CSV.")

    canon_sources, ordered, label = build_sources(df["source"].tolist())

    df2 = df.copy()
    df2["source_url"] = canon_sources  # keep canon url
    df2["source_ref"] = [f"[{label[s]}]" if s in label else "" for s in canon_sources]
    # replace display source with [S#]
    df2["source"] = df2["source_ref"]

    # reorder columns: keep original order but add source_url at end
    # also avoid showing raw long url in main table; keep in appendix file
    show_cols = [c for c in df.columns if c != "source"] + ["source"]
    appendix_cols = show_cols + ["source_url"]

    df_show = df2[show_cols]
    df_app = df2[appendix_cols]

    # write csv variants
    df_show.to_csv(OUTDIR / "table_semantic_noise_selected10_cited.csv", index=False, encoding="utf-8-sig")
    df_app.to_csv(OUTDIR / "table_semantic_noise_selected10_cited_with_urls.csv", index=False, encoding="utf-8-sig")

    # markdown + latex
    md = df_to_md_with_sources(df_show, ordered)
    (OUTDIR / "table_semantic_noise_selected10_cited.md").write_text(md, encoding="utf-8")

    tex = df_to_tex_with_sources(df_show, ordered)
    (OUTDIR / "table_semantic_noise_selected10_cited.tex").write_text(tex, encoding="utf-8")

    # excel: one sheet for table, one for sources, one for appendix
    try:
        import openpyxl  # noqa: F401
        with pd.ExcelWriter(OUTDIR / "table_semantic_noise_selected10_cited.xlsx", engine="openpyxl") as w:
            df_show.to_excel(w, index=False, sheet_name="table")
            pd.DataFrame({"source_ref": [f"[S{i}]" for i in range(1, len(ordered)+1)],
                          "source_url": ordered}).to_excel(w, index=False, sheet_name="sources")
            df_app.to_excel(w, index=False, sheet_name="appendix_urls")
    except Exception:
        # if excel engine missing, silently skip
        pass

    print("Done. Wrote:")
    print(" - outputs/table_semantic_noise_selected10_cited.md")
    print(" - outputs/table_semantic_noise_selected10_cited.tex")
    print(" - outputs/table_semantic_noise_selected10_cited.csv")
    print(" - outputs/table_semantic_noise_selected10_cited_with_urls.csv")
    print(" - outputs/table_semantic_noise_selected10_cited.xlsx (if openpyxl available)")

if __name__ == "__main__":
    main()
