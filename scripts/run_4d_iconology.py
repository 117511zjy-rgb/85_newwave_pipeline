import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

IN_PATH = "data_raw/events_raw_4d_working.csv"
OUT_DIR = Path("outputs")
OUT_DIR.mkdir(exist_ok=True)

def gini(x: np.ndarray) -> float:
    x = x.astype(float)
    x = x[x >= 0]
    if len(x) == 0:
        return 0.0
    if np.allclose(x.sum(), 0):
        return 0.0
    x = np.sort(x)
    n = len(x)
    cumx = np.cumsum(x)
    return float((n + 1 - 2 * (cumx / cumx[-1]).sum()) / n)

def main():
    df = pd.read_csv(IN_PATH).fillna("")
    df["year"] = df["year"].astype(str).str[:4]
    df = df[df["year"].str.match(r"^\d{4}$", na=False)].copy()

    yearly = df.groupby("year").size().rename("n_events").reset_index()
    if "rupture_score" in df.columns:
        df["rupture_score"] = pd.to_numeric(df["rupture_score"], errors="coerce").fillna(0.0)
        yearly_r = df.groupby("year")["rupture_score"].mean().rename("rupture_mean").reset_index()
        yearly = yearly.merge(yearly_r, on="year", how="left")
    else:
        yearly["rupture_mean"] = np.nan

    yearly = yearly.sort_values("year")
    yearly.to_csv(OUT_DIR / "table_4d_time_yearly.csv", index=False, encoding="utf-8-sig")

    df["city_clean"] = df["city"].astype(str).str.strip()

    def city_gini(group):
        vc = group["city_clean"].replace("", "unknown").value_counts().values
        return gini(vc)

    space = df.groupby("year").apply(city_gini).rename("city_gini").reset_index()
    space.to_csv(OUT_DIR / "table_4d_space_city_gini_yearly.csv", index=False, encoding="utf-8-sig")

    if "institution_level" in df.columns:
        inst = (df.assign(institution_level=df["institution_level"].replace("", "unknown"))
                  .groupby(["year", "institution_level"])
                  .size().rename("n").reset_index())
        inst["share"] = inst.groupby("year")["n"].transform(lambda s: s / s.sum())
        inst.to_csv(OUT_DIR / "table_4d_social_institution_yearly.csv", index=False, encoding="utf-8-sig")
    else:
        inst = pd.DataFrame(columns=["year","institution_level","n","share"])

    if "diffusion_channel" in df.columns:
        tech = (df.assign(diffusion_channel=df["diffusion_channel"].replace("", "unknown"))
                  .groupby(["year", "diffusion_channel"])
                  .size().rename("n").reset_index())
        tech["share"] = tech.groupby("year")["n"].transform(lambda s: s / s.sum())
        tech.to_csv(OUT_DIR / "table_4d_tech_channel_yearly.csv", index=False, encoding="utf-8-sig")
    else:
        tech = pd.DataFrame(columns=["year","diffusion_channel","n","share"])

    plt.figure()
    plt.plot(yearly["year"], yearly["n_events"], marker="o")
    plt.xticks(rotation=45)
    plt.title("Time: Events per year")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "fig_4d_time_events_per_year.png", dpi=200)
    plt.close()

    plt.figure()
    plt.plot(yearly["year"], yearly["rupture_mean"], marker="o")
    plt.xticks(rotation=45)
    plt.title("Time: Rupture mean per year")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "fig_4d_time_rupture_mean.png", dpi=200)
    plt.close()

    plt.figure()
    plt.plot(space["year"], space["city_gini"], marker="o")
    plt.xticks(rotation=45)
    plt.title("Space: City concentration (Gini) per year")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "fig_4d_space_city_gini.png", dpi=200)
    plt.close()

    if len(inst):
        top_inst = (inst.groupby("institution_level")["n"].sum()
                      .sort_values(ascending=False).head(6).index.tolist())
        inst2 = inst[inst["institution_level"].isin(top_inst)].copy()
        pivot = inst2.pivot(index="year", columns="institution_level", values="share").fillna(0)
        plt.figure()
        for col in pivot.columns:
            plt.plot(pivot.index, pivot[col], marker="o", label=col)
        plt.xticks(rotation=45)
        plt.title("Social: Institution-level share (top 6)")
        plt.legend(fontsize=8)
        plt.tight_layout()
        plt.savefig(OUT_DIR / "fig_4d_social_institution_share.png", dpi=200)
        plt.close()

    if len(tech):
        top_ch = (tech.groupby("diffusion_channel")["n"].sum()
                    .sort_values(ascending=False).head(6).index.tolist())
        tech2 = tech[tech["diffusion_channel"].isin(top_ch)].copy()
        pivot = tech2.pivot(index="year", columns="diffusion_channel", values="share").fillna(0)
        plt.figure()
        for col in pivot.columns:
            plt.plot(pivot.index, pivot[col], marker="o", label=col)
        plt.xticks(rotation=45)
        plt.title("Tech: Diffusion channel share (top 6)")
        plt.legend(fontsize=8)
        plt.tight_layout()
        plt.savefig(OUT_DIR / "fig_4d_tech_channel_share.png", dpi=200)
        plt.close()

    print("Done. 4D outputs written to outputs/")

if __name__ == "__main__":
    main()
