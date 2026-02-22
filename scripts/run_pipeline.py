import os
import re
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx

# =========================
# Config
# =========================
RAW_PATH = "data_raw/events_raw.csv"
OUT_DIR = "outputs"
os.makedirs(OUT_DIR, exist_ok=True)


# =========================
# Utilities
# =========================
def split_multi(v: str) -> List[str]:
    if pd.isna(v) or str(v).strip() == "":
        return []
    parts = re.split(r"[;；,，/|]+", str(v))
    return [p.strip() for p in parts if p.strip()]


def safe_year(date_str: str):
    if pd.isna(date_str):
        return np.nan
    s = str(date_str).strip()
    m = re.match(r"^(\d{4})", s)
    if m:
        return int(m.group(1))
    return np.nan


def minmax_series(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
    mn, mx = s.min(), s.max()
    if mx - mn == 0:
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mn) / (mx - mn)


def shannon_entropy(values: List[str]) -> float:
    """Shannon entropy for categorical list."""
    if not values:
        return 0.0
    vc = pd.Series(values).value_counts(normalize=True)
    return float(-(vc * np.log(vc + 1e-12)).sum())


# =========================
# Step 1: Load & clean
# =========================
def load_and_clean(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Cannot find input CSV: {path}")

    # Robust CSV read (skip malformed lines)
    df = pd.read_csv(path, engine="python", on_bad_lines="skip")

    required = [
        "event_id", "date", "city", "event_type", "actors",
        "organizations", "themes", "source", "confidence"
    ]
    for c in required:
        if c not in df.columns:
            df[c] = ""

    # Normalize text
    df["event_id"] = df["event_id"].astype(str).str.strip()
    df["city"] = df["city"].astype(str).str.strip()
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()
    df["actors"] = df["actors"].astype(str)
    df["organizations"] = df["organizations"].astype(str)
    df["themes"] = df["themes"].astype(str)
    df["source"] = df["source"].astype(str)

    # Year extraction
    df["year"] = df["date"].apply(safe_year)
    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype(int)

    # Confidence
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.8).clip(0, 1)

    # Remove empty ids
    df = df[df["event_id"].str.len() > 0].copy()

    # Deduplicate by event_id (keep row with higher confidence)
    df = df.sort_values("confidence", ascending=False).drop_duplicates(subset=["event_id"], keep="first")

    # Parsed list fields
    df["actors_list"] = df["actors"].apply(split_multi)
    df["orgs_list"] = df["organizations"].apply(split_multi)
    df["themes_list"] = df["themes"].apply(split_multi)

    # Save cleaned base table
    df.to_csv(f"{OUT_DIR}/table_events_clean.csv", index=False, encoding="utf-8-sig")
    return df


# =========================
# Step 2: Build yearly actor network
# =========================
def build_actor_graph(df_year: pd.DataFrame) -> nx.Graph:
    G = nx.Graph()
    for _, r in df_year.iterrows():
        actors = r["actors_list"]
        conf = float(r["confidence"])

        # Add nodes
        for a in actors:
            if not G.has_node(a):
                G.add_node(a)

        # Co-participation edges
        for i in range(len(actors)):
            for j in range(i + 1, len(actors)):
                u, v = actors[i], actors[j]
                if G.has_edge(u, v):
                    G[u][v]["weight"] += conf
                    G[u][v]["count"] += 1
                else:
                    G.add_edge(u, v, weight=conf, count=1)
    return G


def yearly_metrics(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, nx.Graph]]:
    years = sorted(df["year"].unique())
    rows = []
    graphs = {}

    prev_nodes = set()
    prev_edges = set()

    for y in years:
        d = df[df["year"] == y].copy()
        G = build_actor_graph(d)
        graphs[y] = G

        n = G.number_of_nodes()
        m = G.number_of_edges()
        density = nx.density(G) if n > 1 else 0.0
        event_count = len(d)

        # Reorganization proxy: 1 - Jaccard similarity (nodes+edges) vs prev year
        nodes = set(G.nodes())
        edges = set(tuple(sorted(e)) for e in G.edges())
        if prev_nodes:
            node_jacc = len(nodes & prev_nodes) / max(1, len(nodes | prev_nodes))
            edge_jacc = len(edges & prev_edges) / max(1, len(edges | prev_edges))
            reorg_raw = 1 - 0.5 * (node_jacc + edge_jacc)
        else:
            reorg_raw = 0.0

        rows.append({
            "year": y,
            "events": event_count,
            "nodes": n,
            "edges": m,
            "density": density,
            "reorg_raw": reorg_raw
        })

        prev_nodes = nodes
        prev_edges = edges

    met = pd.DataFrame(rows).sort_values("year").reset_index(drop=True)
    met.to_csv(f"{OUT_DIR}/table_network_metrics_yearly.csv", index=False, encoding="utf-8-sig")
    return met, graphs


# =========================
# Step 3: 85 New Wave specific O/R/S
# =========================
def compute_indices_85(df_events: pd.DataFrame, met: pd.DataFrame) -> pd.DataFrame:
    """
    O (Openness): event-type entropy + new actor ratio + theme novelty
    R (Reorganization): structural change proxy (reorg_raw + density change)
    S (SteadyState): actor persistence + theme repetition + low volatility
    """
    years = sorted(df_events["year"].unique())
    stat_rows = []

    seen_actors = set()
    seen_themes = set()

    for y in years:
        d = df_events[df_events["year"] == y]

        # Event type entropy
        type_entropy = shannon_entropy(d["event_type"].fillna("other").astype(str).tolist())

        # Actors set
        actors_y = set()
        for arr in d["actors_list"]:
            actors_y.update(arr)

        # Themes set
        themes_y = set()
        for arr in d["themes_list"]:
            themes_y.update(arr)

        # Openness components
        new_actor_ratio = len(actors_y - seen_actors) / max(1, len(actors_y))
        theme_novelty = len(themes_y - seen_themes) / max(1, len(themes_y))

        # Steady-state components
        actor_persistence = len(actors_y & seen_actors) / max(1, len(actors_y))
        theme_repetition = len(themes_y & seen_themes) / max(1, len(themes_y))

        stat_rows.append({
            "year": y,
            "type_entropy": type_entropy,
            "new_actor_ratio": new_actor_ratio,
            "theme_novelty": theme_novelty,
            "actor_persistence": actor_persistence,
            "theme_repetition": theme_repetition
        })

        seen_actors |= actors_y
        seen_themes |= themes_y

    st = pd.DataFrame(stat_rows)
    df = met.merge(st, on="year", how="left").sort_values("year").reset_index(drop=True)

    # Build O/R/S
    df["density_change"] = df["density"].diff().abs().fillna(0.0)

    # O
    df["O"] = pd.concat([
        minmax_series(df["type_entropy"]),
        minmax_series(df["new_actor_ratio"]),
        minmax_series(df["theme_novelty"])
    ], axis=1).mean(axis=1)

    # R
    df["R"] = pd.concat([
        minmax_series(df["reorg_raw"]),
        minmax_series(df["density_change"])
    ], axis=1).mean(axis=1)

    # S
    df["S"] = pd.concat([
        minmax_series(df["actor_persistence"]),
        minmax_series(df["theme_repetition"]),
        1 - minmax_series(df["density_change"])
    ], axis=1).mean(axis=1).clip(0, 1)

    # Stage rule
    def stage_rule(row):
        if row["R"] >= max(row["O"], row["S"]):
            return "Reorganization"
        if row["O"] > row["S"]:
            return "Openness"
        return "SteadyState"

    df["stage"] = df.apply(stage_rule, axis=1)

    # Soft probabilities
    p = df[["O", "R", "S"]].copy()
    p = p.div(p.sum(axis=1).replace(0, 1), axis=0)
    df["p_openness"] = p["O"]
    df["p_reorganization"] = p["R"]
    df["p_steadystate"] = p["S"]

    # Save combined table
    keep_cols = [
        "year", "events", "nodes", "edges", "density", "reorg_raw",
        "type_entropy", "new_actor_ratio", "theme_novelty",
        "actor_persistence", "theme_repetition",
        "O", "R", "S", "stage",
        "p_openness", "p_reorganization", "p_steadystate"
    ]
    df[keep_cols].to_csv(f"{OUT_DIR}/table_indices_and_stages_yearly.csv", index=False, encoding="utf-8-sig")
    return df


# =========================
# Step 4: Bridge actors
# =========================
def top_bridges(graphs: Dict[int, nx.Graph], topn: int = 10) -> pd.DataFrame:
    rows = []
    for y, G in graphs.items():
        if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
            continue
        bet = nx.betweenness_centrality(G, weight="weight", normalized=True)
        top = sorted(bet.items(), key=lambda t: t[1], reverse=True)[:topn]
        for rank, (actor, val) in enumerate(top, start=1):
            rows.append({
                "year": y,
                "rank": rank,
                "actor": actor,
                "betweenness": val
            })
    tb = pd.DataFrame(rows)
    tb.to_csv(f"{OUT_DIR}/table_top_bridges.csv", index=False, encoding="utf-8-sig")
    return tb


# =========================
# Step 5: Plotting
# =========================
def plot_events_timeline(df: pd.DataFrame):
    g = df.groupby("year")["event_id"].count().reset_index(name="events")
    plt.figure(figsize=(10, 4))
    plt.plot(g["year"], g["events"], marker="o")
    plt.title("Event Count Timeline")
    plt.xlabel("Year")
    plt.ylabel("Number of events")
    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/fig_events_timeline.png", dpi=220)
    plt.close()


def plot_indices(df_stage: pd.DataFrame):
    plt.figure(figsize=(10, 5))
    plt.plot(df_stage["year"], df_stage["O"], label="O (Openness)")
    plt.plot(df_stage["year"], df_stage["R"], label="R (Reorganization)")
    plt.plot(df_stage["year"], df_stage["S"], label="S (SteadyState)")
    plt.legend()
    plt.title("85 New Wave O/R/S Indices Over Time")
    plt.xlabel("Year")
    plt.ylabel("Index (0-1)")
    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/fig_indices_ors.png", dpi=220)
    plt.close()


def plot_stage_probabilities(df_stage: pd.DataFrame):
    plt.figure(figsize=(10, 5))
    plt.plot(df_stage["year"], df_stage["p_openness"], label="P(Openness)")
    plt.plot(df_stage["year"], df_stage["p_reorganization"], label="P(Reorganization)")
    plt.plot(df_stage["year"], df_stage["p_steadystate"], label="P(SteadyState)")
    plt.legend()
    plt.title("Stage Probabilities")
    plt.xlabel("Year")
    plt.ylabel("Probability")
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/fig_stage_probabilities.png", dpi=220)
    plt.close()


def plot_network_snapshots(graphs: Dict[int, nx.Graph], years: List[int]):
    for y in years:
        G = graphs.get(y)
        if G is None or G.number_of_nodes() == 0:
            continue
        plt.figure(figsize=(7, 7))
        pos = nx.spring_layout(G, seed=42, k=0.6)
        w = [G[u][v].get("weight", 1.0) for u, v in G.edges()]
        max_w = max(w) if w else 1.0
        widths = [0.3 + 1.2 * (x / max_w) for x in w]
        nx.draw_networkx_nodes(G, pos, node_size=60)
        nx.draw_networkx_edges(G, pos, width=widths, alpha=0.4)
        # labels only for small graphs
        if G.number_of_nodes() <= 25:
            nx.draw_networkx_labels(G, pos, font_size=8)
        plt.title(f"Actor Co-participation Network ({y})")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(f"{OUT_DIR}/fig_network_year_{y}.png", dpi=220)
        plt.close()


# =========================
# Main
# =========================
def main():
    df = load_and_clean(RAW_PATH)
    plot_events_timeline(df)

    met, graphs = yearly_metrics(df)
    stage = compute_indices_85(df, met)
    top_bridges(graphs, topn=12)

    plot_indices(stage)
    plot_stage_probabilities(stage)

    years = sorted(graphs.keys())
    if len(years) >= 3:
        snapshots = [years[0], years[len(years)//2], years[-1]]
    else:
        snapshots = years
    plot_network_snapshots(graphs, snapshots)

    print("Done. Files written to outputs/")


if __name__ == "__main__":
    main()