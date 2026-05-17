"""
COMPARISON GRAPHS -- Raft vs PBFT
===================================
Generates publication-ready graphs for the 4 comparison scenarios.
Reads from analysis/comparison_results.json produced by benchmark/comparison.py

Usage:
    python analysis/graphs_comparison.py
"""

import json
import os

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("[!] matplotlib not found: pip install matplotlib")

RAFT_C  = "#2563EB"
PBFT_C  = "#7C3AED"
GRID_C  = "#E5E7EB"
OUT_DIR = "analysis/graphs"


def load(path="analysis/comparison_results.json"):
    if not os.path.exists(path):
        print(f"[!] {path} not found. Run: python -m benchmark.comparison")
        return []
    with open(path) as f:
        return json.load(f)


def filt(data, scenario=None, algo=None, **kwargs):
    out = data
    if scenario:
        out = [r for r in out if r["scenario"] == scenario]
    if algo:
        out = [r for r in out if r["algo"] == algo]
    for k, v in kwargs.items():
        out = [r for r in out if r.get(k) == v]
    return out


def savefig(fig, name):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


# ─────────────────────────────────────────────
# GRAPH 1 -- CHURN
# ─────────────────────────────────────────────

def graph_churn(data):
    """
    Bar chart: throughput and error rate for Raft vs PBFT under churn.
    Side-by-side bars make the comparison immediate.
    """
    if not HAS_MPL:
        return
    d = filt(data, scenario="churn")
    if not d:
        print("  [!] No churn data")
        return

    raft = next((r for r in d if r["algo"] == "raft"), None)
    pbft = next((r for r in d if r["algo"] == "pbft"), None)
    if not raft or not pbft:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5))
    fig.suptitle("Scenario 1 -- Churn: Continuous Node Crashes & Revivals",
                 fontsize=14, fontweight="bold")

    # Throughput
    bars = ax1.bar(["Raft", "PBFT"],
                   [raft["throughput_rps"], pbft["throughput_rps"]],
                   color=[RAFT_C, PBFT_C], width=0.4, alpha=0.88)
    for bar, val in zip(bars, [raft["throughput_rps"], pbft["throughput_rps"]]):
        ax1.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + 0.3,
                 f"{val:.1f}", ha="center", fontweight="bold", fontsize=11)
    ax1.set_ylabel("Throughput (req/s)", fontsize=12)
    ax1.set_title("Throughput under churn")
    ax1.grid(True, axis="y", color=GRID_C)
    ax1.set_ylim(0, max(raft["throughput_rps"], pbft["throughput_rps"]) * 1.25)

    # Error rate
    bars2 = ax2.bar(["Raft", "PBFT"],
                    [raft["error_rate"] * 100, pbft["error_rate"] * 100],
                    color=[RAFT_C, PBFT_C], width=0.4, alpha=0.88)
    for bar, val in zip(bars2,
                        [raft["error_rate"] * 100, pbft["error_rate"] * 100]):
        ax2.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + 0.2,
                 f"{val:.1f}%", ha="center", fontweight="bold", fontsize=11)
    ax2.set_ylabel("Error Rate (%)", fontsize=12)
    ax2.set_title("Error rate under churn")
    ax2.grid(True, axis="y", color=GRID_C)

    # Annotation
    crashes_r = raft.get("extra_crashes", "?")
    crashes_p = pbft.get("extra_crashes", "?")
    fig.text(0.5, 0.01,
             f"Raft: {crashes_r} crashes  |  PBFT: {crashes_p} crashes  "
             f"(same churn rate applied to both)",
             ha="center", fontsize=9, color="gray")

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    savefig(fig, "comparison_churn.png")


# ─────────────────────────────────────────────
# GRAPH 2 -- ASYMMETRIC DELAY
# ─────────────────────────────────────────────

def graph_asymmetric(data):
    """
    Line chart: avg latency vs delay applied to leader/primary.
    Both algorithms on the same axes -- the divergence tells the story.
    """
    if not HAS_MPL:
        return
    d = filt(data, scenario="asymmetric")
    if not d:
        print("  [!] No asymmetric data")
        return

    delays = sorted(set(r["delay_ms"] for r in d))
    raft_lat = [next((r["avg_latency_ms"] for r in d
                      if r["algo"] == "raft" and r["delay_ms"] == dl), 0)
                for dl in delays]
    pbft_lat = [next((r["avg_latency_ms"] for r in d
                      if r["algo"] == "pbft" and r["delay_ms"] == dl), 0)
                for dl in delays]
    raft_thr = [next((r["throughput_rps"] for r in d
                      if r["algo"] == "raft" and r["delay_ms"] == dl), 0)
                for dl in delays]
    pbft_thr = [next((r["throughput_rps"] for r in d
                      if r["algo"] == "pbft" and r["delay_ms"] == dl), 0)
                for dl in delays]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle(
        "Scenario 2 -- Asymmetric Delay: Messages to Leader/Primary Only",
        fontsize=14, fontweight="bold")

    # Latency
    ax1.plot(delays, raft_lat, "o-", color=RAFT_C, linewidth=2,
             markersize=7, label="Raft")
    ax1.plot(delays, pbft_lat, "s-", color=PBFT_C, linewidth=2,
             markersize=7, label="PBFT")
    ax1.fill_between(delays, raft_lat, pbft_lat, alpha=0.07, color=PBFT_C)
    ax1.set_xlabel("Delay added to leader/primary (ms)", fontsize=12)
    ax1.set_ylabel("Avg Latency (ms)", fontsize=12)
    ax1.set_title("Latency vs leader delay")
    ax1.legend()
    ax1.grid(True, color=GRID_C)

    # Throughput
    ax2.plot(delays, raft_thr, "o-", color=RAFT_C, linewidth=2,
             markersize=7, label="Raft")
    ax2.plot(delays, pbft_thr, "s-", color=PBFT_C, linewidth=2,
             markersize=7, label="PBFT")
    ax2.set_xlabel("Delay added to leader/primary (ms)", fontsize=12)
    ax2.set_ylabel("Throughput (req/s)", fontsize=12)
    ax2.set_title("Throughput vs leader delay")
    ax2.legend()
    ax2.grid(True, color=GRID_C)

    fig.text(0.5, 0.01,
             "Raft: leader is the single write bottleneck.  "
             "PBFT: replicas compensate via peer-to-peer PREPARE/COMMIT.",
             ha="center", fontsize=9, color="gray")

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    savefig(fig, "comparison_asymmetric.png")


# ─────────────────────────────────────────────
# GRAPH 3 -- SCALABILITY
# ─────────────────────────────────────────────

def graph_scalability(data):
    """
    Line chart: throughput and latency vs cluster size.
    Overlaid with theoretical O(n) and O(n^2) curves (normalised).
    The empirical lines should closely track the theoretical ones.
    """
    if not HAS_MPL:
        return
    d = filt(data, scenario="scalability")
    if not d:
        print("  [!] No scalability data")
        return

    raft_rows = sorted(filt(d, algo="raft"), key=lambda r: r["cluster_size"])
    pbft_rows = sorted(filt(d, algo="pbft"), key=lambda r: r["cluster_size"])

    raft_sizes = [r["cluster_size"] for r in raft_rows]
    raft_thr   = [r["throughput_rps"] for r in raft_rows]
    raft_lat   = [r["avg_latency_ms"] for r in raft_rows]
    pbft_sizes = [r["cluster_size"] for r in pbft_rows]
    pbft_thr   = [r["throughput_rps"] for r in pbft_rows]
    pbft_lat   = [r["avg_latency_ms"] for r in pbft_rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Scenario 3 -- Scalability: Throughput & Latency vs Cluster Size",
                 fontsize=14, fontweight="bold")

    # -- Throughput with theoretical overlay --
    ax1.plot(raft_sizes, raft_thr, "o-", color=RAFT_C, linewidth=2,
             markersize=8, label="Raft (empirical)")
    ax1.plot(pbft_sizes, pbft_thr, "s-", color=PBFT_C, linewidth=2,
             markersize=8, label="PBFT (empirical)")

    # Theoretical: Raft degrades as 1/n, PBFT as 1/n^2 (normalised to first point)
    all_sizes = list(range(3, 11))
    if raft_thr:
        base_r = raft_thr[0] * raft_sizes[0]
        theo_raft = [base_r / n for n in all_sizes]
        ax1.plot(all_sizes, theo_raft, "--", color=RAFT_C, alpha=0.35,
                 linewidth=1.5, label="Raft O(n) theory")
    if pbft_thr:
        base_p = pbft_thr[0] * pbft_sizes[0] ** 2
        theo_pbft = [base_p / n ** 2 for n in all_sizes]
        ax1.plot(all_sizes, theo_pbft, "--", color=PBFT_C, alpha=0.35,
                 linewidth=1.5, label="PBFT O(n^2) theory")

    ax1.set_xlabel("Cluster size (n)", fontsize=12)
    ax1.set_ylabel("Throughput (req/s)", fontsize=12)
    ax1.set_title("Throughput vs cluster size")
    ax1.legend(fontsize=9)
    ax1.grid(True, color=GRID_C)
    ax1.set_xticks(list(range(3, 11)))

    # -- Latency --
    ax2.plot(raft_sizes, raft_lat, "o-", color=RAFT_C, linewidth=2,
             markersize=8, label="Raft")
    ax2.plot(pbft_sizes, pbft_lat, "s-", color=PBFT_C, linewidth=2,
             markersize=8, label="PBFT")
    ax2.set_xlabel("Cluster size (n)", fontsize=12)
    ax2.set_ylabel("Avg Latency (ms)", fontsize=12)
    ax2.set_title("Latency vs cluster size")
    ax2.legend()
    ax2.grid(True, color=GRID_C)
    ax2.set_xticks(list(range(3, 11)))

    # Highlight n=7 overlap if both have it
    if 7 in raft_sizes and 7 in pbft_sizes:
        for ax in (ax1, ax2):
            ax.axvline(7, color="gray", linestyle=":", alpha=0.6)
            ax.text(7.1, ax.get_ylim()[1] * 0.95, "n=7\noverlap",
                    fontsize=8, color="gray")

    plt.tight_layout()
    savefig(fig, "comparison_scalability.png")


# ─────────────────────────────────────────────
# GRAPH 4 -- SLOW NODE
# ─────────────────────────────────────────────

def graph_slow_node(data):
    """
    Grouped bar chart: throughput for (Raft, PBFT) x (slow follower, slow leader).
    Makes the architectural difference immediately visible:
    - Raft is barely affected by a slow follower.
    - PBFT is significantly affected by a slow replica.
    """
    if not HAS_MPL:
        return
    d = filt(data, scenario="slow_node")
    if not d:
        print("  [!] No slow_node data")
        return

    configs = [("raft", "follower"), ("raft", "leader"),
               ("pbft", "follower"), ("pbft", "leader")]
    labels  = ["Raft\nslow follower", "Raft\nslow leader",
               "PBFT\nslow replica", "PBFT\nslow primary"]
    colors  = [RAFT_C, RAFT_C, PBFT_C, PBFT_C]
    alphas  = [0.5, 0.95, 0.5, 0.95]

    thr_vals = []
    lat_vals = []
    for algo, role in configs:
        row = next((r for r in d
                    if r["algo"] == algo and r["slow_role"] == role), None)
        thr_vals.append(row["throughput_rps"] if row else 0)
        lat_vals.append(row["avg_latency_ms"] if row else 0)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle(
        f"Scenario 4 -- Slow Node: One Node Delayed by "
        f"{d[0].get('slow_delay_ms', '?')}ms (Not Dead)",
        fontsize=14, fontweight="bold")

    xs = range(len(labels))

    # Throughput
    bars = ax1.bar(xs, thr_vals, color=colors,
                   alpha=0.88, width=0.5, edgecolor="white")
    for i, (bar, val) in enumerate(zip(bars, thr_vals)):
        # Dim follower bars to show they are the "less impacted" case
        bar.set_alpha(alphas[i])
        ax1.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + 0.2,
                 f"{val:.1f}", ha="center", fontweight="bold", fontsize=10)
    ax1.set_xticks(list(xs))
    ax1.set_xticklabels(labels, fontsize=10)
    ax1.set_ylabel("Throughput (req/s)", fontsize=12)
    ax1.set_title("Throughput: who is slow?")
    ax1.grid(True, axis="y", color=GRID_C)

    # Latency
    bars2 = ax2.bar(xs, lat_vals, color=colors,
                    alpha=0.88, width=0.5, edgecolor="white")
    for i, (bar, val) in enumerate(zip(bars2, lat_vals)):
        bar.set_alpha(alphas[i])
        ax2.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + 0.5,
                 f"{val:.0f}ms", ha="center", fontweight="bold", fontsize=10)
    ax2.set_xticks(list(xs))
    ax2.set_xticklabels(labels, fontsize=10)
    ax2.set_ylabel("Avg Latency (ms)", fontsize=12)
    ax2.set_title("Latency: who is slow?")
    ax2.grid(True, axis="y", color=GRID_C)

    # Legend
    raft_patch = mpatches.Patch(color=RAFT_C, label="Raft")
    pbft_patch = mpatches.Patch(color=PBFT_C, label="PBFT")
    light_patch = mpatches.Patch(facecolor="gray", alpha=0.4,
                                 label="slow follower/replica (light)")
    dark_patch  = mpatches.Patch(facecolor="gray", alpha=0.95,
                                 label="slow leader/primary (dark)")
    fig.legend(handles=[raft_patch, pbft_patch, light_patch, dark_patch],
               loc="lower center", ncol=4, fontsize=9,
               bbox_to_anchor=(0.5, -0.02))

    fig.text(0.5, -0.06,
             "Raft: slow follower barely matters (majority doesn't need it).  "
             "PBFT: slow replica stalls PREPARE phase -- all-to-all quorum waits.",
             ha="center", fontsize=9, color="gray")

    plt.tight_layout(rect=[0, 0.08, 1, 1])
    savefig(fig, "comparison_slow_node.png")


# ─────────────────────────────────────────────
# GRAPH 5 -- SUMMARY RADAR
# ─────────────────────────────────────────────

def graph_summary_radar(data):
    """
    Radar/spider chart summarising all 4 dimensions for Raft and PBFT.
    Dimensions: throughput, latency (inverted), fault tolerance, scalability.
    Gives an at-a-glance comparison for the report cover page.
    """
    if not HAS_MPL:
        return

    # Compute normalised scores [0,1] higher = better for both
    def score_throughput(algo):
        rows = filt(data, algo=algo, scenario="churn")
        if not rows:
            return 0.5
        return rows[0]["throughput_rps"]

    def score_latency(algo):  # lower latency = higher score
        rows = filt(data, algo=algo, scenario="churn")
        if not rows:
            return 0.5
        return 1 / (rows[0]["avg_latency_ms"] + 1)

    def score_error(algo):    # lower error = higher score
        rows = filt(data, algo=algo, scenario="churn")
        if not rows:
            return 0.5
        return 1 - rows[0]["error_rate"]

    def score_scalability(algo):  # throughput at largest tested size
        rows = sorted(filt(data, algo=algo, scenario="scalability"),
                      key=lambda r: r["cluster_size"])
        if not rows:
            return 0.5
        # More throughput at large n = better scalability
        return rows[-1]["throughput_rps"]

    def score_slow_resilience(algo):  # throughput with slow follower
        rows = filt(data, algo=algo, scenario="slow_node",
                    **{"slow_role": "follower"})
        if not rows:
            return 0.5
        return rows[0]["throughput_rps"]

    raw = {
        "raft": [score_throughput("raft"), score_latency("raft"),
                 score_error("raft"), score_scalability("raft"),
                 score_slow_resilience("raft")],
        "pbft": [score_throughput("pbft"), score_latency("pbft"),
                 score_error("pbft"), score_scalability("pbft"),
                 score_slow_resilience("pbft")],
    }

    # Normalise each dimension to [0, 1]
    labels = ["Throughput", "Low Latency", "Reliability",
              "Scalability", "Slow\nResilience"]
    scores = {}
    for dim in range(5):
        mx = max(raw["raft"][dim], raw["pbft"][dim]) or 1
        scores.setdefault("raft", []).append(raw["raft"][dim] / mx)
        scores.setdefault("pbft", []).append(raw["pbft"][dim] / mx)

    N = len(labels)
    angles = [n / float(N) * 2 * 3.14159 for n in range(N)]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(7, 7),
                           subplot_kw=dict(polar=True))

    for algo, color in [("raft", RAFT_C), ("pbft", PBFT_C)]:
        vals = scores[algo] + scores[algo][:1]
        ax.plot(angles, vals, "o-", color=color, linewidth=2, markersize=6,
                label=algo.upper())
        ax.fill(angles, vals, color=color, alpha=0.12)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_yticks([0.25, 0.5, 0.75, 1.0])
    ax.set_yticklabels(["25%", "50%", "75%", "100%"], fontsize=8, color="gray")
    ax.set_title("Overall Comparison -- Raft vs PBFT",
                 fontsize=14, fontweight="bold", pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1), fontsize=11)

    savefig(fig, "comparison_summary_radar.png")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def generate_all():
    print("\nGenerating comparison graphs...")
    data = load()
    if not data:
        return

    graph_churn(data)
    graph_asymmetric(data)
    graph_scalability(data)
    graph_slow_node(data)
    graph_summary_radar(data)

    print(f"\n  All graphs saved to {OUT_DIR}/")


if __name__ == "__main__":
    generate_all()