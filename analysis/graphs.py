"""
SAGEMATH ANALYSIS - Comparative Graph Generation
================================================
Load benchmark results and generate report-ready comparison graphs.

Generated graphs:
  1. Latency vs Load (Raft vs PBFT)
  2. Throughput vs Nodes
  3. Fault Tolerance Comparison
  4. Recovery Time Distribution
  5. Byzantine Impact on PBFT
  6. Message Complexity (theoretical)

USAGE (SageMath or Jupyter):
    load("analysis/graphs.sage")
    generate_all_graphs()
"""

import csv
import json
import os

# SageMath imports. Matplotlib is used as a fallback in plain Python.
try:
    from sage.all import *
    SAGE_AVAILABLE = True
except ImportError:
    SAGE_AVAILABLE = False
    print("[Warning] SageMath not found, using matplotlib fallback")

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


# Project color palette.
RAFT_COLOR  = "#2563EB"   # Blue
PBFT_COLOR  = "#7C3AED"   # Purple
CHAOS_COLOR = "#DC2626"   # Red
GRID_COLOR  = "#E5E7EB"


def load_results(path="analysis/results.json"):
    """Load benchmark results from JSON."""
    if not os.path.exists(path):
        print(f"[Warning] {path} not found. Run the benchmark first.")
        return []
    with open(path) as f:
        return json.load(f)


def filter_results(results, test=None, algo=None):
    """Filter benchmark results by test and/or algorithm."""
    out = results
    if test:
        out = [r for r in out if r["test"] == test]
    if algo:
        out = [r for r in out if r["algo"] == algo]
    return out


# Graph 1: latency vs load.

def plot_latency_vs_load(results, out="analysis/graphs/latency_vs_load.png"):
    """
    Compare average Raft and PBFT latency as request load increases.
    """
    data = filter_results(results, test="throughput_vs_load")

    raft_data = [(r["target_rps"], r["avg_latency_ms"]) for r in data if r["algo"] == "raft"]
    pbft_data = [(r["target_rps"], r["avg_latency_ms"]) for r in data if r["algo"] == "pbft"]

    raft_data.sort(); pbft_data.sort()

    if SAGE_AVAILABLE:
        raft_pts = point(raft_data, color=RAFT_COLOR, size=30, legend_label="Raft")
        pbft_pts = point(pbft_data, color=PBFT_COLOR, size=30, legend_label="PBFT")

        if len(raft_data) > 1:
            raft_x, raft_y = zip(*raft_data)
            raft_line = line(list(zip(raft_x, raft_y)), color=RAFT_COLOR, thickness=2)
        else:
            raft_line = Graphics()

        if len(pbft_data) > 1:
            pbft_x, pbft_y = zip(*pbft_data)
            pbft_line = line(list(zip(pbft_x, pbft_y)), color=PBFT_COLOR, thickness=2)
        else:
            pbft_line = Graphics()

        p = raft_pts + raft_line + pbft_pts + pbft_line
        p.axes_labels(["Load (req/s)", "Avg Latency (ms)"])
        p.set_legend_options(loc="upper left")
        p.save(out, title="Latency vs Load: Raft vs PBFT", figsize=[8, 5])

    elif MATPLOTLIB_AVAILABLE:
        fig, ax = plt.subplots(figsize=(8, 5))
        if raft_data:
            rx, ry = zip(*raft_data)
            ax.plot(rx, ry, 'o-', color=RAFT_COLOR, label="Raft", linewidth=2, markersize=6)
        if pbft_data:
            px, py = zip(*pbft_data)
            ax.plot(px, py, 's-', color=PBFT_COLOR, label="PBFT", linewidth=2, markersize=6)

        ax.set_xlabel("Load (req/s)", fontsize=12)
        ax.set_ylabel("Avg Latency (ms)", fontsize=12)
        ax.set_title("Latency vs Load: Raft vs PBFT", fontsize=14, fontweight='bold')
        ax.legend(); ax.grid(True, color=GRID_COLOR)
        plt.tight_layout()
        os.makedirs(os.path.dirname(out), exist_ok=True)
        plt.savefig(out, dpi=150); plt.close()
        print(f"Saved: {out}")


# Graph 2: throughput comparison.

def plot_throughput_comparison(results, out="analysis/graphs/throughput_comparison.png"):
    """Plot comparative Raft and PBFT throughput bars at different loads."""
    data = filter_results(results, test="throughput_vs_load")

    loads = sorted(set(r["target_rps"] for r in data))
    raft_thr = []
    pbft_thr = []

    for load in loads:
        r_vals = [r["throughput_rps"] for r in data if r["algo"] == "raft" and r["target_rps"] == load]
        p_vals = [r["throughput_rps"] for r in data if r["algo"] == "pbft" and r["target_rps"] == load]
        raft_thr.append(r_vals[0] if r_vals else 0)
        pbft_thr.append(p_vals[0] if p_vals else 0)

    if MATPLOTLIB_AVAILABLE and loads:
        fig, ax = plt.subplots(figsize=(9, 5))
        x = range(len(loads))
        w = 0.35
        bars1 = ax.bar([i - w/2 for i in x], raft_thr, w, label="Raft", color=RAFT_COLOR, alpha=0.85)
        bars2 = ax.bar([i + w/2 for i in x], pbft_thr, w, label="PBFT", color=PBFT_COLOR, alpha=0.85)

        ax.set_xticks(list(x)); ax.set_xticklabels([f"{l} rps" for l in loads])
        ax.set_xlabel("Target Load", fontsize=12)
        ax.set_ylabel("Actual Throughput (req/s)", fontsize=12)
        ax.set_title("Throughput Comparison: Raft vs PBFT", fontsize=14, fontweight='bold')
        ax.legend(); ax.grid(True, axis='y', color=GRID_COLOR)
        plt.tight_layout()
        os.makedirs(os.path.dirname(out), exist_ok=True)
        plt.savefig(out, dpi=150); plt.close()
        print(f"Saved: {out}")


# Graph 3: fault tolerance.

def plot_fault_tolerance(results, out="analysis/graphs/fault_tolerance.png"):
    """
    Show how performance degrades as failed node count increases.
    """
    data = filter_results(results, test="fault_tolerance")

    raft_data = [(r["f"], r["throughput_rps"]) for r in data if r["algo"] == "raft"]
    pbft_data = [(r["f"], r["throughput_rps"]) for r in data if r["algo"] == "pbft"]

    if MATPLOTLIB_AVAILABLE:
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

        for ax, algo_data, color, name in [
            (axes[0], raft_data, RAFT_COLOR, "Raft"),
            (axes[1], pbft_data, PBFT_COLOR, "PBFT"),
        ]:
            if algo_data:
                algo_data.sort()
                fs, thrs = zip(*algo_data)
                ax.bar(fs, thrs, color=color, alpha=0.85, width=0.5)
                # Expected theoretical degradation line.
                if len(fs) > 1:
                    expected = [thrs[0] * (1 - 0.15 * f) for f in fs]
                    ax.plot(fs, expected, '--', color='gray', label='Expected degradation')

            ax.set_title(f"{name} - Throughput vs Failures", fontsize=13, fontweight='bold')
            ax.set_xlabel("Number of Failed Nodes (f)")
            ax.set_ylabel("Throughput (req/s)")
            ax.set_xticks([0, 1, 2])
            ax.grid(True, axis='y', color=GRID_COLOR)

        plt.suptitle("Fault Tolerance Impact on Performance", fontsize=14, fontweight='bold')
        plt.tight_layout()
        os.makedirs(os.path.dirname(out), exist_ok=True)
        plt.savefig(out, dpi=150); plt.close()
        print(f"Saved: {out}")


# Graph 4: recovery time.

def plot_recovery_time(results, out="analysis/graphs/recovery_time.png"):
    """Compare Raft recovery time through re-election with PBFT view-change time."""
    data = filter_results(results, test="recovery_time")

    raft_rt = next((r["avg_latency_ms"] for r in data if r["algo"] == "raft"), 0)
    pbft_rt = next((r["avg_latency_ms"] for r in data if r["algo"] == "pbft"), 0)

    if MATPLOTLIB_AVAILABLE:
        fig, ax = plt.subplots(figsize=(7, 5))
        bars = ax.bar(
            ["Raft\n(Re-election)", "PBFT\n(View-change)"],
            [raft_rt, pbft_rt],
            color=[RAFT_COLOR, PBFT_COLOR],
            width=0.4, alpha=0.85
        )
        for bar, val in zip(bars, [raft_rt, pbft_rt]):
            ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 5,
                    f'{val:.0f}ms', ha='center', va='bottom', fontweight='bold')

        ax.set_ylabel("Recovery Time (ms)", fontsize=12)
        ax.set_title("Recovery Time After Leader/Primary Failure", fontsize=14, fontweight='bold')
        ax.grid(True, axis='y', color=GRID_COLOR)
        plt.tight_layout()
        os.makedirs(os.path.dirname(out), exist_ok=True)
        plt.savefig(out, dpi=150); plt.close()
        print(f"Saved: {out}")


# Graph 5: message complexity.

def plot_message_complexity(out="analysis/graphs/message_complexity.png"):
    """
    Theoretical comparison of message complexity.
    Raft : O(n) par heartbeat / log entry
    PBFT : O(n^2) per request (3-phase protocol)
    """
    ns = list(range(3, 20))
    raft_msgs = [2 * n for n in ns]         # Approximate n messages in each direction.
    pbft_msgs = [3 * n * n for n in ns]     # Approximate 3-phase n^2 message pattern.

    if SAGE_AVAILABLE:
        raft_points = list(zip(ns, raft_msgs))
        pbft_points = list(zip(ns, pbft_msgs))
        p = (line(raft_points, color=RAFT_COLOR, thickness=2, legend_label="Raft O(n)") +
             line(pbft_points, color=PBFT_COLOR, thickness=2, legend_label="PBFT O(n^2)"))
        p.axes_labels(["Number of nodes (n)", "Messages per consensus round"])
        p.set_legend_options(loc="upper left")
        p.save(out, title="Message Complexity: Raft vs PBFT (Theoretical)", figsize=[8, 5])

    elif MATPLOTLIB_AVAILABLE:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(ns, raft_msgs, '-o', color=RAFT_COLOR, label="Raft - O(n)", linewidth=2, markersize=4)
        ax.plot(ns, pbft_msgs, '-s', color=PBFT_COLOR, label="PBFT - O(n^2)", linewidth=2, markersize=4)
        ax.fill_between(ns, raft_msgs, pbft_msgs, alpha=0.08, color=PBFT_COLOR)

        ax.set_xlabel("Number of Nodes (n)", fontsize=12)
        ax.set_ylabel("Messages per Consensus Round", fontsize=12)
        ax.set_title("Message Complexity: Raft vs PBFT (Theoretical)", fontsize=14, fontweight='bold')
        ax.legend(); ax.grid(True, color=GRID_COLOR)
        plt.tight_layout()
        os.makedirs(os.path.dirname(out), exist_ok=True)
        plt.savefig(out, dpi=150); plt.close()
        print(f"Saved: {out}")


# Graph 6: Byzantine impact.

def plot_byzantine_impact(results, out="analysis/graphs/byzantine_impact.png"):
    """Plot the impact of Byzantine node count on PBFT throughput."""
    data = filter_results(results, test="byzantine_impact", algo="pbft")

    if not data:
        return

    byz_counts = sorted(set(r["byzantine_nodes"] for r in data))
    throughputs = [
        next((r["throughput_rps"] for r in data if r["byzantine_nodes"] == b), 0)
        for b in byz_counts
    ]
    error_rates = [
        next((r["error_rate"] * 100 for r in data if r["byzantine_nodes"] == b), 0)
        for b in byz_counts
    ]

    if MATPLOTLIB_AVAILABLE:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        ax1.bar(byz_counts, throughputs, color=PBFT_COLOR, width=0.4, alpha=0.85)
        ax1.set_xlabel("Byzantine Nodes"); ax1.set_ylabel("Throughput (req/s)")
        ax1.set_title("PBFT Throughput vs Byzantine Nodes"); ax1.grid(True, axis='y', color=GRID_COLOR)

        ax2.bar(byz_counts, error_rates, color=CHAOS_COLOR, width=0.4, alpha=0.85)
        ax2.set_xlabel("Byzantine Nodes"); ax2.set_ylabel("Error Rate (%)")
        ax2.set_title("PBFT Error Rate vs Byzantine Nodes"); ax2.grid(True, axis='y', color=GRID_COLOR)

        plt.suptitle("Byzantine Fault Impact on PBFT", fontsize=14, fontweight='bold')
        plt.tight_layout()
        os.makedirs(os.path.dirname(out), exist_ok=True)
        plt.savefig(out, dpi=150); plt.close()
        print(f"Saved: {out}")


# Main entry point.

def generate_all_graphs(results_path="analysis/results.json"):
    """Generate all graphs with one command."""
    results = load_results(results_path)
    if not results:
        print("No results found. Please run the benchmark first:")
        print("  python main.py --benchmark")
        # Generate theoretical graphs even when benchmark results are missing.
        plot_message_complexity()
        return

    os.makedirs("analysis/graphs", exist_ok=True)
    print("Generating graphs...")

    plot_latency_vs_load(results)
    plot_throughput_comparison(results)
    plot_fault_tolerance(results)
    plot_recovery_time(results)
    plot_message_complexity()
    plot_byzantine_impact(results)

    print("\nAll graphs generated in analysis/graphs/")


if __name__ == "__main__":
    generate_all_graphs()
