import sys
import os
import asyncio
import random
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np

# =================================================================
# DESIGN SYSTEM - DARK DASHBOARD
# =================================================================
BG_MAIN          = "#0B0F19"     # Main dark background
PANEL_BG         = "#131A26"     # Panel background
BORDER_COLOR     = "#1F293D"     # Subtle borders
TEXT_MAIN        = "#F1F5F9"     # Primary text
TEXT_MUTED       = "#64748B"     # Secondary text and labels

ACCENT_RAFT      = "#38BDF8"     # Raft accent color
ACCENT_PBFT      = "#F43F5E"     # PBFT accent color
GRID_COLOR       = "#1E293B"

CONSOLE_BG       = "#070A10"
CONSOLE_FG       = "#34D399"     # Console success log text

FONT_FAMILY      = "Segoe UI"
FONT_MONO        = "Consolas"

class RealTimeBenchmarkGUI:

    def __init__(self, root):
        self.root = root
        self.root.title("Consensus Engine Spectrum | Live Parallel Simulation")
        self.root.geometry("1300x820")
        self.root.configure(bg=BG_MAIN)

        self.scenarios = ["LAN (local)", "WAN (microgrid)", "Cyber attack"]
        
        # Start with empty metrics.
        self._clear_results()
        self.running = False

        self._setup_styles()
        self._build_interface()
        
        # Create fixed plot slots once at startup.
        self._init_matplotlib_plots()
        self._draw_plots()  # Initial empty render.

    def _clear_results(self):
        self.results = {
            sc: {
                "RAFT": {"commits": 0, "tps": 0.0, "latency_ms": 0, "msgs": 0},
                "PBFT": {"commits": 0, "tps": 0.0, "latency_ms": 0, "msgs": 0}
            } for sc in self.scenarios
        }

    def _setup_styles(self):
        # Global Matplotlib configuration.
        plt.rcParams.update({
            "font.family": "sans-serif",
            "font.sans-serif": [FONT_FAMILY, "Arial"],
            "axes.edgecolor": BORDER_COLOR,
            "axes.facecolor": PANEL_BG,
            "figure.facecolor": PANEL_BG,
            "xtick.color": TEXT_MUTED,
            "ytick.color": TEXT_MUTED,
            "grid.color": GRID_COLOR,
            "text.color": TEXT_MAIN
        })

        # TTK style configuration for native widgets.
        s = ttk.Style()
        s.theme_use("clam")
        s.configure(".", background=BG_MAIN, foreground=TEXT_MAIN)
        s.configure("TFrame", background=BG_MAIN)
        s.configure("Horizontal.TProgressbar", thickness=6, bordercolor=BG_MAIN, troughcolor=CONSOLE_BG, background=ACCENT_RAFT)

    def _build_interface(self):
        # ---- Top Header ----
        header = tk.Frame(self.root, bg=PANEL_BG, height=65, bd=0, highlightbackground=BORDER_COLOR, highlightthickness=1)
        header.pack(fill=tk.X, side=tk.TOP)
        header.pack_propagate(False)

        tk.Label(header, text="CONSENSUS BENCHMARK", bg=PANEL_BG, fg=TEXT_MAIN, font=(FONT_FAMILY, 14, "bold")).pack(side=tk.LEFT, padx=25, pady=18)
        
        self.status_label = tk.Label(header, text="ENGINE STATUS: IDLE", bg=PANEL_BG, fg=ACCENT_RAFT, font=(FONT_FAMILY, 9, "bold"))
        self.status_label.pack(side=tk.LEFT, padx=10, pady=22)

        # Main Layout split
        wrapper = tk.Frame(self.root, bg=BG_MAIN)
        wrapper.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # Left controls panel
        ctrl_panel = tk.Frame(wrapper, bg=PANEL_BG, width=320, highlightbackground=BORDER_COLOR, highlightthickness=1)
        ctrl_panel.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 20))
        ctrl_panel.pack_propagate(False)

        # Right visualization area
        self.chart_frame = tk.Frame(wrapper, bg=PANEL_BG, highlightbackground=BORDER_COLOR, highlightthickness=1)
        self.chart_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._populate_controls(ctrl_panel)

    def _populate_controls(self, container):
        p = tk.Frame(container, bg=PANEL_BG, padx=18, pady=18)
        p.pack(fill=tk.BOTH, expand=True)

        # Configuration controls.
        self._add_section_title(p, "CONFIGURATION CONTROLS")

        tk.Label(p, text="Select Scenario", bg=PANEL_BG, fg=TEXT_MUTED, font=(FONT_FAMILY, 9)).pack(anchor=tk.W, pady=(10, 2))
        self.scenario_var = tk.StringVar(value="Run all scenarios")
        combo = ttk.Combobox(p, textvariable=self.scenario_var, values=self.scenarios + ["Run all scenarios"], state="readonly")
        combo.pack(fill=tk.X, pady=(0, 15))

        # Duration slider.
        tk.Label(p, text="Simulation Steps (Seconds)", bg=PANEL_BG, fg=TEXT_MUTED, font=(FONT_FAMILY, 9)).pack(anchor=tk.W)
        row_dur = tk.Frame(p, bg=PANEL_BG)
        row_dur.pack(fill=tk.X, pady=(2, 15))
        self.duration_var = tk.IntVar(value=10)
        self.lbl_dur = tk.Label(row_dur, text="10s", bg=PANEL_BG, fg=TEXT_MAIN, font=(FONT_FAMILY, 9, "bold"))
        self.lbl_dur.pack(side=tk.RIGHT)
        ttk.Scale(row_dur, from_=5, to=30, orient=tk.HORIZONTAL, variable=self.duration_var,
                  command=lambda v: self.lbl_dur.config(text=f"{int(float(v))}s")).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Node-count slider.
        tk.Label(p, text="Network Nodes Cluster (n)", bg=PANEL_BG, fg=TEXT_MUTED, font=(FONT_FAMILY, 9)).pack(anchor=tk.W)
        row_nodes = tk.Frame(p, bg=PANEL_BG)
        row_nodes.pack(fill=tk.X, pady=(2, 20))
        self.nodes_var = tk.IntVar(value=4)
        self.lbl_nodes = tk.Label(row_nodes, text="4", bg=PANEL_BG, fg=TEXT_MAIN, font=(FONT_FAMILY, 9, "bold"))
        self.lbl_nodes.pack(side=tk.RIGHT)
        ttk.Scale(row_nodes, from_=4, to=16, orient=tk.HORIZONTAL, variable=self.nodes_var,
                  command=lambda v: self.lbl_nodes.config(text=str(int(float(v))))).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Progress indicator.
        self.progress = ttk.Progressbar(p, orient="horizontal", mode="determinate")
        self.progress.pack(fill=tk.X, pady=(10, 8))

        # Simulation launch button.
        self.run_btn = tk.Button(
            p, text="START SIMULTANEOUS EXECUTION", bg=ACCENT_RAFT, fg=BG_MAIN,
            font=(FONT_FAMILY, 10, "bold"), relief="flat", bd=0, cursor="hand2",
            activebackground="#22D3EE", activeforeground=BG_MAIN, padx=0, pady=10,
            command=self._launch_simulation
        )
        self.run_btn.pack(fill=tk.X, pady=(0, 20))

        # Live telemetry section.
        self._add_section_title(p, "MONITORING PROTOCOLS")
        
        for color, name, complexity in [(ACCENT_RAFT, "RAFT (CFT)", "O(n) Message density"), (ACCENT_PBFT, "PBFT (BFT)", "O(n^2) Message density")]:
            r = tk.Frame(p, bg=PANEL_BG, pady=5)
            r.pack(fill=tk.X)
            indicator = tk.Frame(r, bg=color, width=4, height=16)
            indicator.pack(side=tk.LEFT, padx=(0, 10))
            tk.Label(r, text=name, bg=PANEL_BG, fg=TEXT_MAIN, font=(FONT_FAMILY, 9, "bold")).pack(side=tk.LEFT)
            tk.Label(r, text=f"  -  {complexity}", bg=PANEL_BG, fg=TEXT_MUTED, font=(FONT_FAMILY, 8, "italic")).pack(side=tk.LEFT)

        # Integrated terminal log.
        tk.Label(p, text="LIVE ENGINE TERMINAL", bg=PANEL_BG, fg=TEXT_MUTED, font=(FONT_FAMILY, 8, "bold")).pack(anchor=tk.W, pady=(20, 4))
        self.log_box = tk.Text(p, height=8, wrap=tk.WORD, font=(FONT_MONO, 8), bg=CONSOLE_BG, fg=CONSOLE_FG, bd=0, highlightbackground=BORDER_COLOR, highlightthickness=1, padx=8, pady=8)
        self.log_box.pack(fill=tk.BOTH, expand=True)

    def _add_section_title(self, parent, text):
        lbl = tk.Label(parent, text=text, bg=PANEL_BG, fg=TEXT_MAIN, font=(FONT_FAMILY, 8, "bold"))
        lbl.pack(anchor=tk.W, pady=(5, 2))
        sep = tk.Frame(parent, bg=BORDER_COLOR, height=1)
        sep.pack(fill=tk.X, pady=(2, 10))

    def _log(self, text, is_system=False):
        ts = time.strftime("%H:%M:%S")
        prefix = ">> " if is_system else "   "
        self.log_box.insert(tk.END, f"[{ts}] {prefix}{text}\n")
        self.log_box.see(tk.END)

    # =================================================================
    # MATPLOTLIB INITIALIZATION
    # =================================================================
    def _init_matplotlib_plots(self):
        # Create the shared figure once.
        self.fig = plt.figure(figsize=(10.5, 7.0))
        self.fig.patch.set_facecolor(PANEL_BG)

        # Use a fixed grid so plot positions remain stable.
        self.gs = gridspec.GridSpec(2, 2, left=0.07, right=0.96, top=0.93, bottom=0.12, hspace=0.38, wspace=0.24)
        self.axes = [self.fig.add_subplot(self.gs[row, col]) for row in range(2) for col in range(2)]

        # Attach the canvas to the Tkinter frame.
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    # =================================================================
    # SIMULATION ENGINE
    # =================================================================
    def _launch_simulation(self):
        if self.running: return
        self.running = True
        self.run_btn.config(state=tk.DISABLED, bg=BORDER_COLOR)
        self.status_label.config(text="ENGINE STATUS: RUNNING METRICS", fg=ACCENT_PBFT)

        # Reset metrics before each run.
        self._clear_results()
        self._draw_plots()

        scen_selection = self.scenario_var.get()
        scenarios_to_process = self.scenarios if scen_selection == "Run all scenarios" else [scen_selection]
        
        duration = self.duration_var.get()
        n_nodes = self.nodes_var.get()

        threading.Thread(
            target=self._execution_loop, 
            args=(scenarios_to_process, duration, n_nodes), 
            daemon=True
        ).start()

    def _execution_loop(self, targeted_scenarios, duration, n_nodes):
        self._log("Bootstrapping network verification layers...", is_system=True)
        
        total_ticks = len(targeted_scenarios) * duration
        global_tick_counter = 0

        raft_perf_modifier = 5.0 / n_nodes
        pbft_perf_modifier = (4.0 / n_nodes) ** 2

        for sc in targeted_scenarios:
            self._log(f"Spawning parallel clients on environment: [{sc}]")

            if sc == "LAN (local)":
                r_base_tps, p_base_tps = 150.0, 90.0
                r_lat, p_lat = 25, 95
            elif sc == "WAN (microgrid)":
                r_base_tps, p_base_tps = 85.0, 30.0
                r_lat, p_lat = 75, 310
            else: 
                r_base_tps, p_base_tps = 120.0, 45.0
                r_lat, p_lat = 40, 190

            for tick in range(1, duration + 1):
                time.sleep(0.5)  # Keep the demo responsive while preserving visible updates.
                
                network_noise = random.uniform(0.92, 1.08)
                
                current_raft_tps = round(r_base_tps * raft_perf_modifier * network_noise, 1)
                current_pbft_tps = round(p_base_tps * pbft_perf_modifier * network_noise, 1)

                raft_accumulated_commits = int(current_raft_tps * tick)
                pbft_accumulated_commits = int(current_pbft_tps * tick)

                raft_msgs = 2 * (n_nodes - 1)
                pbft_msgs = 2 * n_nodes * (n_nodes - 1)

                # Update both protocol metrics for the current tick.
                self.results[sc]["RAFT"] = {
                    "commits": raft_accumulated_commits,
                    "tps": current_raft_tps,
                    "latency_ms": int(r_lat * (1 / raft_perf_modifier) * random.uniform(0.9, 1.1)),
                    "msgs": raft_msgs
                }
                
                self.results[sc]["PBFT"] = {
                    "commits": pbft_accumulated_commits,
                    "tps": current_pbft_tps,
                    "latency_ms": int(p_lat * (1 / pbft_perf_modifier) * random.uniform(0.9, 1.1)),
                    "msgs": pbft_msgs
                }

                global_tick_counter += 1
                prog_percent = int((global_tick_counter / total_ticks) * 100)
                
                self.root.after(0, lambda p=prog_percent: self.progress.configure(value=p))
                self.root.after(0, self._draw_plots)

            self._log(f"Scenario [{sc}] transaction round finished.", is_system=True)

        self.root.after(0, self._finalize_simulation)

    def _finalize_simulation(self):
        self.running = False
        self.run_btn.config(state=tk.NORMAL, bg=ACCENT_RAFT)
        self.status_label.config(text="ENGINE STATUS: IDLE", fg=ACCENT_RAFT)
        self.progress.configure(value=0)
        self._log("All multi-threaded iterations successfully compiled.", is_system=True)

    # =================================================================
    # STABLE PLOT RENDERING
    # =================================================================
    def _draw_plots(self):
        categories = self.scenarios
        x_indices = np.arange(len(categories))
        bar_width = 0.28

        # Collect live metrics.
        r_commits = [self.results[c]["RAFT"]["commits"] for c in categories]
        p_commits = [self.results[c]["PBFT"]["commits"] for c in categories]
        
        r_tps     = [self.results[c]["RAFT"]["tps"] for c in categories]
        p_tps     = [self.results[c]["PBFT"]["tps"] for c in categories]
        
        r_lat     = [self.results[c]["RAFT"]["latency_ms"] for c in categories]
        p_lat     = [self.results[c]["PBFT"]["latency_ms"] for c in categories]
        
        r_msgs    = [self.results[c]["RAFT"]["msgs"] for c in categories]
        p_msgs    = [self.results[c]["PBFT"]["msgs"] for c in categories]

        plots_mapping = [
            ("TRANSACTION COMMITS (GROWTH)", r_commits, p_commits, "Total Commits Count", False),
            ("CONCURRENT THROUGHPUT", r_tps, p_tps, "Transactions / Sec (TPS)", False),
            ("PROPAGATION LATENCY", r_lat, p_lat, "Time Delay (ms)", True),
            ("ALGORITHMIC MESSAGE STRUCTURE", r_msgs, p_msgs, "Messages / Consensus Round", False)
        ]

        # Reuse the fixed axes for each refresh.
        for ax, (title, raft_data, pbft_data, y_lbl, invert_label) in zip(self.axes, plots_mapping):
            ax.clear()  # Clear the plot while keeping its fixed layout slot.
            
            bar_raft = ax.bar(x_indices - bar_width/2, raft_data, bar_width, label="RAFT", color=ACCENT_RAFT, alpha=0.9)
            bar_pbft = ax.bar(x_indices + bar_width/2, pbft_data, bar_width, label="PBFT", color=ACCENT_PBFT, alpha=0.9)

            ax.set_title(title, loc="left", pad=10, fontsize=9, color=TEXT_MAIN, weight="bold")
            ax.set_ylabel(y_lbl, labelpad=5, color=TEXT_MUTED, fontsize=8)
            ax.set_xticks(x_indices)
            ax.set_xticklabels(categories, fontsize=8, color=TEXT_MUTED)
            
            ax.yaxis.grid(True, linestyle="--", alpha=0.1)
            ax.set_axisbelow(True)

            # Apply consistent border styling.
            for name, spine in ax.spines.items():
                if name in ["top", "right", "left"]:
                    spine.set_visible(False)
                else:
                    spine.set_color(BORDER_COLOR)

            # Keep y-axis limits stable while adapting to current values.
            max_val = max(max(raft_data), max(pbft_data))
            ax.set_ylim(0, 10 if max_val == 0 else max_val * 1.15)

            # Display live values above active bars.
            for rect, val in list(zip(bar_raft, raft_data)) + list(zip(bar_pbft, pbft_data)):
                if val == 0: continue
                val_str = f"{val}" if isinstance(val, int) or val == int(val) else f"{val:.1f}"
                ax.text(rect.get_x() + rect.get_width()/2, rect.get_height() + (ax.get_ylim()[1] * 0.015),
                        val_str, ha="center", va="bottom", fontsize=7, color=TEXT_MAIN, weight="semibold")

            if invert_label:
                ax.text(0.98, 0.93, "Lower is optimal", transform=ax.transAxes, ha="right", va="top",
                        fontsize=7, style="italic", color=TEXT_MUTED)

        # Rebuild the global legend.
        self.fig.legends = []
        proxies = [
            plt.Rectangle((0,0), 1, 1, facecolor=ACCENT_RAFT),
            plt.Rectangle((0,0), 1, 1, facecolor=ACCENT_PBFT)
        ]
        self.fig.legend(proxies, ["RAFT Cluster Protocol (Crash Fault Tolerant Framework)", "PBFT Byzantine Network Matrix (Malicious Peer Defense)"],
                   loc="lower center", ncol=2, bbox_to_anchor=(0.5, 0.02),
                   fontsize=8.5, handlelength=1.0, frameon=False, labelcolor=TEXT_MAIN)

        # Refresh the existing canvas.
        self.canvas.draw()

# =================================================================
# ENTRYPOINT ENTRY ROUTER
# =================================================================
if __name__ == "__main__":
    root = tk.Tk()
    app = RealTimeBenchmarkGUI(root)
    root.mainloop()
