import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import glob
import os

# Try to find a CJK font for Chinese CSV headers (if any)
_cjk_fonts = ["Microsoft YaHei", "SimHei", "WenQuanYi Micro Hei", "Noto Sans CJK SC"]
for _f in _cjk_fonts:
    try:
        fm.findfont(_f, fallback_to_default=False)
        plt.rcParams["font.sans-serif"] = [_f]
        plt.rcParams["axes.unicode_minus"] = False
        break
    except Exception:
        continue

csv_files = sorted(glob.glob("metrics_*.csv"))
if not csv_files:
    print("no metrics_*.csv files found")
    exit()

for f in csv_files:
    name = os.path.splitext(f)[0]
    df = pd.read_csv(f)
    print(f"\n=== {name} === ({len(df)} rows)")

    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    fig.suptitle(name, fontsize=14)

    # Memory
    ax = axes[0, 0]
    df.plot(x="run_sec", y=["heap_alloc_mb", "total_alloc_mb", "sys_mb"],
            ax=ax, marker=".")
    ax.set_title("Memory (MB)")
    ax.set_xlabel("sec")

    # Workers
    ax = axes[0, 1]
    df.plot(x="run_sec", y=["workers_running", "workers_idle", "workers_created"],
            ax=ax, marker=".")
    ax.set_title("Workers")
    ax.set_xlabel("sec")

    # GC pause
    ax = axes[0, 2]
    df.plot(x="run_sec", y=["gc_pause_total_ms", "gc_pause_avg_ms"],
            ax=ax, marker=".")
    ax.set_title("GC Pause (ms)")
    ax.set_xlabel("sec")

    # CPU
    ax = axes[1, 0]
    df.plot(x="run_sec", y=["gc_cpu_pct", "cpu_pct"],
            ax=ax, marker=".")
    ax.set_title("CPU (%)")
    ax.set_xlabel("sec")

    # GC count
    ax = axes[1, 1]
    df.plot(x="run_sec", y=["gc_total"],
            ax=ax, marker=".", legend=False)
    ax.set_title("GC Total")
    ax.set_xlabel("sec")
    ax.legend(["gc_total"])

    # Goroutines + queue
    ax = axes[1, 2]
    df.plot(x="run_sec", y=["goroutines", "task_queue_len"],
            ax=ax, marker=".")
    ax.set_title("Goroutines & Queue")
    ax.set_xlabel("sec")

    plt.tight_layout()
    plt.savefig(f"{name}.png", dpi=150)
    plt.close()
    print(f"  -> {name}.png")
