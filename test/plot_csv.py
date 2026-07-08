import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import glob
import os

# Try to find a CJK font for Chinese CSV headers
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
    
    try:
        df = pd.read_csv(f)
    except Exception as e:
        print(f"  -> skip {f}: {e}")
        continue
    
    if df.empty:
        print(f"  -> skip {f}: empty file")
        continue
    
    print(f"\n=== {name} === ({len(df)} rows)")
    
    # Define metrics to plot (each in its own subplot)
    metrics = [
        ('heap_alloc_mb', 'Heap Alloc (MB)'),
        ('total_alloc_mb', 'Total Alloc (MB)'),
        ('sys_mb', 'System Memory (MB)'),
        ('workers_running', 'Workers Running'),
        ('workers_idle', 'Workers Idle'),
        ('workers_created', 'Workers Created'),
        ('gc_pause_total_ms', 'GC Pause Total (ms)'),
        ('gc_pause_avg_ms', 'GC Pause Avg (ms)'),
        ('gc_cpu_pct', 'GC CPU (%)'),
        ('cpu_pct', 'CPU (%)'),
        ('gc_total', 'GC Total Count'),
        ('goroutines', 'Goroutines'),
        ('task_queue_len', 'Task Queue Length')
    ]
    
    # Filter existing metrics
    existing_metrics = [(col, title) for col, title in metrics if col in df.columns]
    
    if not existing_metrics:
        print(f"  -> skip {f}: no valid metric columns")
        continue
    
    # Calculate subplot layout (max 3 columns)
    n = len(existing_metrics)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    
    fig, axes = plt.subplots(nrows, ncols, figsize=(5*ncols, 4*nrows))
    fig.suptitle(name, fontsize=16, fontweight='bold')
    
    if n == 1:
        axes = [axes]
    else:
        axes = axes.flatten()
    
    # Plot each metric
    for idx, (col, title) in enumerate(existing_metrics):
        ax = axes[idx]
        df.plot(x="run_sec", y=[col], ax=ax, marker=".", linewidth=1.5, legend=False)
        ax.set_title(title, fontsize=12)
        ax.set_xlabel("Time (seconds)")
        ax.set_ylabel(title)
        ax.grid(True, alpha=0.3)
        
        # Annotate last value
        last_val = df[col].iloc[-1]
        last_time = df['run_sec'].iloc[-1]
        ax.annotate(f'{last_val:.2f}', 
                   xy=(last_time, last_val),
                   xytext=(5, 5), 
                   textcoords='offset points',
                   fontsize=8,
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.3))
    
    # Hide unused subplots
    for idx in range(len(existing_metrics), len(axes)):
        axes[idx].axis('off')
    
    plt.tight_layout()
    
    # Save as SVG (vector format for infinite zoom)
    svg_file = f"{name}.svg"
    plt.savefig(svg_file, format='svg')
    plt.close()
    print(f"  -> {svg_file}")