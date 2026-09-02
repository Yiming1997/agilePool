"""Generate comparison charts for the strict Hook benchmark runs."""

import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results-strict"
BASELINE = ROOT / "main基准"
OUTPUT = ROOT / "hook-comparison"


def read_samples(path):
    rows = []
    with path.open(encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            if not row.get("run_sec"):
                continue
            rows.append({key: float(value) for key, value in row.items()
                         if key not in {"hook_mode"}})
    return rows


def max_value(rows, key):
    return max((row[key] for row in rows), default=0.0)


def last_value(rows, key):
    return rows[-1][key] if rows else 0.0


def parse_elapsed(log_path):
    current_mode = None
    current_name = None
    values = {}
    pattern = re.compile(r"^Elapsed:\s+(?:(\d+)m\s*)?([0-9]+(?:\.[0-9]+)?)s$")
    with log_path.open(encoding="utf-8", errors="replace") as stream:
        for line in stream:
            marker = re.match(r"^\[(none|hook|trace)\] (\S+)", line)
            if marker:
                current_mode, current_name = marker.groups()
                continue
            elapsed = pattern.match(line.strip())
            if elapsed and current_mode and current_name:
                minutes = float(elapsed.group(1) or 0)
                values[(current_mode, current_name)] = minutes * 60 + float(elapsed.group(2))
    return values


def baseline_file(name):
    match = {
        "fixed_100w_20k": "metrics_fixed_w100_t20000_linkedlist.csv",
        "fixed_500w_50k": "metrics_fixed_w500_t50000_linkedlist.csv",
        "fixed_2kw_100k": "metrics_fixed_w2000_t100000_linkedlist.csv",
        "fixed_10kw_200k": "metrics_fixed_w10000_t200000_linkedlist.csv",
        "fixed_immediate_500k": "metrics_fixed_w20000_t500000_linkedlist.csv",
        "normal_immediate_500k": "metrics_normal_w20000_t500000_linkedlist.csv",
        "uniform_immediate_500k": "metrics_uniform_w20000_t500000_linkedlist.csv",
    }
    return BASELINE / match[name] if name in match else None


def main():
    OUTPUT.mkdir(exist_ok=True)
    elapsed = parse_elapsed(ROOT / "run_test_hook_all_strict.log")
    names = sorted({path.stem for mode in ("none", "hook", "trace")
                    for path in (RESULTS / mode).glob("*.csv")})
    records = []

    for name in names:
        paths = {mode: RESULTS / mode / f"{name}.csv" for mode in ("none", "hook", "trace")}
        if not all(path.exists() for path in paths.values()):
            continue
        samples = {mode: read_samples(path) for mode, path in paths.items()}
        record = {"name": name}
        for mode in ("none", "hook", "trace"):
            record[f"{mode}_elapsed_s"] = elapsed.get((mode, name), 0.0)
            record[f"{mode}_max_heap_mb"] = max_value(samples[mode], "heap_alloc_mb")
            record[f"{mode}_last_heap_mb"] = last_value(samples[mode], "heap_alloc_mb")
            record[f"{mode}_max_sys_mb"] = max_value(samples[mode], "sys_mb")
            record[f"{mode}_last_total_alloc_mb"] = last_value(samples[mode], "total_alloc_mb")
            record[f"{mode}_max_gc"] = max_value(samples[mode], "gc_total")
        records.append(record)

    baseline_rows = []
    for row in records:
        path = baseline_file(row["name"])
        if not path or not path.exists():
            continue
        base = read_samples(path)
        if not base:
            continue
        base_heap = max_value(base, "heap_alloc_mb")
        base_sys = max_value(base, "sys_mb")
        base_total = last_value(base, "total_alloc_mb")
        baseline_rows.append({
            "name": row["name"],
            "baseline_max_heap_mb": base_heap,
            "none_max_heap_mb": row["none_max_heap_mb"],
            "none_heap_change_pct": 100 * (row["none_max_heap_mb"] - base_heap) / base_heap,
            "baseline_max_sys_mb": base_sys,
            "none_max_sys_mb": row["none_max_sys_mb"],
            "none_sys_change_pct": 100 * (row["none_max_sys_mb"] - base_sys) / base_sys,
            "baseline_total_alloc_mb": base_total,
            "none_total_alloc_mb": row["none_last_total_alloc_mb"],
            "none_total_change_pct": 100 * (row["none_last_total_alloc_mb"] - base_total) / base_total,
        })

    baseline_by_name = {row["name"]: row for row in baseline_rows}
    summary = []
    for row in records:
        baseline = baseline_by_name.get(row["name"], {})
        summary_row = {"name": row["name"]}
        for mode in ("none", "hook", "trace"):
            summary_row[f"{mode}_elapsed_s"] = row[f"{mode}_elapsed_s"]
            summary_row[f"{mode}_max_heap_mb"] = row[f"{mode}_max_heap_mb"]
            summary_row[f"{mode}_max_sys_mb"] = row[f"{mode}_max_sys_mb"]
            summary_row[f"{mode}_total_alloc_mb"] = row[f"{mode}_last_total_alloc_mb"]
            summary_row[f"{mode}_max_gc"] = row[f"{mode}_max_gc"]

        def change(left, right):
            return round(100 * (right - left) / left, 4) if left else ""

        summary_row["hook_elapsed_change_vs_none_pct"] = change(
            row["none_elapsed_s"], row["hook_elapsed_s"])
        summary_row["trace_elapsed_change_vs_none_pct"] = change(
            row["none_elapsed_s"], row["trace_elapsed_s"])
        summary_row["hook_heap_change_vs_none_pct"] = change(
            row["none_max_heap_mb"], row["hook_max_heap_mb"])
        summary_row["trace_heap_change_vs_none_pct"] = change(
            row["none_max_heap_mb"], row["trace_max_heap_mb"])
        summary_row["hook_sys_change_vs_none_pct"] = change(
            row["none_max_sys_mb"], row["hook_max_sys_mb"])
        summary_row["trace_sys_change_vs_none_pct"] = change(
            row["none_max_sys_mb"], row["trace_max_sys_mb"])
        summary_row["hook_total_alloc_change_vs_none_pct"] = change(
            row["none_last_total_alloc_mb"], row["hook_last_total_alloc_mb"])
        summary_row["trace_total_alloc_change_vs_none_pct"] = change(
            row["none_last_total_alloc_mb"], row["trace_last_total_alloc_mb"])

        summary_row.update(baseline)
        summary_row["none_elapsed_change_vs_baseline_pct"] = ""
        if baseline.get("baseline_max_heap_mb"):
            summary_row["none_heap_change_vs_baseline_pct"] = round(
                100 * (row["none_max_heap_mb"] - baseline["baseline_max_heap_mb"])
                / baseline["baseline_max_heap_mb"], 4)
            summary_row["none_sys_change_vs_baseline_pct"] = round(
                100 * (row["none_max_sys_mb"] - baseline["baseline_max_sys_mb"])
                / baseline["baseline_max_sys_mb"], 4)
            summary_row["none_total_alloc_change_vs_baseline_pct"] = round(
                100 * (row["none_last_total_alloc_mb"] - baseline["baseline_total_alloc_mb"])
                / baseline["baseline_total_alloc_mb"], 4)
        else:
            summary_row["none_heap_change_vs_baseline_pct"] = ""
            summary_row["none_sys_change_vs_baseline_pct"] = ""
            summary_row["none_total_alloc_change_vs_baseline_pct"] = ""
        summary.append(summary_row)

    fieldnames = list(summary[0].keys())
    with (OUTPUT / "comparison_summary.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary)

    groups = {
        "all": lambda name: True,
        "fixed": lambda name: name.startswith("fixed_"),
        "uniform": lambda name: name.startswith("uniform_") or name == "uniform_json",
        "normal": lambda name: name.startswith("normal_"),
        "immediate": lambda name: "immediate" in name,
        "phased": lambda name: name.startswith("phased_"),
        "nonblock": lambda name: "nonblock" in name,
        "short_task": lambda name: name in {"fixed_1ms_1m", "fixed_10kw_100ms"},
        "long_task": lambda name: name in {"fixed_10kw_2s", "fixed_3s_5k"},
    }

    def average(rows, key):
        values = [float(row[key]) for row in rows if row[key] not in ("", None)]
        return sum(values) / len(values) if values else ""

    def median(rows, key):
        values = sorted(float(row[key]) for row in rows if row[key] not in ("", None))
        if not values:
            return ""
        middle = len(values) // 2
        return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2

    def pct(left, right):
        return 100 * (right - left) / left if left else ""

    aggregate = []
    for group, selector in groups.items():
        selected = [row for row in records if selector(row["name"])]
        if not selected:
            continue
        row = {"group": group, "case_count": len(selected)}
        for mode in ("none", "hook", "trace"):
            for metric in ("elapsed_s", "max_heap_mb", "max_sys_mb", "last_total_alloc_mb"):
                key = f"{mode}_{metric}"
                row[f"{mode}_{metric}_avg"] = average(selected, key)
                row[f"{mode}_{metric}_median"] = median(selected, key)
        for metric in ("elapsed_s", "max_heap_mb", "max_sys_mb", "last_total_alloc_mb"):
            for mode in ("hook", "trace"):
                row[f"{mode}_{metric}_change_vs_none_pct"] = pct(
                    row[f"none_{metric}_avg"], row[f"{mode}_{metric}_avg"])
        aggregate.append(row)

    with (OUTPUT / "aggregate_summary.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=aggregate[0].keys())
        writer.writeheader()
        writer.writerows(aggregate)

    outliers = []
    for row in records:
        hook_change = pct(row["none_elapsed_s"], row["hook_elapsed_s"])
        trace_change = pct(row["none_elapsed_s"], row["trace_elapsed_s"])
        if abs(hook_change) >= 2 or abs(trace_change) >= 5:
            outliers.append({
                "name": row["name"],
                "none_elapsed_s": row["none_elapsed_s"],
                "hook_elapsed_s": row["hook_elapsed_s"],
                "hook_change_pct": hook_change,
                "trace_elapsed_s": row["trace_elapsed_s"],
                "trace_change_pct": trace_change,
                "none_max_heap_mb": row["none_max_heap_mb"],
                "trace_max_heap_mb": row["trace_max_heap_mb"],
                "none_total_alloc_mb": row["none_last_total_alloc_mb"],
                "trace_total_alloc_mb": row["trace_last_total_alloc_mb"],
            })
    with (OUTPUT / "outliers.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=outliers[0].keys())
        writer.writeheader()
        writer.writerows(outliers)

    print(f"wrote {len(summary)} rows to {OUTPUT / 'comparison_summary.csv'}")
    print(f"wrote {len(aggregate)} aggregate rows and {len(outliers)} outlier rows")


if __name__ == "__main__":
    main()
