"""Compare pre-refactor, pre-optimization and working-tree sources symmetrically.

Each comparison with the optimized source has ABBA order after removing the
other baseline's processes. The common worker and workloads remain unchanged.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from statistics import median
import subprocess
import sys


ROOT = Path(__file__).resolve().parent
REPOSITORY = ROOT.parents[3]
ORDER = ["pre-refactor", "before-optimization", "optimized", "optimized", "before-optimization", "pre-refactor"]


def aggregate(workers):
    labels = tuple(dict.fromkeys(ORDER))
    scenarios = workers[0]["data"]["scenarios"]
    assert all(set(w["data"]["scenarios"]) == set(scenarios) for w in workers)
    for label in labels:
        assert len({w["data"]["environment"]["source_sha256_excluding_version"]
                    for w in workers if w["label"] == label}) == 1
    summary = {}
    for name, scenario in scenarios.items():
        for worker in workers:
            for field in ("workload", "capacity"):
                assert worker["data"]["scenarios"][name].get(field) == scenario.get(field)
        summary[name] = {}
        for label in labels:
            rounds = [row for w in workers if w["label"] == label
                      for row in w["data"]["scenarios"][name]["rounds"]]
            summary[name][label] = {
                **{key: median(row[key] for row in rounds) for key in rounds[0]},
                "samples": len(rounds),
                "min_seconds": min(row["seconds"] for row in rounds),
                "max_seconds": max(row["seconds"] for row in rounds),
            }
        for baseline in labels[:2]:
            before = summary[name][baseline]["seconds"]
            after = summary[name]["optimized"]["seconds"]
            summary[name][f"vs-{baseline}"] = {
                "elapsed_change_pct": (after / before - 1) * 100,
                "throughput_change_pct": (before / after - 1) * 100,
            }
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pre-refactor-source", type=Path, required=True)
    parser.add_argument("--before-source", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", type=Path, default=ROOT / "reply-optimization-comparison.json")
    args = parser.parse_args()
    if args.repeats <= 0:
        parser.error("--repeats must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    sources = {
        "pre-refactor": args.pre_refactor_source.resolve(),
        "before-optimization": args.before_source.resolve(),
        "optimized": REPOSITORY / "src",
    }
    started = datetime.now(timezone.utc).isoformat()
    workers = []
    driver = REPOSITORY / "scripts" / "benchmark_runtime.py"
    for index, label in enumerate(ORDER, 1):
        destination = args.output.parent / f"reply-optimization-{index}-{label}.json"
        print(f"Benchmark {index}/6: {label}", flush=True)
        subprocess.run([
            sys.executable, str(driver), "--worker-source", str(sources[label]),
            "--output", str(destination), "--repeats", str(args.repeats),
        ], cwd=REPOSITORY, check=True, timeout=240)
        workers.append({"label": label, "file": destination.name,
                        "data": json.loads(destination.read_text(encoding="utf-8"))})
    report = {
        "started_at": started, "completed_at": datetime.now(timezone.utc).isoformat(),
        "order": ORDER, "repeats_per_worker": args.repeats,
        "commits": {"pre-refactor": "33022f6", "before-optimization": "78684a2",
                    "optimized": "working tree based on 78684a2"},
        "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
        "summary": aggregate(workers), "workers": workers,
    }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    patch = subprocess.check_output([
        "git", "diff", "HEAD", "--", "src/apixis/core/event/base/process.py",
        "src/apixis/core/event/subscription/waiter.py",
    ], cwd=REPOSITORY, text=True)
    (args.output.parent / "reply-optimization.patch").write_text(patch, encoding="utf-8")
    print(f"Saved {args.output}", flush=True)


if __name__ == "__main__":
    main()
