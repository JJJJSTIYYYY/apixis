"""Render this run's measurements with an explicitly selected Chinese font.

Requires matplotlib. Run with --font /path/to/a/Chinese/font.ttf when the
listed font families are unavailable. SVG text is embedded as glyph paths.
"""

import argparse
import json
from pathlib import Path
from statistics import median
import warnings

import matplotlib

matplotlib.use("Agg")

from matplotlib import font_manager, pyplot as plt  # noqa: E402
from matplotlib.ft2font import FT2Font  # noqa: E402


ROOT = Path(__file__).resolve().parent
COLORS = ["#2563eb", "#ea580c", "#059669", "#9333ea", "#475569"]
GRAPH_LABELS = {
    ("independent", "chain", "immediate"): "独立图·串行",
    ("independent", "parallel", "immediate"): "独立图·并行",
    ("shared", "chain", "immediate"): "共享图·串行",
    ("shared", "parallel", "immediate"): "共享图·并行",
    ("shared", "chain", "sleep-1ms"): "共享串行·等待 1 ms",
}
def configure_font(path):
    if path is None:
        for family in ("PingFang SC", "Heiti SC", "Microsoft YaHei", "Noto Sans CJK SC", "Arial Unicode MS"):
            try:
                path = font_manager.findfont(family, fallback_to_default=False)
                break
            except ValueError:
                continue
        else:
            raise RuntimeError("No Chinese font found; pass --font explicitly.")
    font_manager.fontManager.addfont(str(path))
    family = font_manager.FontProperties(fname=path).get_name()
    plt.rcParams.update({
        "font.family": family,
        "axes.unicode_minus": False,
        "svg.fonttype": "path",
        "font.size": 11,
        "axes.spines.top": False,
        "axes.spines.right": False,
    })
    # Fail instead of publishing a chart with missing Chinese glyphs.
    vocabulary = "当前版本性能测量图调度正式轮次耗时秒背压容量千节点吞吐越高低好无限制跨线程工作量不同单次预热并发调用三轮中位数"
    vocabulary += "".join(GRAPH_LABELS.values())
    charmap = FT2Font(str(path)).get_charmap()
    missing = {c for c in vocabulary if ord(c) not in charmap and not c.isspace()}
    if missing:
        raise RuntimeError(f"Font {family} lacks glyphs: {''.join(sorted(missing))}")
    return family


def style(axis, xlabel, ylabel):
    axis.set_xlabel(xlabel)
    axis.set_ylabel(ylabel)
    axis.grid(alpha=0.2)
    axis.set_ylim(bottom=0)


def save(figure, name, directory=ROOT):
    for suffix in ("png", "svg"):
        figure.savefig(directory / f"{name}.{suffix}", dpi=180, facecolor="white")
    plt.close(figure)


def graph_chart(reports):
    figure, axes = plt.subplots(2, 2, figsize=(14, 10), layout="constrained")
    figure.suptitle("图调度性能 · 当前版本 78684a2", fontsize=18)
    for axis, count, nodes in zip(axes[0], (1000, 3000), (16, 32)):
        for (key, label), color in zip(GRAPH_LABELS.items(), COLORS):
            report = next(r for r in reports if r["invocations"] == count and
                          (r["ownership"], r["topology"], r["workload"]) == key)
            axis.plot([1, 2, 3], [r["total_seconds"] for r in report["rounds"]],
                      "o-", color=color, linewidth=2, label=label)
        axis.set_title(f"{count:,} 次并发调用 × {nodes} 节点／次")
        axis.set_xticks([1, 2, 3])
        style(axis, "正式测量轮次", "含建图耗时（秒，越低越好）")
        axis.legend(fontsize=9)

    sweep = sorted((r for r in reports if r["workload"] == "sleep-50ms"),
                   key=lambda r: r["event_loop_backpressure"])
    capacities = [r["event_loop_backpressure"] for r in sweep]
    for axis, metric, divisor, ylabel in (
        (axes[1, 0], "total_seconds", 1, "含建图耗时中位数（秒，越低越好）"),
        (axes[1, 1], "nodes_per_second", 1000, "吞吐中位数（千节点／秒，越高越好）"),
    ):
        values = [median(round_[metric] for round_ in r["rounds"]) / divisor for r in sweep]
        axis.plot(capacities, values, "o-", color=COLORS[0], linewidth=2)
        for x, y in zip(capacities, values):
            axis.annotate(f"{y:.3f}", (x, y), xytext=(0, 10), textcoords="offset points", ha="center")
        axis.set_title("8,192 次调用 × 4 节点，每节点等待 50 ms")
        axis.set_xticks(capacities)
        axis.margins(y=0.2)
        style(axis, "派发背压容量", ylabel)
    save(figure, "graph-performance")


def history_chart(history):
    overview = history["overview_chart"]
    stages = overview["stages"]
    figure, axes = plt.subplots(3, 2, figsize=(18, 16), layout="constrained")
    figure.suptitle(f"{len(stages)} 轮配对性能比较 · 各轮相对自己的基线", fontsize=20)
    positions = list(range(1, len(stages) + 1))
    short_labels = {"线程 core 重构": "线程重构", "线程等待优化": "等待优化"}
    labels = ["\n".join([str(s["round"]), *(short_labels.get(label, label) for label in s["labels"])])
              for s in stages]
    for axis, panel in zip(axes.flat, overview["panels"], strict=True):
        axis.axvspan(2.6, 3.4, color="#e5e7eb", alpha=0.5)
        axis.axvspan(len(stages) - 0.4, len(stages) + 0.4, color="#dbeafe", alpha=0.6)
        for index, series in enumerate(panel["series"]):
            values = [float("nan") if value is None else value for value in series["values"]]
            axis.plot(positions, values, "o-", linewidth=1.8,
                      color=COLORS[index % len(COLORS)] if index < 5 else "#be123c", label=series["label"])
        axis.axhline(0, color="#6b7280", linewidth=0.8)
        axis.set_title(panel["title"])
        axis.set_ylabel("耗时变化（%，负值更快）")
        axis.set_xticks(positions, labels, fontsize=9)
        axis.grid(alpha=0.2)
        axis.legend(fontsize=9, ncol=2)
    save(figure, "performance-history", ROOT.parent)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--font", type=Path)
    parser.add_argument("--history", action="store_true", help="Also redraw the paired-comparison history.")
    args = parser.parse_args()
    family = configure_font(args.font)
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        graph_chart(json.loads((ROOT / "graph.json").read_text(encoding="utf-8"))["results"])
        if args.history:
            history_chart(json.loads((ROOT.parent / "history.json").read_text(encoding="utf-8")))
    print(f"Rendered PNG and SVG charts using {family}.")


if __name__ == "__main__":
    main()
