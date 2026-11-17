"""Render a Markdown comparison of two reports written by ``benchmarks/run.py``."""

import argparse
import json
import statistics
from pathlib import Path
from typing import Any, Optional


def _medians(path: Path) -> dict[str, dict[str, Optional[float]]]:
    report = json.loads(path.read_text(encoding="utf-8"))
    medians: dict[str, dict[str, Optional[float]]] = {}
    for result in report["results"]:
        peaks = [
            sample["process_peak_rss_bytes"]
            for sample in result["samples"]
            if sample.get("process_peak_rss_bytes") is not None
        ]
        medians[result["case"]] = {
            "seconds": result["median_seconds"],
            "peak": statistics.median(peaks) if peaks else None,
        }
    return medians


def _ratio(current: Optional[float], baseline: Optional[float]) -> str:
    if not current or not baseline:
        return "n/a"
    return f"{current / baseline:.2f}x"


def _cell(value: Optional[float], scale: float, unit: str) -> str:
    return "n/a" if value is None else f"{value * scale:.1f} {unit}"


def render(baseline_path: Path, current_path: Path) -> str:
    baseline = _medians(baseline_path)
    current = _medians(current_path)
    lines = [
        "| Case | Baseline | Current | Time | Baseline peak RSS | Current peak RSS | Memory |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    megabyte = 1 / (1024 * 1024)
    for case, now in current.items():
        before: dict[str, Any] = baseline.get(case, {"seconds": None, "peak": None})
        lines.append(
            f"| {case} "
            f"| {_cell(before['seconds'], 1000, 'ms')} "
            f"| {_cell(now['seconds'], 1000, 'ms')} "
            f"| {_ratio(now['seconds'], before['seconds'])} "
            f"| {_cell(before['peak'], megabyte, 'MB')} "
            f"| {_cell(now['peak'], megabyte, 'MB')} "
            f"| {_ratio(now['peak'], before['peak'])} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("current", type=Path)
    args = parser.parse_args()
    print(render(args.baseline, args.current), end="")


if __name__ == "__main__":
    main()
