"""Matched comparisons and small SVG summaries using the standard library."""
import argparse
import csv
import json
import pathlib
import statistics
import xml.etree.ElementTree as ET


def median(values):
    return statistics.median(values)


def svg_chart(path, labels, off, on, title, unit):
    ns = "http://www.w3.org/2000/svg"
    ET.register_namespace("", ns)
    root = ET.Element(f"{{{ns}}}svg", width="940", height="430", viewBox="0 0 940 430", role="img")
    def el(kind, **attrs):
        return ET.SubElement(root, f"{{{ns}}}{kind}", {k.replace("_", "-"): str(v) for k, v in attrs.items()})
    def text(x, y, value, **attrs):
        el("text", x=x, y=y, font_family="sans-serif", font_size=13, **attrs).text = value
    el("rect", width=940, height=430, fill="white")
    text(30, 30, title, font_weight="bold")
    text(30, 52, "Three matched repetitions; bars = medians, whiskers = observed ranges")
    maximum = max([v for group in off + on for v in group] + [1]) * 1.15
    left, top, bottom = 75, 85, 340
    for tick in range(5):
        value = maximum * tick / 4
        y = bottom - (bottom - top) * tick / 4
        el("line", x1=left, x2=920, y1=y, y2=y, stroke="#ddd")
        text(65, y + 4, f"{value:.0f}", text_anchor="end")
    text(15, 78, unit)
    for i, label in enumerate(labels):
        center = 150 + i * 165
        for dx, data, color in [(-24, off[i], "#315b96"), (24, on[i], "#d97826")]:
            height = median(data) / maximum * (bottom - top)
            el("rect", x=center + dx - 18, y=bottom - height, width=36, height=height, fill=color)
            high, low = max(data), min(data)
            y1, y2 = bottom - high / maximum * (bottom - top), bottom - low / maximum * (bottom - top)
            el("line", x1=center + dx, x2=center + dx, y1=y1, y2=y2, stroke="#222")
            for y in (y1, y2):
                el("line", x1=center + dx - 6, x2=center + dx + 6, y1=y, y2=y, stroke="#222")
        text(center, 363, label.replace("_", " "), text_anchor="middle")
    el("rect", x=260, y=390, width=14, height=14, fill="#315b96")
    text(282, 402, "Retry only")
    el("rect", x=450, y=390, width=14, height=14, fill="#d97826")
    text(472, 402, "Speculation enabled")
    ET.ElementTree(root).write(path, encoding="unicode", xml_declaration=True)


def analyze(raw, output):
    output.mkdir(parents=True, exist_ok=True)
    rows = [json.loads(line) for line in (raw / "observations.jsonl").read_text().splitlines()]
    manifest = json.loads((raw / "manifest.json").read_text())
    assert len(rows) == 30 and all(r["correct"] and r["timing_complete"] for r in rows)
    labels = [s["name"] for s in manifest["manifest"]["scenarios"]]
    index = {(r["scenario"], r["repetition"], r["speculation"]): r for r in rows}
    assert len(index) == len(rows)
    pairs = []
    for scenario in labels:
        for rep in range(3):
            off, on = index[scenario, rep, "off"], index[scenario, rep, "on"]
            assert off["input_sha256"] == on["input_sha256"] and off["result_sha256"] == on["result_sha256"]
            pairs.append(dict(scenario=scenario, repetition=rep,
                              speedup=off["job_ms"] / on["job_ms"],
                              latency_change_pct=100 * (on["job_ms"] / off["job_ms"] - 1),
                              cpu_change_pct=100 * (on["total_cpu_us"] / off["total_cpu_us"] - 1),
                              slot_change_pct=100 * (on["total_slot_us"] / off["total_slot_us"] - 1),
                              spec_cpu_over_baseline_pct=100 * on["speculative_cpu_us"] / off["total_cpu_us"],
                              wasted_cpu_fraction_pct=100 * on["wasted_cpu_us"] / on["total_cpu_us"],
                              wasted_slot_fraction_pct=100 * on["wasted_slot_us"] / on["total_slot_us"]))
    (output / "phase2-pairs.json").write_text(json.dumps(pairs, indent=2) + "\n")
    exclude = {"worker_timings", "metrics"}
    columns = [key for key in rows[0] if key not in exclude]
    with (output / "phase2-observations.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows({k: json.dumps(v) if isinstance(v, list) else v for k, v in row.items() if k not in exclude} for row in rows)
    lines = ["# Phase 2 controlled performance study", "", "Frozen policy v1; 30 serial runs, five scenarios, three matched repetitions per mode. Every run passed the independent serial oracle and complete local attempt-timing reconciliation. No run was filtered for performance.", "",
             "Job latency below starts at coordinator construction (including input snapshots) and ends after final atomic publication. Map latency starts at first dispatch and ends after all maps are validated and accepted, before final publication. The CSV also includes external coordinator-launch-to-publication observation (5 ms polling) and full process/drain duration. Worker startup is excluded. Full drain time is not hidden inside the latency improvement.", "",
             "| Scenario | Retry-only job median [range], ms | Speculation job median [range], ms | Median paired speedup | Backups / wins per enabled run |",
             "| --- | --- | --- | --- | --- |"]
    for scenario in labels:
        off = [index[scenario, rep, "off"]["job_ms"] for rep in range(3)]
        on = [index[scenario, rep, "on"]["job_ms"] for rep in range(3)]
        group = [p for p in pairs if p["scenario"] == scenario]
        counts = [f"{index[scenario, rep, 'on']['speculative_attempts']}/{index[scenario, rep, 'on']['backup_wins']}" for rep in range(3)]
        lines.append(f"| {scenario} | {median(off):.0f} [{min(off)}, {max(off)}] | {median(on):.0f} [{min(on)}, {max(on)}] | {median([p['speedup'] for p in group]):.3f}× | {', '.join(counts)} |")
    lines += ["", "![Job latency](phase2-latency.svg)", "", "| Scenario | Map medians off / on, ms | Paired CPU change | Paired slot-time change | Speculative CPU / baseline CPU | Wasted CPU fraction (on) | Wasted slot fraction (on) |",
              "| --- | --- | --- | --- | --- | --- | --- |"]
    for scenario in labels:
        group = [p for p in pairs if p["scenario"] == scenario]
        off = median([index[scenario, r, "off"]["map_ms"] for r in range(3)])
        on = median([index[scenario, r, "on"]["map_ms"] for r in range(3)])
        values = [median([p[k] for p in group]) for k in ("cpu_change_pct", "slot_change_pct", "spec_cpu_over_baseline_pct", "wasted_cpu_fraction_pct", "wasted_slot_fraction_pct")]
        lines.append(f"| {scenario} | {off:.0f} / {on:.0f} | " + " | ".join(f"{v:+.2f}%" if i < 2 else f"{v:.2f}%" for i, v in enumerate(values)) + " |")
    lines += ["", "Percentages are medians of matched pair values, not ratios of unrelated averages. CPU change includes ordinary, retry, original and backup execution; it can be negative due to noise or avoided retries. Speculative CPU/baseline is separately measured backup work. Wasted work means **all nonwinning attempts**, including a slow original defeated by its backup; it is not restricted to speculative losers.", "",
              "Occupied-slot time includes controlled sleep, map computation and candidate I/O up to the timing boundary. Thread CPU time uses CLOCK_THREAD_CPUTIME_ID and excludes sleeping; it excludes the final timing-file write and process/gRPC thread overhead. This is a synthetic slowdown study of the existing word-count workload, not a real cluster or CPU-intensive throughput benchmark.", "",
              "![Wasted occupied-slot fraction](phase2-waste.svg)", "", "## Outcomes and negative findings", ""]
    for scenario in labels:
        group = [p for p in pairs if p["scenario"] == scenario]
        changes = ", ".join(f"{p['latency_change_pct']:+.2f}%" for p in group)
        lines.append(f"- {scenario}: matched job latency changes, repetitions 0–2: {changes}.")
    regressed = [p for p in pairs if p["latency_change_pct"] > 0]
    lines += ["", f"{len(regressed)} of {len(pairs)} matched pairs had higher job latency with speculation enabled. Three repetitions expose some natural variability but are too few for strong statistical significance claims. No significance or general speedup guarantee is claimed.", "",
              "Speculation does not cancel losers. Faster result publication can therefore coexist with almost unchanged time until all workers become idle. The full process/drain durations are retained in the CSV; the large wasted occupied-slot fractions in straggler cases include the original's injected delay. CPU costs are small and noisy at this scale. The saturated control deliberately has one worker/one slot and cannot place a backup on another worker. Concurrent multiworker saturation and supersession accounting are tested separately with deterministic gates.", "",
              "## Evidence and reproduction", "", "- Frozen policy: `docs/speculative-policy.md`; frozen schedules: `benchmarks/manifest.json`.",
              "- Evidence input directory: `" + str(raw.relative_to(SOURCE) if raw.is_relative_to(SOURCE) else raw.name) + "`.",
              ("- This directory contains sanitized numerical observations from the original Phase 2 study. Full internal runs are retained privately and are not redistributed." if manifest.get("curated_evidence") else "- Each raw run contains original inputs, immutable snapshots, worker candidates and timing files, accepted results, coordinator metrics/events, stdout logs, an observation and process cleanup record."),
              "- The study's manifest includes source and binary SHA-256 fingerprints. `observations.jsonl` preserves every run, and the CSV/pair JSON preserve every numerical comparison.",
              "- Injected crashes write a timing record before exiting; the study reconciles this local evidence because the coordinator cannot retrieve a terminal RPC snapshot from a dead process. Coordinator-only work metrics remain explicitly labeled observed/lower bounds when attempts are unresolved.", "",
              "```sh", "python3 benchmarks/run_study.py --bin-dir build-phase2-release --output runs/phase2-study-reproduction",
              "python3 benchmarks/analyze.py --raw runs/phase2-study-reproduction --output reports/reproduction", "```", ""]
    (output / "phase2-performance.md").write_text("\n".join(lines))
    off = [[index[s, r, "off"]["job_ms"] for r in range(3)] for s in labels]
    on = [[index[s, r, "on"]["job_ms"] for r in range(3)] for s in labels]
    svg_chart(output / "phase2-latency.svg", labels, off, on, "Time to a validated, published result", "ms")
    off = [[100 * index[s, r, "off"]["wasted_slot_us"] / index[s, r, "off"]["total_slot_us"] for r in range(3)] for s in labels]
    on = [[100 * index[s, r, "on"]["wasted_slot_us"] / index[s, r, "on"]["total_slot_us"] for r in range(3)] for s in labels]
    svg_chart(output / "phase2-waste.svg", labels, off, on, "Occupied-slot time spent on nonwinning attempts", "%")
    print((output / "phase2-performance.md").read_text())


SOURCE = pathlib.Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, default=SOURCE / "reports")
    args = parser.parse_args()
    analyze(args.raw.resolve(), args.output.resolve())
