#!/usr/bin/env python3
import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Point:
    backend: str
    label: str
    events: Optional[int]
    facts: Optional[int]
    episodes: Optional[int]
    procedures: Optional[int]
    insights: Optional[int]
    mean_us: float
    median_us: float
    std_dev_us: float


LABEL_RE = re.compile(
    r"events(?P<events>\d+)_facts(?P<facts>\d+)_episodes(?P<episodes>\d+)_procedures(?P<procedures>\d+)_insights(?P<insights>\d+)"
)


def parse_label(label: str):
    match = LABEL_RE.search(label)
    if not match:
        return (None, None, None, None, None)
    return (
        int(match.group("events")),
        int(match.group("facts")),
        int(match.group("episodes")),
        int(match.group("procedures")),
        int(match.group("insights")),
    )


def ns_to_us(value: float) -> float:
    return value / 1000.0


def load_groups(input_dir: Path) -> dict[str, list[Point]]:
    groups: dict[str, list[Point]] = {}
    for estimate_path in input_dir.glob("**/new/estimates.json"):
        try:
            rel = estimate_path.relative_to(input_dir)
        except ValueError:
            continue
        parts = rel.parts
        if len(parts) < 4:
            continue
        if parts[0] in ("memory", "sqlite"):
            group_name = input_dir.name
            backend = parts[0]
            label = parts[1]
        else:
            group_name = parts[0]
            backend = parts[1]
            label = parts[2]
        with estimate_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        events, facts, episodes, procedures, insights = parse_label(label)
        point = Point(
            backend=backend,
            label=label,
            events=events,
            facts=facts,
            episodes=episodes,
            procedures=procedures,
            insights=insights,
            mean_us=ns_to_us(data["mean"]["point_estimate"]),
            median_us=ns_to_us(data["median"]["point_estimate"]),
            std_dev_us=ns_to_us(data["std_dev"]["point_estimate"]),
        )
        groups.setdefault(group_name, []).append(point)
    return groups


def choose_x_axis(points: list[Point]):
    def metric_values(getter):
        return [getter(point) for point in points if getter(point) is not None]

    metrics = [
        ("events count", lambda p: p.events),
        ("facts count", lambda p: p.facts),
        ("episodes count", lambda p: p.episodes),
        ("procedures count", lambda p: p.procedures),
        ("insights count", lambda p: p.insights),
    ]

    for label, getter in metrics:
        values = metric_values(getter)
        if values and len(set(values)) > 1:
            x_values = sorted(set(values))
            return label, x_values, min(x_values), max(x_values), False

    x_values = list(range(len(points)))
    return "dataset index", x_values, 0, max(x_values) if x_values else 1, True


def format_count(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M".rstrip("0").rstrip(".")
    if value >= 1_000:
        return f"{value / 1_000:.1f}k".rstrip("0").rstrip(".")
    return str(value)


def build_svg(points: list[Point]) -> str:
    width = 960
    height = 420
    margin = 60
    plot_width = width - margin * 2
    plot_height = height - margin * 2

    backends = sorted({point.backend for point in points})
    colors = {"memory": "#1f77b4", "sqlite": "#ff7f0e"}

    x_label, x_values, x_min, x_max, use_index = choose_x_axis(points)

    max_y = max(point.mean_us for point in points) if points else 1.0
    max_y *= 1.1

    def x_scale(value: int, index: int) -> float:
        if use_index:
            x_pos = index
            x_range = max(len(points) - 1, 1)
            return margin + (x_pos / x_range) * plot_width
        return margin + ((value - x_min) / max(x_max - x_min, 1)) * plot_width

    def y_scale(value: float) -> float:
        return margin + plot_height - (value / max_y) * plot_height

    svg = [
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">',
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{margin + plot_height}" stroke="#333"/>',
        f'<line x1="{margin}" y1="{margin + plot_height}" x2="{margin + plot_width}" y2="{margin + plot_height}" stroke="#333"/>',
        f'<text x="{width / 2}" y="30" text-anchor="middle" font-size="16" font-family="Arial">build_memory_packet latency (microseconds)</text>',
        f'<text x="{width / 2}" y="{height - 10}" text-anchor="middle" font-size="12" font-family="Arial">{x_label}</text>',
        f'<text x="20" y="{height / 2}" text-anchor="middle" font-size="12" font-family="Arial" transform="rotate(-90 20 {height / 2})">mean latency (us)</text>',
    ]

    # Y axis ticks
    ticks = 5
    for i in range(ticks + 1):
        value = max_y * (i / ticks)
        y = y_scale(value)
        svg.append(f'<line x1="{margin - 4}" y1="{y}" x2="{margin}" y2="{y}" stroke="#333"/>')
        svg.append(
            f'<text x="{margin - 8}" y="{y + 4}" text-anchor="end" font-size="10" font-family="Arial">{value:.0f}</text>'
        )

    # X axis ticks
    max_labels = 8
    if not use_index:
        step = max(1, len(x_values) // max_labels)
        for idx, value in enumerate(x_values):
            if idx % step != 0 and idx != len(x_values) - 1:
                continue
            x = x_scale(value, 0)
            svg.append(
                f'<line x1="{x}" y1="{margin + plot_height}" x2="{x}" y2="{margin + plot_height + 4}" stroke="#333"/>'
            )
            label = format_count(value)
            rotation = " rotate(-35 {})".format(x) if len(x_values) > 6 else ""
            svg.append(
                f'<text x="{x}" y="{margin + plot_height + 18}" text-anchor="middle" font-size="10" font-family="Arial" transform="translate(0,0){rotation}">{label}</text>'
            )
    else:
        step = max(1, len(points) // max_labels)
        for i, point in enumerate(points):
            if i % step != 0 and i != len(points) - 1:
                continue
            x = x_scale(i, i)
            svg.append(
                f'<line x1="{x}" y1="{margin + plot_height}" x2="{x}" y2="{margin + plot_height + 4}" stroke="#333"/>'
            )
            rotation = " rotate(-35 {})".format(x) if len(points) > 6 else ""
            svg.append(
                f'<text x="{x}" y="{margin + plot_height + 18}" text-anchor="middle" font-size="10" font-family="Arial" transform="translate(0,0){rotation}">{point.label}</text>'
            )

    # Series lines and points
    for backend in backends:
        series = [point for point in points if point.backend == backend]
        series.sort(key=lambda p: p.events if p.events is not None else 0)
        poly_points = []
        for index, point in enumerate(series):
            x_value = point.events if point.events is not None else index
            x = x_scale(x_value, index)
            y = y_scale(point.mean_us)
            poly_points.append(f"{x},{y}")
        color = colors.get(backend, "#333333")
        svg.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{" ".join(poly_points)}"/>'
        )
        for index, point in enumerate(series):
            x_value = point.events if point.events is not None else index
            x = x_scale(x_value, index)
            y = y_scale(point.mean_us)
            svg.append(f'<circle cx="{x}" cy="{y}" r="4" fill="{color}">')
            tooltip = f"{backend} {point.label} mean={point.mean_us:.2f}us median={point.median_us:.2f}us std={point.std_dev_us:.2f}us"
            svg.append(f"<title>{tooltip}</title></circle>")

    # Legend
    legend_x = margin + 10
    legend_y = margin - 30
    for idx, backend in enumerate(backends):
        color = colors.get(backend, "#333333")
        x = legend_x + idx * 120
        svg.append(f'<rect x="{x}" y="{legend_y}" width="12" height="12" fill="{color}"/>')
        svg.append(
            f'<text x="{x + 18}" y="{legend_y + 10}" font-size="12" font-family="Arial">{backend}</text>'
        )

    svg.append("</svg>")
    return "\n".join(svg)


def build_table(points: list[Point]) -> str:
    rows = []
    for point in sorted(points, key=lambda p: (p.backend, p.events or 0)):
        rows.append(
            "<tr>"
            f"<td>{point.backend}</td>"
            f"<td>{point.label}</td>"
            f"<td>{point.events or '-'}</td>"
            f"<td>{point.facts or '-'}</td>"
            f"<td>{point.episodes or '-'}</td>"
            f"<td>{point.procedures or '-'}</td>"
            f"<td>{point.insights or '-'}</td>"
            f"<td>{point.mean_us:.2f}</td>"
            f"<td>{point.median_us:.2f}</td>"
            f"<td>{point.std_dev_us:.2f}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def render_html(groups: dict[str, list[Point]], title: str) -> str:
    sections = []
    for name in sorted(groups.keys()):
        points = groups[name]
        svg = build_svg(points)
        table_rows = build_table(points)
        sections.append(
            f"""
            <section>
              <h2>{name}</h2>
              <div class="chart">{svg}</div>
              <table>
                <thead>
                  <tr>
                    <th>backend</th>
                    <th>dataset</th>
                    <th>events</th>
                    <th>facts</th>
                    <th>episodes</th>
                    <th>procedures</th>
                    <th>insights</th>
                    <th>mean (us)</th>
                    <th>median (us)</th>
                    <th>std dev (us)</th>
                  </tr>
                </thead>
                <tbody>
                  {table_rows}
                </tbody>
              </table>
            </section>
            """
        )
    sections_html = "\n".join(sections)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 24px;
      color: #222;
    }}
    .chart {{
      margin-bottom: 24px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      font-size: 13px;
    }}
    th, td {{
      border: 1px solid #ddd;
      padding: 8px;
      text-align: left;
    }}
    th {{
      background-color: #f5f5f5;
    }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <p>Source: {title}. Values are microseconds (mean, median, std dev).</p>
  {sections_html}
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate HTML chart from Criterion output.")
    parser.add_argument(
        "--input",
        default="target/criterion",
        help="Path to Criterion benchmark root directory.",
    )
    parser.add_argument(
        "--output",
        default="target/criterion/summary.html",
        help="Output HTML file path.",
    )
    parser.add_argument("--title", default="Engram build_memory_packet benchmark")
    args = parser.parse_args()

    input_dir = Path(args.input)
    if not input_dir.exists():
        raise SystemExit(f"Input directory not found: {input_dir}")

    groups = load_groups(input_dir)
    if not groups:
        raise SystemExit(f"No estimates.json found under {input_dir}")

    html = render_html(groups, args.title)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
