from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Mapping, Optional

import yaml

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

from data_loader import DSO

ResultsByYear = Mapping[int, int]
ResultsByDSO = Mapping[DSO, ResultsByYear]


def _load_graph_config(config_path: Path) -> Dict:
    if not config_path.exists():
        print(f"Graph config not found at {config_path}. Skipping visualization.")
        return {}
    with open(config_path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
        if not isinstance(data, dict):
            raise ValueError("Graph configuration must be a mapping at the top level.")
        return data


def _year_window(
    values: ResultsByYear,
    start_year: Optional[int],
    end_year: Optional[int],
) -> Tuple[List[int], List[int]]:
    if not values:
        return [], []
    years = sorted(values.keys())
    start = start_year if start_year is not None else years[0]
    end = end_year if end_year is not None else years[-1]
    if start > end:
        raise ValueError(
            f"Invalid year range: start_year ({start}) > end_year ({end})"
        )
    filtered_years = [year for year in years if start <= year <= end]
    filtered_totals = [int(values[year]) for year in filtered_years]
    return filtered_years, filtered_totals


def _ensure_non_zero_axis(ax, totals: Iterable[int]) -> None:
    series = list(totals)
    if not series:
        return
    min_val = min(series)
    max_val = max(series)
    if min_val == max_val:
        margin = max(1, int(min_val * 0.05)) if min_val else 1
        bottom = max(min_val - margin, 1)
        top = max_val + margin
    else:
        span = max_val - min_val
        margin = max(int(span * 0.08), int(max_val * 0.01), 1)
        bottom = max(min_val - margin, 1)
        top = max_val + margin
    ax.set_ylim(bottom=bottom, top=top)


def _plot_series(
    graph_id: str,
    years: List[int],
    totals: List[int],
    title: str,
    output_dir: Path,
    color: str,
    x_label: str,
    y_label: str,
) -> Path:
    if not years:
        raise ValueError(f"No data available for graph '{graph_id}'.")
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{graph_id}_active_connections.svg"
    fig_path = output_dir / filename
    fig, ax = plt.subplots(figsize=(8.5, 5))
    ax.plot(years, totals, marker="o", linewidth=2.4, color=color)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.grid(True, axis="y", alpha=0.35, linestyle="--")
    ax.grid(True, axis="x", alpha=0.15)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    _ensure_non_zero_axis(ax, totals)
    fig.tight_layout()
    fig.savefig(fig_path, format="svg")
    plt.close(fig)
    return fig_path


def _aggregate_totals(results: ResultsByDSO) -> Dict[int, int]:
    aggregate: Dict[int, int] = {}
    for dso_data in results.values():
        for year, value in dso_data.items():
            aggregate[year] = aggregate.get(year, 0) + int(value)
    return aggregate


def plot_connection_graphs(
    results: ResultsByDSO,
    config_path: Path | str = "graph_config.yaml",
) -> Dict[str, Path]:
    """
    Generate one graph per DSO and a combined graph based on a YAML config.
    Returns mapping from graph id to generated file path.
    """
    config = _load_graph_config(Path(config_path))
    if not config:
        return {}
    year_ranges = config.get("year_ranges", {})
    output_dir = Path(config.get("output_dir", "figures"))
    axes_defaults = config.get("axes", {})

    palette = plt.get_cmap("tab10")
    generated: Dict[str, Path] = {}

    for idx, dso in enumerate(DSO):
        dso_results = results.get(dso, {})
        if not dso_results:
            continue
        dso_key = dso.value
        yr_config = year_ranges.get(dso_key, {})
        years, totals = _year_window(
            dso_results,
            yr_config.get("start_year"),
            yr_config.get("end_year"),
        )
        if not years:
            continue
        title = yr_config.get(
            "title", f"{dso_key.title()} active gas connections"
        )
        graph_id = yr_config.get("graph_id", dso_key.replace(" ", "_"))
        color = yr_config.get("color") or palette(idx % palette.N)
        x_label = yr_config.get("x_label", axes_defaults.get("x_label", "Year"))
        y_label = yr_config.get(
            "y_label", axes_defaults.get("y_label", "Active gas connections")
        )
        generated[graph_id] = _plot_series(
            graph_id=graph_id,
            years=years,
            totals=totals,
            title=title,
            output_dir=output_dir,
            color=color,
            x_label=x_label,
            y_label=y_label,
        )

    # Summed graph
    aggregate_values = _aggregate_totals(results)
    total_cfg = year_ranges.get("all_dsos", {})
    years, totals = _year_window(
        aggregate_values,
        total_cfg.get("start_year"),
        total_cfg.get("end_year"),
    )
    if years:
        graph_id = total_cfg.get("graph_id", "all_dsos")
        title = total_cfg.get("title", "All DSOs - active gas connections")
        color = total_cfg.get("color") or palette(len(DSO) % palette.N)
        x_label = total_cfg.get("x_label", axes_defaults.get("x_label", "Year"))
        y_label = total_cfg.get(
            "y_label", axes_defaults.get("y_label", "Active gas connections")
        )
        generated[graph_id] = _plot_series(
            graph_id=graph_id,
            years=years,
            totals=totals,
            title=title,
            output_dir=output_dir,
            color=color,
            x_label=x_label,
            y_label=y_label,
        )

    return generated
