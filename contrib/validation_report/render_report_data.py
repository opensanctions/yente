#!/usr/bin/env python3
"""Render a JSON report data file into an HTML report."""

import hashlib
import json
import statistics
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import click
from jinja2 import Environment, FileSystemLoader
from markdown_it import MarkdownIt

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TEMPLATES_DIR = Path(__file__).parent / "templates"
RESOURCES_DIR = Path(__file__).parent / "resources"

FIXTURE_DESCRIPTIONS: dict[str, str] = {
    "negatives_global.csv": (
        "Global true-negatives from an internal OpenSanctions reference dataset of synthetic person records. "
        "Generated with multi-cultural name diversity, geographic correlation, and realistic field variations "
        "to test false-positive rates across diverse global naming conventions."
    ),
    "negatives_us.csv": (
        "US true-negatives from an internal OpenSanctions reference dataset of synthetic person records. "
        "Reflects US-based individuals including cultural mixing (e.g. US nationals with international name origins) "
        "to test false-positive rates for US-centric screening."
    ),
    "positives_un_treated.csv": (
        "Generated from the `un_sc_sanctions` dataset, treated version: minor typos and name reshuffles applied. These are true-positives."
    ),
    "positives_un_untreated.csv": (
        "Generated from the `un_sc_sanctions`. These are true-positives."
    ),
    "positives_us_congress_untreated.csv": (
        "Generated from the `us_congress` dataset. These are true-positives."
    ),
}


def md5(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


def compute_stats(entries: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [e["score"] for e in entries]
    matches = sum(1 for e in entries if e["match"])
    total = len(entries)
    return {
        "mean_top_score": statistics.mean(scores) if scores else 0.0,
        "matches": matches,
        "total": total,
        "pct_matches": (matches / total * 100) if total else 0.0,
    }


@click.command()
@click.argument("input_json", type=click.Path(exists=True))
@click.option("--dataset", default="default", show_default=True)
@click.option(
    "--output",
    default="report.html",
    show_default=True,
    type=click.Path(),
    help="Path to write the HTML report.",
)
def main(input_json: str, dataset: str, output: str) -> None:
    data: dict[str, dict[str, list[dict[str, Any]]]] = json.loads(
        Path(input_json).read_text()
    )

    fixtures = []
    for fixture_path in sorted(FIXTURES_DIR.glob("*.csv")):
        lines = fixture_path.read_text(encoding="utf-8").splitlines()
        records = max(0, len(lines) - 1)  # subtract header
        fixtures.append(
            {
                "name": fixture_path.name,
                "description": FIXTURE_DESCRIPTIONS.get(fixture_path.name, ""),
                "md5": md5(fixture_path),
                "records": records,
            }
        )

    results = {
        fixture_name: {algo: compute_stats(entries) for algo, entries in algos.items()}
        for fixture_name, algos in data.items()
    }

    pip_freeze = subprocess.check_output(["pip", "freeze"], text=True).strip()
    yente_git_version = subprocess.check_output(
        ["git", "describe", "--tags", "--always", "--dirty"],
        text=True,
        cwd=Path(__file__).parent,
    ).strip()

    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR), keep_trailing_newline=True
    )
    md_rendered = env.get_template("report.md.j2").render(
        date=date.today().isoformat(),
        dataset=dataset,
        fixtures=fixtures,
        results=results,
        pip_freeze=pip_freeze,
        yente_git_version=yente_git_version,
    )

    html_body = MarkdownIt("gfm-like").render(md_rendered)
    html = (
        f"<!doctype html>\n<html>\n<head>\n<meta charset='utf-8'>\n"
        f"<style>\n{(RESOURCES_DIR / 'github-markdown.css').read_text()}\n</style>\n"
        f"<style>\n{(RESOURCES_DIR / 'report.css').read_text()}\n</style>\n"
        f"</head>\n<body class='markdown-body'>\n{html_body}</body>\n</html>\n"
    )

    output_path = Path(output)
    output_path.write_text(html, encoding="utf-8")
    print(f"report written to {output_path}")


if __name__ == "__main__":
    main()
