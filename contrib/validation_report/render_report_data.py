#!/usr/bin/env python3
"""Render a JSON report data file into an HTML report."""

import hashlib
import statistics
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import click
import orjson
from jinja2 import Environment, FileSystemLoader
from markdown_it import MarkdownIt

FIXTURES_DIR = Path(__file__).parent / "build" / "fixtures"
TEMPLATES_DIR = Path(__file__).parent / "templates"
RESOURCES_DIR = Path(__file__).parent / "resources"


def md5(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


def compute_stats(entries: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [e["score"] for e in entries]
    matches = sum(1 for e in entries if e["match"])
    total = len(entries)
    stats: dict[str, Any] = {
        "mean_top_score": statistics.mean(scores) if scores else 0.0,
        "matches": matches,
        "total": total,
        "pct_matches": (matches / total * 100) if total else 0.0,
    }

    entries_with_id = [e for e in entries if "expected_id_found" in e]
    if entries_with_id:
        n = len(entries_with_id)
        id_found_count = sum(1 for e in entries_with_id if e["expected_id_found"])
        id_matched_count = sum(
            1 for e in entries_with_id if e.get("expected_id_matched")
        )
        stats["id_recall"] = (id_found_count / n * 100) if n else 0.0
        stats["id_match_rate"] = (id_matched_count / n * 100) if n else 0.0
        id_ranks = [
            e["expected_id_rank"]
            for e in entries_with_id
            if e.get("expected_id_rank") is not None
        ]
        stats["mean_id_rank"] = statistics.mean(id_ranks) if id_ranks else 0.0

    return stats


@click.command()
@click.argument("input_json", type=click.Path(), required=False, default=None)
@click.option("--dataset", default="default", show_default=True)
@click.option(
    "--output",
    default=None,
    type=click.Path(),
    help="Path to write the HTML report (default: build/report.html next to this script).",
)
def main(input_json: str | None, dataset: str, output: str | None) -> None:
    input_path = (
        Path(input_json)
        if input_json
        else Path(__file__).parent / "build" / "report_data.json"
    )
    report: dict[str, Any] = orjson.loads(input_path.read_bytes())
    query_scope: str = report.get("query_scope", dataset)
    indexed_dataset: str = report.get("indexed_dataset", dataset)
    data: dict[str, dict[str, list[dict[str, Any]]]] = report["results"]

    fixtures = []
    for fixture_path in sorted(FIXTURES_DIR.glob("*.json")):
        meta = orjson.loads(fixture_path.read_bytes())
        fixtures.append(
            {
                "name": meta["name"],
                "description": meta["description"],
                "generated_from": meta["generated_from"],
                "records": len(meta["data"]),
                "md5": md5(fixture_path),
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
        query_scope=query_scope,
        indexed_dataset=indexed_dataset,
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

    output_path = (
        Path(output) if output else Path(__file__).parent / "build" / "report.html"
    )
    output_path.write_text(html, encoding="utf-8")
    print(f"report written to {output_path}")


if __name__ == "__main__":
    main()
