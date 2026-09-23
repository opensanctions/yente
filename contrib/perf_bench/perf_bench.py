"""Benchmark the /match pipeline in-process, one stage at a time.

Run the curated query set in ``queries.json`` against a local index by calling
the same yente functions the ``/match`` router uses, time each stage with a
wall clock, and compare the sum against an end-to-end ``_match_one_query``
call. Use it to find out where match latency goes, and to compare two git
trees or two index builds via the ``--output`` JSON.
"""

import asyncio
import json
import logging
import statistics
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig
from rich.console import Console
from rich.table import Table

from yente import settings
from yente.app import warm_up
from yente.data.common import (
    EntityExample,
    EntityMatches,
    EntityMatchQuery,
    EntityMatchResponse,
    TotalSpec,
)
from yente.data.dataset import Dataset
from yente.data.entity import Entity
from yente.logs import configure_logging
from yente.provider import SearchProvider, with_provider
from yente.routers.match import _match_one_query
from yente.routers.util import get_algorithm_by_name, get_dataset
from yente.scoring import score_results
from yente.search.queries import DEFAULT_SORTS, Operator, entity_query
from yente.search.search import result_entities, search_entities
from yente.util import limit_window

STAGES = ("from_example", "build_query", "search", "decode", "score", "serialize")
STAGE_HEADERS = ("example", "query", "search", "decode", "score", "serial.")
DEFAULT_QUERIES = Path(__file__).parent / "queries.json"
# Rich assumes 80 columns when stdout is not a terminal, which truncates the table.
console = Console(width=None if sys.stdout.isatty() else 220)


@dataclass
class Case:
    id: str
    tags: list[str]
    example: EntityExample


@dataclass
class Sample:
    """Timings of one sequential run of a case, in seconds per stage."""

    stages: dict[str, float]
    took_ms: int
    hits: int
    matches: int

    @property
    def total(self) -> float:
        return sum(self.stages.values())


@dataclass
class CaseResult:
    case: Case
    samples: list[Sample] = field(default_factory=list)
    e2e: list[float] = field(default_factory=list)
    error: str | None = None

    def median_stage(self, stage: str) -> float:
        return statistics.median(s.stages[stage] for s in self.samples)

    def median_total(self) -> float:
        return statistics.median(s.total for s in self.samples)

    def median_e2e(self) -> float:
        return statistics.median(self.e2e)


@dataclass
class Options:
    dataset: str
    algorithm: str
    limit: int
    threshold: float
    cutoff: float
    repeat: int
    batch: int
    profile: str | None
    profile_out: Path | None
    candidates: int = 0


def candidate_count(limit: int) -> int:
    # Mirrors the candidate window computation in yente/routers/match.py.
    candidates = limit * settings.MATCH_CANDIDATES
    candidates = max(20, min(settings.MAX_RESULTS, candidates))
    candidates = min(candidates, max(settings.MAX_MATCH_CANDIDATES, limit))
    candidates, _ = limit_window(candidates, 0)
    return candidates


def load_cases(path: Path, tags: Sequence[str]) -> list[Case]:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    cases: list[Case] = []
    for item in raw:
        case_tags = list(item.get("tags", []))
        if tags and not set(tags) & set(case_tags):
            continue
        example = EntityExample.model_validate(item["query"])
        cases.append(Case(id=item["id"], tags=case_tags, example=example))
    return cases


async def time_stages(
    case: Case,
    ds: Dataset,
    provider: SearchProvider,
    algorithm: type[ScoringAlgorithm],
    opts: Options,
) -> Sample:
    """Run the match pipeline for one case, stage by stage, and time each stage.

    The hit list is materialised before scoring, while the router feeds a lazy
    generator into ``score_results``; the total is the same, only the
    interleaving of decode and compare differs.
    """
    stages: dict[str, float] = {}
    t0 = time.perf_counter()
    entity = Entity.from_example(case.example)
    t1 = time.perf_counter()
    stages["from_example"] = t1 - t0

    query = entity_query(ds, entity, filter_op=Operator.OR)
    t2 = time.perf_counter()
    stages["build_query"] = t2 - t1

    response = await search_entities(
        provider,
        query,
        limit=opts.candidates,
        sort=DEFAULT_SORTS,
        track_total_hits=False,
    )
    t3 = time.perf_counter()
    stages["search"] = t3 - t2

    hits = list(result_entities(response))
    t4 = time.perf_counter()
    stages["decode"] = t4 - t3

    total, scored = await score_results(
        algorithm,
        entity,
        hits,
        threshold=opts.threshold,
        cutoff=opts.cutoff,
        limit=opts.limit,
        config=ScoringConfig(weights={}, config={}),
    )
    t5 = time.perf_counter()
    stages["score"] = t5 - t4

    parsed = EntityExample(
        id=case.example.id,
        schema=entity.schema.name,
        properties=dict(entity.properties),
    )
    matches = EntityMatches(
        status=200,
        results=scored,
        total=TotalSpec(value=total, relation="eq"),
        query=parsed,
    )
    EntityMatchResponse(
        responses={case.id: matches}, limit=opts.limit
    ).model_dump_json()
    t6 = time.perf_counter()
    stages["serialize"] = t6 - t5

    return Sample(
        stages=stages,
        took_ms=int(response.get("took", 0)),
        hits=len(hits),
        matches=total,
    )


def match_one(case: Case, ds: Dataset, provider: SearchProvider, opts: Options) -> Any:
    """Build the same ``_match_one_query`` coroutine the router builds."""
    match = EntityMatchQuery(queries={case.id: case.example})
    return _match_one_query(
        ds,
        opts.algorithm,
        match,
        case.id,
        case.example,
        [],
        (),
        (),
        (),
        None,
        (),
        provider,
        opts.candidates,
        opts.limit,
        opts.cutoff,
        opts.threshold,
    )


async def time_e2e(
    case: Case, ds: Dataset, provider: SearchProvider, opts: Options
) -> float:
    start = time.perf_counter()
    await match_one(case, ds, provider, opts)
    return time.perf_counter() - start


async def time_batch(
    cases: Sequence[Case], ds: Dataset, provider: SearchProvider, opts: Options
) -> float:
    start = time.perf_counter()
    await asyncio.gather(*(match_one(c, ds, provider, opts) for c in cases))
    return time.perf_counter() - start


async def run_cases(
    cases: list[Case],
    ds: Dataset,
    provider: SearchProvider,
    algorithm: type[ScoringAlgorithm],
    opts: Options,
) -> list[CaseResult]:
    results: list[CaseResult] = []
    with console.status("") as status:
        for idx, case in enumerate(cases):
            status.update(f"[{idx + 1}/{len(cases)}] {case.id}")
            result = CaseResult(case=case)
            try:
                # Untimed first pass: ES first-touch cost for this query's terms
                # would otherwise land on the stage run alone and skew the parity.
                await match_one(case, ds, provider, opts)
                for _ in range(opts.repeat):
                    result.samples.append(
                        await time_stages(case, ds, provider, algorithm, opts)
                    )
                    result.e2e.append(await time_e2e(case, ds, provider, opts))
            except Exception as exc:  # noqa: BLE001
                result.error = f"{type(exc).__name__}: {exc}"
            results.append(result)
    return results


async def run_batches(
    cases: list[Case], ds: Dataset, provider: SearchProvider, opts: Options
) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for start in range(0, len(cases), opts.batch):
        group = cases[start : start + opts.batch]
        wall = await time_batch(group, ds, provider, opts)
        batches.append({"cases": [c.id for c in group], "wall": wall})
    return batches


async def run_async(
    cases: list[Case], opts: Options
) -> tuple[list[CaseResult], list[dict[str, Any]], dict[str, Any]]:
    settings.AUTO_REINDEX = False
    configure_logging()
    logging.getLogger().setLevel(logging.WARNING)
    with console.status("Warming up catalog and matchers..."):
        await warm_up()
    ds = await get_dataset(opts.dataset)
    algorithm = get_algorithm_by_name(opts.algorithm)
    opts.candidates = candidate_count(opts.limit)

    async with with_provider() as provider:
        indices = await provider.get_alias_indices(settings.ENTITY_INDEX)
        profiler = None
        if opts.profile is not None:
            from pyinstrument import Profiler

            profiler = Profiler(async_mode="enabled")
            profiler.start()
        results = await run_cases(cases, ds, provider, algorithm, opts)
        if profiler is not None:
            profiler.stop()
            if opts.profile == "html" and opts.profile_out is not None:
                opts.profile_out.write_text(profiler.output_html())
            else:
                console.print(profiler.output_text(unicode=True, color=True))

        batches: list[dict[str, Any]] = []
        if opts.batch > 1:
            batches = await run_batches(cases, ds, provider, opts)

    meta = {
        "timestamp": datetime.now(UTC).isoformat(),
        "git_sha": git_sha(),
        "yente_version": settings.VERSION,
        "index_alias": settings.ENTITY_INDEX,
        "indices": indices,
        "dataset": opts.dataset,
        "algorithm": opts.algorithm,
        "algorithm_resolved": algorithm.NAME,
        "limit": opts.limit,
        "candidates": opts.candidates,
        "threshold": opts.threshold,
        "cutoff": opts.cutoff,
        "repeat": opts.repeat,
        "batch": opts.batch,
    }
    return results, batches, meta


def git_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).parent,
        )
        return out.stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None


def ms(seconds: float) -> str:
    return f"{seconds * 1000:.1f}"


def percentile(values: Sequence[float], pct: float) -> float:
    if len(values) < 2:
        return values[0] if values else 0.0
    cuts = statistics.quantiles(values, n=100, method="inclusive")
    return cuts[int(pct) - 1]


def print_cases(results: list[CaseResult]) -> None:
    table = Table(title="Per-query median (ms)", show_lines=False, pad_edge=False)
    table.add_column("id", no_wrap=True)
    table.add_column("hits", justify="right")
    table.add_column("match", justify="right")
    for stage in STAGE_HEADERS:
        table.add_column(stage, justify="right", no_wrap=True)
    table.add_column("took", justify="right", style="dim")
    table.add_column("sum", justify="right", style="bold")
    table.add_column("e2e", justify="right", style="bold")
    for res in results:
        if res.error is not None or not res.samples:
            table.add_row(res.case.id, *([""] * (len(STAGES) + 4)), style="red")
            console.print(f"[red]{res.case.id}: {res.error}[/red]")
            continue
        took = statistics.median(s.took_ms for s in res.samples)
        table.add_row(
            res.case.id,
            str(res.samples[-1].hits),
            str(res.samples[-1].matches),
            *(ms(res.median_stage(stage)) for stage in STAGES),
            f"{took:.0f}",
            ms(res.median_total()),
            ms(res.median_e2e()),
        )
    console.print(table)


def print_summary(results: list[CaseResult]) -> None:
    ok = [r for r in results if r.error is None and r.samples]
    if not ok:
        return
    table = Table(title=f"Across {len(ok)} queries (ms)")
    table.add_column("stage")
    table.add_column("p50", justify="right")
    table.add_column("p95", justify="right")
    table.add_column("max", justify="right")
    table.add_column("share", justify="right")
    total_sum = sum(r.median_total() for r in ok)
    for stage in STAGES:
        vals = sorted(r.median_stage(stage) for r in ok)
        share = sum(vals) / total_sum if total_sum else 0.0
        table.add_row(
            stage,
            ms(percentile(vals, 50)),
            ms(percentile(vals, 95)),
            ms(vals[-1]),
            f"{share:.0%}",
        )
    took = sorted(statistics.median(s.took_ms for s in r.samples) for r in ok)
    table.add_row(
        "  es took",
        f"{percentile(took, 50):.0f}",
        f"{percentile(took, 95):.0f}",
        f"{took[-1]:.0f}",
        "",
        style="dim",
    )
    sums = sorted(r.median_total() for r in ok)
    e2es = sorted(r.median_e2e() for r in ok)
    table.add_row(
        "sum",
        ms(percentile(sums, 50)),
        ms(percentile(sums, 95)),
        ms(sums[-1]),
        "",
        style="bold",
    )
    table.add_row(
        "e2e",
        ms(percentile(e2es, 50)),
        ms(percentile(e2es, 95)),
        ms(e2es[-1]),
        "",
        style="bold",
    )
    console.print(table)
    # The e2e call does not serialize a response, so compare against the other stages.
    drift = [
        (r.median_total() - r.median_stage("serialize")) / r.median_e2e() - 1.0
        for r in ok
        if r.median_e2e() > 0
    ]
    if drift:
        console.print(
            f"Stage sum vs e2e: median {statistics.median(drift):+.1%}, "
            f"worst {max(drift, key=abs):+.1%}"
        )


def print_batches(results: list[CaseResult], batches: list[dict[str, Any]]) -> None:
    if not batches:
        return
    by_id = {r.case.id: r for r in results}
    table = Table(title="Concurrent batches vs sequential e2e (ms)")
    table.add_column("batch")
    table.add_column("n", justify="right")
    table.add_column("wall", justify="right")
    table.add_column("seq sum", justify="right")
    table.add_column("max e2e", justify="right")
    table.add_column("wall/max", justify="right")
    for idx, batch in enumerate(batches):
        members = [by_id[c] for c in batch["cases"] if by_id[c].e2e]
        if not members:
            continue
        seq = sum(r.median_e2e() for r in members)
        worst = max(r.median_e2e() for r in members)
        table.add_row(
            str(idx),
            str(len(batch["cases"])),
            ms(batch["wall"]),
            ms(seq),
            ms(worst),
            f"{batch['wall'] / worst:.2f}x",
        )
    console.print(table)


def to_json(
    results: list[CaseResult], batches: list[dict[str, Any]], meta: dict[str, Any]
) -> dict[str, Any]:
    return {
        "meta": meta,
        "cases": [
            {
                "id": r.case.id,
                "tags": r.case.tags,
                "error": r.error,
                "hits": r.samples[-1].hits if r.samples else None,
                "matches": r.samples[-1].matches if r.samples else None,
                "samples": [
                    {"stages": s.stages, "took_ms": s.took_ms} for s in r.samples
                ],
                "e2e": r.e2e,
            }
            for r in results
        ],
        "batches": batches,
    }


@click.group()
def cli() -> None:
    """In-process benchmark for the yente /match pipeline."""


@cli.command()
@click.option(
    "--queries",
    type=click.Path(exists=True, path_type=Path),
    default=DEFAULT_QUERIES,
    show_default=True,
)
@click.option("--dataset", default="default", show_default=True)
@click.option("--algorithm", default=settings.DEFAULT_ALGORITHM, show_default=True)
@click.option("--limit", default=settings.MATCH_PAGE, show_default=True)
@click.option("--threshold", default=settings.SCORE_THRESHOLD, show_default=True)
@click.option(
    "--cutoff",
    default=None,
    type=float,
    help="Defaults to --threshold, like the router.",
)
@click.option(
    "--repeat",
    default=3,
    show_default=True,
    help="Sequential runs per query; medians are reported.",
)
@click.option(
    "--batch",
    default=1,
    show_default=True,
    help="Also run the set in concurrent groups of this size.",
)
@click.option(
    "--tag", "tags", multiple=True, help="Only run queries carrying any of these tags."
)
@click.option(
    "--profile",
    type=click.Choice(["text", "html"]),
    default=None,
    help="Profile the sequential run with pyinstrument.",
)
@click.option(
    "--profile-out",
    type=click.Path(path_type=Path),
    default=None,
    help="Where to write the HTML profile.",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=None,
    help="Write raw timings and metadata as JSON.",
)
def run(
    queries: Path,
    dataset: str,
    algorithm: str,
    limit: int,
    threshold: float,
    cutoff: float | None,
    repeat: int,
    batch: int,
    tags: tuple[str, ...],
    profile: str | None,
    profile_out: Path | None,
    output: Path | None,
) -> None:
    """Time every stage of the match pipeline for each query in the set."""
    if profile == "html" and profile_out is None:
        raise click.UsageError("--profile html requires --profile-out")
    cases = load_cases(queries, tags)
    if not cases:
        raise click.UsageError("No queries selected.")
    if cutoff is None:
        cutoff = threshold
    opts = Options(
        dataset=dataset,
        algorithm=algorithm,
        limit=limit,
        threshold=threshold,
        cutoff=min(cutoff, threshold),
        repeat=repeat,
        batch=batch,
        profile=profile,
        profile_out=profile_out,
    )
    results, batches, meta = asyncio.run(run_async(cases, opts))
    console.print(
        f"[dim]{meta['git_sha']} yente {meta['yente_version']} · {', '.join(meta['indices'])} · "
        f"algorithm={meta['algorithm_resolved']} candidates={meta['candidates']} "
        f"repeat={meta['repeat']}[/dim]"
    )
    print_cases(results)
    print_summary(results)
    print_batches(results, batches)
    if output is not None:
        output.write_text(json.dumps(to_json(results, batches, meta), indent=2))
        console.print(f"Wrote {output}")
    if any(r.error for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    cli()
