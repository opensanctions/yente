import click
import csv
import asyncio
import subprocess
import sys
from typing import List, Dict, Type, Union, Any
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
)
import statistics
from dataclasses import dataclass
import orjson

from yente.data.dataset import Dataset
from yente.provider.base import SearchProvider
from yente.routers.util import ENABLED_ALGORITHMS, get_algorithm_by_name
from yente.data.entity import Entity
from yente.data.common import EntityExample, ScoredEntityResponse
from yente.search.queries import entity_query
from yente.search.search import search_entities, result_entities
from yente.scoring import score_results
from yente.routers.util import get_dataset
from yente.provider import get_provider
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig
from yente import settings
from yente.logs import get_logger

log = get_logger(__name__)


@dataclass
class PersonRecord:
    """Represents a synthetic person record for screening tests."""

    full_name: str
    first_name: str
    middle_name: str | None
    last_name: str
    gender: str  # "female", "male", "other"
    date_of_birth: str
    place_of_birth: str
    nationality: str

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for CSV export."""
        return {
            "full_name": self.full_name,
            "first_name": self.first_name,
            "middle_name": self.middle_name or "",
            "last_name": self.last_name,
            "gender": self.gender,
            "date_of_birth": self.date_of_birth,
            "place_of_birth": self.place_of_birth,
            "nationality": self.nationality,
        }


def read_person_csv(file_path: str) -> List[PersonRecord]:
    """Read PersonRecord objects from a CSV file."""
    persons = []
    with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            person = PersonRecord(
                full_name=row.get("full_name", ""),
                first_name=row.get("first_name", ""),
                middle_name=row.get("middle_name") if row.get("middle_name") else None,
                last_name=row.get("last_name", ""),
                gender=row.get("gender", ""),
                date_of_birth=row.get("date_of_birth", ""),
                place_of_birth=row.get("place_of_birth", ""),
                nationality=row.get("nationality", ""),
            )
            persons.append(person)
    return persons


def person_to_entity_example(person: PersonRecord) -> EntityExample:
    """Convert a PersonRecord to an EntityExample for matching."""
    properties: Dict[str, Union[str, List[Any]]] = {
        "name": [person.full_name],
        "birthDate": [person.date_of_birth],
        "nationality": [person.nationality],
    }
    if person.first_name:
        properties["firstName"] = [person.first_name]
    if person.last_name:
        properties["lastName"] = [person.last_name]

    if person.middle_name:
        properties["middleName"] = [person.middle_name]

    if person.place_of_birth:
        properties["birthPlace"] = [person.place_of_birth]

    if person.gender:
        properties["gender"] = [person.gender]

    return EntityExample(
        id=None,
        schema="Person",
        properties=properties,
    )


async def benchmark_person(
    person: PersonRecord,
    algorithms: List[Type[ScoringAlgorithm]],
    dataset: Dataset,
    provider: SearchProvider,
) -> Dict[Type[ScoringAlgorithm], List[ScoredEntityResponse]]:
    """Benchmark a person against multiple algorithms.

    Args:
        person: The PersonRecord to benchmark
        algorithms: List of algorithm classes to test
        dataset: The dataset to search in
        provider: The search provider

    Returns:
        Dict mapping algorithm classes to lists of ScoredEntityResponse
    """
    # Convert PersonRecord to EntityExample
    example = person_to_entity_example(person)
    entity = Entity.from_example(example)

    # Generate candidates using entity_query
    query = entity_query(dataset, entity)

    # Use the same limit for candidate generation as in match.py
    candidates_limit = settings.MATCH_PAGE * settings.MATCH_CANDIDATES
    response = await search_entities(provider, query, limit=candidates_limit)
    candidates = list(result_entities(response))

    # Score results with each algorithm
    results: Dict[Type[ScoringAlgorithm], List[ScoredEntityResponse]] = {}

    for algorithm in algorithms:
        # Use default scoring config
        config = ScoringConfig.defaults()

        # Score the candidates
        total, scored = score_results(
            algorithm,
            entity,
            candidates,
            threshold=settings.SCORE_THRESHOLD,  # Same as match.py
            cutoff=-1.0,  # Return all results, no cutoff
            limit=settings.MATCH_PAGE,  # Same as match.py
            config=config,
        )
        # if not scored[0].match:
        #     print(example)

        results[algorithm] = scored

    return results


def calculate_algorithm_statistics(
    results: List[
        tuple[PersonRecord, Dict[Type[ScoringAlgorithm], List[ScoredEntityResponse]]]
    ],
) -> Dict[str, Any]:
    """Calculate statistics for each algorithm across all persons."""
    assert results, "No results to calculate statistics from"

    # Get all algorithms from the first result
    algorithms = list(results[0][1].keys())
    stats: Dict[str, Any] = {"total_persons": len(results)}

    for algorithm in algorithms:
        top_scores = []
        median_scores = []
        lowest_scores = []
        candidate_counts = []
        empty_result_count = 0
        persons_with_matches = 0

        # Collect top 5 person names with their scores for this algorithm
        top_persons_with_scores = []

        for person, person_results in results:
            result_list = person_results[algorithm]
            candidate_counts.append(len(result_list))

            if not result_list:
                # Use 0 for empty result lists
                top_scores.append(0.0)
                median_scores.append(0.0)
                lowest_scores.append(0.0)
                empty_result_count += 1
            else:
                scores = [result.score for result in result_list]
                max_score = max(scores)
                top_scores.append(max_score)
                median_scores.append(statistics.median(scores))
                lowest_scores.append(min(scores))

                # Store person name and their top score for ranking
                top_persons_with_scores.append((person.full_name, max_score))

                # Check if any result has match set
                if any(
                    hasattr(result, "match") and result.match for result in result_list
                ):
                    persons_with_matches += 1

        # Get top 5 person names by score
        top_persons_with_scores.sort(key=lambda x: x[1], reverse=True)
        top_5_names_with_scores = [
            (name, score) for name, score in top_persons_with_scores[:5]
        ]

        stats[algorithm.NAME] = {
            "top_mean": statistics.mean(top_scores),
            "median_mean": statistics.mean(median_scores),
            "lowest_mean": statistics.mean(lowest_scores),
            "empty_result_count": empty_result_count,
            "persons_with_matches": persons_with_matches,
            "total_persons": len(results),
            "top_5_names_with_scores": top_5_names_with_scores,
        }

    return stats


async def benchmark_async(
    person_file: Path, matchers: tuple[str, ...], dataset_name: str
) -> List[
    tuple[PersonRecord, Dict[Type[ScoringAlgorithm], List[ScoredEntityResponse]]]
]:
    """Benchmark candidate generation with specified algorithms."""
    # Read the person file into an iterable called person of PersonRecord
    persons = read_person_csv(str(person_file))[:100]

    # Get algorithm classes from names
    algorithms = [get_algorithm_by_name(matcher) for matcher in matchers]

    # Get dataset and provider
    dataset = await get_dataset(dataset_name)
    provider = await get_provider()

    # Benchmark each person with progress bar
    results = []
    total_persons = len(persons)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=Console(),
        transient=True,
    ) as progress:
        task = progress.add_task(
            f"Processing {person_file.name}...", total=total_persons
        )

        for person in persons:
            person_results = await benchmark_person(
                person, algorithms, dataset, provider
            )
            results.append((person, person_results))
            progress.advance(task)

    return results


@click.group()
def cli() -> None:
    """Benchmark candidate generation with specified algorithms."""
    pass


@cli.command()
@click.option(
    "--person-file",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="CSV file containing PersonRecord data",
)
@click.option(
    "--matcher",
    type=click.Choice([algo.NAME for algo in ENABLED_ALGORITHMS]),
    required=True,
    help="Algorithm name to use for matching",
)
@click.option(
    "--dataset",
    default="sanctions",
    help="Dataset to use for matching (default: sanctions)",
)
def run(person_file: Path, matcher: str, dataset: str) -> None:
    """Run benchmark and output JSON results."""
    # Run the async benchmark function
    results = asyncio.run(benchmark_async(person_file, (matcher,), dataset))

    # Calculate statistics
    stats = calculate_algorithm_statistics(results)

    # Print JSON to stdout
    print(orjson.dumps(stats, option=orjson.OPT_INDENT_2).decode())


@cli.command()
@click.option(
    "--person-file",
    type=click.Path(exists=True, path_type=Path),
    multiple=True,
    required=True,
    help="CSV file(s) containing PersonRecord data",
)
@click.option(
    "--matcher",
    type=click.Choice([algo.NAME for algo in ENABLED_ALGORITHMS]),
    required=True,
    help="Algorithm name to use for matching",
)
@click.option(
    "--git-tree",
    multiple=True,
    required=True,
    help="Git tree-ish to checkout before running benchmark",
)
@click.option(
    "--dataset",
    default="sanctions",
    help="Dataset to use for matching (default: sanctions)",
)
def benchmark(
    person_file: tuple[Path, ...], matcher: str, git_tree: tuple[str, ...], dataset: str
) -> None:
    """Benchmark candidate generation with specified algorithms."""
    console = Console()

    # Store results for each combination of person file and git tree
    results_matrix: Dict[str, Dict[str, Dict[str, float]]] = {}

    # Process each person file
    for file_path in person_file:
        results_matrix[file_path.name] = {}

        # Process each git tree
        for tree in git_tree:
            console.print(
                f"\n[bold blue]Processing {file_path.name} with git tree {tree}[/bold blue]"
            )

            # Checkout the git tree
            try:
                subprocess.run(
                    ["git", "checkout", tree],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                console.print(f"[green]Checked out git tree: {tree}[/green]")
            except subprocess.CalledProcessError as e:
                console.print(f"[red]Error checking out git tree {tree}: {e}[/red]")
                console.print(f"[red]stderr: {e.stderr}[/red]")
                continue

            # Run the run subcommand as a subprocess
            cmd = [
                sys.executable,
                __file__,
                "run",
                "--person-file",
                str(file_path),
                "--matcher",
                matcher,
                "--dataset",
                dataset,
            ]

            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)

                # Parse the JSON output
                stats = orjson.loads(result.stdout)

                # Store the mean top score for this combination
                results_matrix[file_path.name][tree] = stats[matcher]

                console.print(
                    f"[green]Completed benchmark for {file_path.name} with {tree}[/green]"
                )

            except subprocess.CalledProcessError as e:
                console.print(f"[red]Error running subprocess: {e}[/red]")
                console.print(f"[red]stderr: {e.stderr}[/red]")
                exit(1)
            except Exception as e:
                console.print(f"[red]Error parsing results: {e}[/red]")
                exit(1)

    # Visualize the results matrix
    visualize_results_matrix(results_matrix, matcher, dataset, console)


def visualize_results_matrix(
    results_matrix: Dict[str, Dict[str, Dict[str, float]]],
    matcher: str,
    dataset: str,
    console: Console,
) -> None:
    """Visualize benchmark results as a matrix with git trees as columns and person files as rows."""
    if not results_matrix:
        console.print("No results to display")
        return

    # Get all git trees from the results
    all_trees = list(results_matrix.values())[0].keys()

    # Create table
    table = Table(
        title=f"Algorithm Performance Comparison\nDataset: {dataset}\nMatcher: {matcher}",
    )

    # Add columns
    table.add_column("Person File", justify="left", style="bold", no_wrap=True)
    for tree in all_trees:
        table.add_column(tree, justify="right", ratio=1)

    # Add rows for each person file
    for file_name, tree_results in results_matrix.items():
        row_data = [file_name]
        for tree in all_trees:
            results = tree_results.get(tree) or {}
            row_data.append(
                f"{results['top_mean']:.3f} ({results['persons_with_matches']}/{results['total_persons']})"
            )
        table.add_row(*row_data)

    console.print(table)


if __name__ == "__main__":
    cli()
