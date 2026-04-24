import time
import asyncio
from typing import Iterable, List, Optional, Type, Tuple
from opentelemetry import trace, metrics
from nomenklatura.matching.types import ScoringAlgorithm, ScoringConfig

from yente import settings
from yente.logs import get_logger
from yente.data.entity import Entity
from yente.data.common import ScoredEntityResponse

log = get_logger(__name__)

_tracer = trace.get_tracer("yente.scoring")
_meter = metrics.get_meter("yente.scoring")
_compare_duration = _meter.create_histogram(
    "yente.scoring.compare_duration",
    unit="s",
    description="Per-candidate algorithm.compare() duration",
)


async def score_results(
    algorithm: Type[ScoringAlgorithm],
    entity: Entity,
    results: Iterable[Tuple[Entity, float]],
    threshold: float = settings.SCORE_THRESHOLD,
    cutoff: float = 0.0,
    limit: Optional[int] = None,
    config: ScoringConfig = ScoringConfig.defaults(),
) -> Tuple[int, List[ScoredEntityResponse]]:
    scored: List[ScoredEntityResponse] = []
    matches = 0
    # Initialise outside the loop so it's defined when the iterable is empty.
    rank = -1
    algo_name = algorithm.__name__
    with _tracer.start_as_current_span("score_results") as span:
        span.set_attribute("scoring.algorithm", algo_name)
        span.set_attribute("scoring.query_schema", entity.schema.name)
        for rank, (result, index_score) in enumerate(results):
            start_score = time.perf_counter()
            scoring = algorithm.compare(query=entity, result=result, config=config)
            end_score = time.perf_counter()
            _compare_duration.record(end_score - start_score, {"algorithm": algo_name})
            # log.debug(
            #     "Scoring result %s" % result.id,
            #     query_schema=entity.schema.name,
            #     result_id=result.id,
            #     result_schema=result.schema.name,
            #     algorithm=algorithm.__name__,
            #     rank=rank,
            #     algo_score=scoring.score,
            #     index_score=index_score,
            #     time=end_score - start_score,
            # )
            # Yield control to the event loop
            # This might allow the event loop to process another request, resulting in
            # more even response times when CPU-bound scoring requests pile up.
            await asyncio.sleep(0)
            response = ScoredEntityResponse.from_entity_result(
                result, scoring, threshold
            )
            if response.score <= cutoff:
                continue
            if response.match:
                matches += 1
            scored.append(response)

        scored = sorted(scored, key=lambda r: r.score, reverse=True)
        if limit is not None:
            scored = scored[:limit]
        span.set_attribute("scoring.candidates", rank + 1)
        span.set_attribute("scoring.scored", len(scored))
        span.set_attribute("scoring.matches", matches)
        return matches, scored
