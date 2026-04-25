---
description: Heuristics to reduce the number of candidates scored in the /match pipeline
date: 2026-04-07
tags: [scoring, performance, matching, issue-1011]
---

# Early stopping heuristics for candidate scoring

GitHub issue: opensanctions/yente#1011

## Problem

The `/match` endpoint retrieves `limit * MATCH_CANDIDATES` (default 5 * 10 = 50) candidates
from Elasticsearch and scores **every one** with the full algorithm (LogicV2). Users can
request up to 500 results, meaning up to 5,000 scoring calls per query. The scoring algorithm
itself isn't terribly slow — yente just invokes it far too often on candidates that will never
make it into the response.

## Research data

Analysis of three production log samples (30,000 rows, ~20,800 valid scoring entries, 418
unique queries, 2026-04-07). Mean ~50 candidates scored per query.

### Most scoring work is wasted

| Metric | Value |
|---|---|
| Total scoring calls | 20,772 |
| Scores < 0.5 (below cutoff) | 82.2% |
| Scores < 0.3 (clearly wasted) | 47.9% |
| Scores >= 0.7 (match threshold) | 1.0% |
| Queries with zero candidates >= 0.5 | 49.3% |
| Queries with zero candidates >= 0.7 | 84.4% |

About half of all queries produce no candidates above 0.5, and 84% produce no matches
(>= 0.7). Yet we score all ~50 candidates for every query.

### ES ranking vs algo score correlation

ES ranking is a **weak** predictor of algo score. The best algo-scored result appears at:

| Within top N ES results | % of queries |
|---|---|
| Top 1 | 23.2% |
| Top 3 | 35.9% |
| Top 5 | 44.7% |
| Top 10 | 63.4% |
| Top 20 | 83.3% |
| Top 50 | 98.3% |

Mean algo score by ES rank bucket (ranks 0-49 contain the bulk of data):

| ES rank bucket | Count | Mean algo score | % with algo >= 0.5 |
|---|---|---|---|
| 0-9 | 4,056 | 0.358 | 26.9% |
| 10-19 | 4,092 | 0.326 | 19.5% |
| 20-29 | 3,957 | 0.310 | 18.0% |
| 30-39 | 3,818 | 0.311 | 16.5% |
| 40-49 | 3,737 | 0.297 | 12.2% |
| 50+ | 1,112 | ~0.19 | 0.0% |

Key observation: within the first 50 candidates, algo scores decline gently (0.36 → 0.30
mean) but good results appear at every rank. ES does a good job excluding truly irrelevant
candidates (rank 50+), but within the top 50 it cannot reliably distinguish good from bad.

### Early stopping simulation

"Stop scoring after N consecutive candidates with algo score below threshold":

| Threshold | Patience | Scoring calls saved | Meaningful best results missed (out of 418) |
|---|---|---|---|
| 0.3 | 3 | 50.8% | 22 |
| 0.3 | 5 | 42.0% | 12 |
| 0.3 | 7 | 36.9% | 9 |
| 0.3 | 10 | 31.8% | 5 |
| 0.3 | 15 | 23.0% | 4 |

Simple early stopping with patience=10 saves ~32% of scoring calls and misses 5 out of
418 queries (1.2%).

### Adaptive patience

When a query has already produced a score above a trigger value, increase patience to
avoid cutting off queries that have real matches buried deeper in the candidate list:

| Base patience | Boosted patience | Trigger | Saved | Missed (out of 418) |
|---|---|---|---|---|
| 5 | 10 | >= 0.4 | 33.3% | 7 |
| 5 | 15 | >= 0.4 | 30.6% | 6 |
| 5 | 20 | >= 0.4 | 27.9% | 5 |
| 5 | 25 | >= 0.4 | 27.0% | 5 |

Adaptive patience helps: queries with no real matches stop early (patience=5, saves the
most work), while queries with promising candidates keep looking longer. The approach
`base=5, boost=20, trigger>=0.4` saves ~28% of scoring calls and misses 5 out of 418
queries (1.2%).

### Missed results profile

With the recommended adaptive settings (base=5, boost=20, trigger>=0.4, min_candidates=10),
the 5 missed results are:

| Best score | At ES rank | Stopped after | Total candidates |
|---|---|---|---|
| 0.667 | 31 | 16 | 49 |
| 0.583 | 9 | 12 | 46 |
| 0.565 | 21 | 10 | 49 |
| 0.543 | 23 | 10 | 48 |
| 0.512 | 43 | 10 | 97 |

These are all sub-threshold results (< 0.7) that would appear in the response list with
`match: false`. The highest missed score is 0.667. For screening use cases where only
`match: true` matters, the quality impact is effectively zero.

### Index score floor

Adding a minimum ES index score before scoring a candidate provides marginal benefit:

| Index score floor | Candidates scored | Good results missed (algo >= 0.5) |
|---|---|---|
| >= 5 | 96.7% | 0 |
| >= 10 | 81.3% | 2 |
| >= 15 | 39.4% | 7 |

Since most candidates already have index_score > 5, this doesn't help much. The early
stopping heuristic is more effective.

### Why MATCH_CANDIDATES=10 is correct (and not the right lever)

The 10x multiplier controls **recall** — how many ES candidates we fetch to ensure the
best algo-scored result is in the pool. The data shows it's well-calibrated:

| MATCH_CANDIDATES equivalent | ES top N (limit=5) | Best result found |
|---|---|---|
| 1x | Top 5 | 44.7% |
| 2x | Top 10 | 63.4% |
| 4x | Top 20 | 83.3% |
| **10x** | **Top 50** | **98.3%** |

Reducing the multiplier would lose real results. And within the 50-candidate window, good
results are spread across all rank buckets — there's no safe truncation point:

| ES rank bucket | % with algo >= 0.5 |
|---|---|
| 0-9 | 26.9% |
| 10-19 | 19.5% |
| 20-29 | 18.0% |
| 30-39 | 16.5% |
| 40-49 | 12.2% |

However, **49.3% of queries have zero candidates above 0.5**. For those queries, the
multiplier is pure waste — we fetch and score 50 candidates to return nothing. The
multiplier is calibrated for the ~50% of queries where matches exist, and the other ~50%
pay the full cost for no benefit.

The multiplier and early stopping solve different problems: the multiplier controls
**recall** (keep it at 10x), early stopping controls **wasted compute** (stop scoring
when it's clearly pointless). Together they preserve result quality while cutting scoring
work by ~28%.

## Proposed approach

### Consecutive-low early stopping with adaptive patience

Add early stopping logic to `score_results()` in `yente/scoring.py`. After scoring each
candidate, track how many consecutive candidates have scored below a low threshold. Once
patience is exhausted, stop scoring remaining candidates. When a promising score has been
seen, multiply patience by a boost factor to keep searching.

Most values are derived from the per-request `threshold` parameter rather than being
independent settings:

- **Early stop threshold** = `threshold * 0.4` (scores below this count as "low")
- **Boost trigger** = `threshold * 0.6` (score that switches to boosted patience)
- **Min candidates** = `limit` (always score at least as many as requested)
- **Boosted patience** = `patience * 4`

This leaves one setting: `SCORE_EARLY_STOP_PATIENCE` (default 5, env-configurable).
Set to a large value (e.g., 9999) to effectively disable early stopping.

```python
EARLY_STOP_BOOST_FACTOR = 4

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
    consecutive_low = 0
    seen_promising = False
    patience = settings.SCORE_EARLY_STOP_PATIENCE
    early_stop_threshold = threshold * 0.4
    boost_trigger = threshold * 0.6
    min_candidates = limit or 0
    for rank, (result, index_score) in enumerate(results):
        scoring = algorithm.compare(query=entity, result=result, config=config)
        # ... existing logging and sleep ...
        response = ScoredEntityResponse.from_entity_result(result, scoring, threshold)

        if response.score > early_stop_threshold:
            consecutive_low = 0
        else:
            consecutive_low += 1

        if response.score >= boost_trigger:
            seen_promising = True

        if response.score <= cutoff:
            continue
        if response.match:
            matches += 1
        scored.append(response)

        effective_patience = (
            patience * EARLY_STOP_BOOST_FACTOR if seen_promising
            else patience
        )
        if consecutive_low >= effective_patience and rank >= min_candidates:
            break

    scored = sorted(scored, key=lambda r: r.score, reverse=True)
    if limit is not None:
        scored = scored[:limit]
    return matches, scored
```

Note: the `consecutive_low` counter and `seen_promising` flag are updated before the
`cutoff` filter — a candidate that's below `cutoff` but above the early-stop threshold
should still reset the counter.

### Settings

One new setting in `yente/settings.py`:

```python
SCORE_EARLY_STOP_PATIENCE: int = 5
```

Configurable via `YENTE_SCORE_EARLY_STOP_PATIENCE` environment variable.

At default threshold (0.7) this yields:
- Early stop threshold: 0.28
- Boost trigger: 0.42
- Base patience: 5
- Boosted patience: 20

## Testing

- Unit tests: mock algorithm that returns predetermined scores; verify early stopping
  triggers at the right rank and that results are not lost.
- Compare `/match` output with and without early stopping on a representative query set
  to validate that result quality is preserved.

## Risks

- **Missed results**: With adaptive patience (base=5, boosted=20, trigger=0.42), the
  simulation shows ~5 missed results out of 418 queries (1.2%). All are sub-threshold
  (highest is 0.667, below the 0.7 match threshold). For screening use cases where only
  `match: true` matters, the quality impact is effectively zero.
- **Query-dependent behavior**: Some entity types or datasets may have different score
  distributions. Deriving thresholds from the per-request `threshold` parameter mitigates
  this — users with a lower threshold automatically get less aggressive early stopping.
- **Sensitivity to candidate ordering**: Early stopping depends on ES returning candidates
  in a roughly score-correlated order. If ES ranking degrades (e.g., after index changes),
  more good results could be missed. The boosted patience provides a buffer for queries
  where ES and algo scoring clearly diverge.

## Follow-up: raising MATCH_CANDIDATES

Once early stopping is in place, the cost model changes: fetching more candidates from ES
is cheap, and early stopping caps how many actually get scored. This makes it tempting to
raise MATCH_CANDIDATES (currently 10) as insurance against the weak ES/algo correlation.

**The data doesn't strongly justify it.** Queries in our sample that fetched beyond 50
candidates show 0% with algo >= 0.5 past rank 50 — ES relevance drops off hard. And 98.3%
of best results already fall within the top 50. The remaining 1.7% have best scores below
0.5 (not meaningful misses).

**The ES/algo divergence is real but bounded.** Per-query Spearman correlation between
index_score and algo_score has a median of 0.42, with 21.7% of queries showing negative
correlation. Top-5 overlap between ES and algo rankings is only 35%. The worst observed
inversion: best algo result (0.592) at ES rank 153. However, even in these worst cases the
buried results are sub-threshold (< 0.7). The ES query construction (name boosting,
fuzziness, phonetic matching) would have to substantially fail for a true match to land
beyond rank 50.

**Recommendation:** Ship early stopping first and measure in production. If the miss rate
is acceptable, a modest bump (e.g., to 15x) is cheap insurance and worth trying — but
don't expect a measurable quality improvement based on what we see today.
