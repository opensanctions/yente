# Match performance bench

In-process benchmark for the `/match` pipeline. It calls the same functions the
router calls, one stage at a time, with a wall clock around each, so you can see
where the time of a match request goes without an HTTP server in the way.

## Prerequisites

- A local Elasticsearch or OpenSearch with a yente index of the dataset you want
  to match against (the full `default` collection is the realistic case).
  `curl -s localhost:9200/_cat/aliases` should list `yente-entities`.
- `YENTE_MANIFEST` pointing at a manifest that contains that dataset, plus any
  other `YENTE_*` connection settings. The repo `.envrc` does this for local dev.
- For `--profile`: `pyinstrument` installed in the environment you run with.
  It is in the `dev` dependency group.

## Running

```bash
python contrib/perf_bench/perf_bench.py run
python contrib/perf_bench/perf_bench.py run --tag many-names --repeat 5
python contrib/perf_bench/perf_bench.py run --no-fuzzy --output /tmp/nofuzzy.json
python contrib/perf_bench/perf_bench.py run --batch 5
python contrib/perf_bench/perf_bench.py run --tag dense --profile text
python contrib/perf_bench/perf_bench.py run --profile html --profile-out /tmp/profile.html
```

Each query runs `--repeat` times sequentially and the median is reported. Every
query gets one untimed pass first, on top of the catalog and matcher warm-up
the app itself does at startup. The first touch of a query's terms in ES can
cost ten times the warm latency, so the numbers here are warm-cache numbers;
cold-cache behaviour is a different measurement. Request parameters (`--limit`,
`--threshold`, `--cutoff`, `--algorithm`) default to what the router uses, and
the candidate window is derived from `--limit` the same way.

## Reading the output

Per query, in milliseconds:

| column | what it measures |
|---|---|
| `from_example` | `Entity.from_example`: property cleaning, name analysis, country hints |
| `build_query` | `entity_query`: turning the entity into the ES query body |
| `search` | the `search_entities` round-trip, including transport and JSON decoding of the response |
| `took` | ES-reported server-side time for the same search; `search - took` is transport plus decode |
| `decode` | turning hits into `Entity` objects |
| `score` | `score_results`: one `algorithm.compare` per candidate |
| `serialize` | building the response models and dumping them to JSON, approximating FastAPI's response serialization |
| `sum` | sum of the stages above |
| `e2e` | a separate, untimed-inside call to the router's `_match_one_query` for the same query |

`e2e` does not include `serialize`, so `sum - serialize` should be close to
`e2e`. The summary prints the drift; a few percent is noise, more than that
means the stage replication has diverged from the router and needs a look.

The `decode` stage materialises the hit list before scoring. The router feeds
a lazy generator into `score_results`, so in production decode and compare
interleave; the total is the same.

`--batch N` additionally runs the query set in concurrent groups of N through
`asyncio.gather`, the way a batched request runs in the router, and prints the
batch wall time against the slowest member's sequential `e2e`. A ratio well
above 1.0 is the cost of concurrent CPU-bound scoring on one event loop.

## Comparing runs

`--output` writes raw per-repeat timings with metadata (git sha, yente version,
index names, fuzzy flag, candidate count). Run once per git tree or index build
and diff the summaries; the JSON is small enough to compare with `jq`.

## Query set

`queries.json` holds 100 tagged cases. Tags select a category with `--tag`,
and several tags may be combined (any match). Categories: `sparse`,
`many-names`, `scripts`, `dates`, `identifiers`, `addresses`, `company`,
`other-schema`, `dense`, `negatives`, `positives`, `long`. Each query also
carries a schema tag (`person`, `company`, `vessel`, ...) and `positive` or
`negative` where the expected outcome is known. Positives are real entities in
the OpenSanctions default collection, so they only resolve against an index of
that data.
