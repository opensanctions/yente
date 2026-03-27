## How `/match` works

The `/match` endpoint does roughly the following for each query in a request:

1. Retrieve a set of **candidates** from Elasticsearch. The number of candidates is `limit × YENTE_MATCH_CANDIDATES` (by default 5 × 10 = 50).
2. Score each candidate against the query using the selected algorithm.
3. Return the top-scoring candidates in the response.

While Elasticsearch contributes to latency, in practice it scales well and is not the bottleneck in how many requests a yente deployment can handle. `/match` is CPU-bound — scoring candidates is the expensive part.

## What affects throughput

How many requests a single yente instance can handle depends on several factors:

- **Scoring algorithm:** `logic-v2` produces better results but is about four times slower than `logic-v1`.
- **`limit`:** The `limit` parameter to the `/match` endpoint controls how many results you request. More results means more candidates to score.
- **`YENTE_MATCH_CANDIDATES`:** How many candidates are retrieved and scored per result. We use 10 by default to ensure the best results, but users have reported acceptable results going down to 3.
- **The workload itself:** `YENTE_MATCH_CANDIDATES` sets an upper limit on candidates scored - but there may be less. For very specific queries or rare names, the number of candidates may be fewer than this upper bound. Less specific queries, common first names, generic company names — almost always return the full set of candidates and are therefore slower.

## Rough throughput numbers

The following numbers provide rough guidance for a single GCE N4 vCPU:

| Configuration | Requests per second |
|---|---|
| `logic-v2`, `YENTE_MATCH_CANDIDATES=10` (default) | ~4 |
| `logic-v1`, `YENTE_MATCH_CANDIDATES=10` | ~15 |
| `logic-v1`, `YENTE_MATCH_CANDIDATES=3` | ~30 |

## What affects latency

All the factors that affect throughput also affect latency — the scoring algorithm, `limit`, `YENTE_MATCH_CANDIDATES`, and the specificity of the workload.

Beyond that, two additional factors matter:

- **Concurrency:** If multiple requests arrive at a yente instance at the same time, they are processed concurrently and latency increases. If you need latency to be reliably low, deploy enough instances so that an idle one is always available to serve a request.
- **Batching:** Each request runs on a single CPU core. Do not batch queries into one request — send them as separate requests so they can be processed in parallel across multiple instances of yente.

## Practical scaling recommendations

We recommend following the standard cloud paradigm: deploy containers with one vCPU each behind a load balancer. Use the throughput numbers above to estimate how many instances you need for your expected request volume, and allow for some headroom. If your traffic is bursty, configure a horizontal autoscaler.

At OpenSanctions, we use Google Cloud Run for this. Cloud Run supports scaling on both CPU utilization and `max_instance_request_concurrency`, which allows the load balancer to quickly spawn new instances during request spikes. Set `startup_cpu_boost` to allow new instances to start up as quickly as possible.
