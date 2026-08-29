# Production deployment checklist

Self-hosting yente means operating it as a production service. We provide the software, the data, and detailed deployment documentation, but the running system is yours to manage. Before going live, work through the items below and budget engineering time accordingly.

## Provision suitable infrastructure

yente ships as two Docker containers: the API application and an ElasticSearch search index. Both can run on a single virtual machine for low-volume workloads, on a Kubernetes cluster for larger production deployments, or on a managed container platform such as Google Cloud Run. We run the [hosted OpenSanctions API]({{ config.extra.opensanctions_url }}/api/) on Cloud Run. The right choice depends on your expected traffic, your latency requirements, and what your team already operates.

We recommend running yente on Linux, either as a virtual machine or a managed container platform. yente has been made to work on Windows hosts, but we do not test against that environment and discourage it for production deployments.

See [Deploying yente](index.md) for example configurations for `docker-compose` and Kubernetes.

## Deploy and configure the service

Configuration happens through [environment variables](../settings.md) and a [manifest file](../datasets.md) that controls which datasets are loaded. In production we recommend separating the periodic reindexing job from the API worker pool, so that a failure in indexing cannot take down the search service. The [example Kubernetes configuration](https://github.com/opensanctions/yente/blob/main/kubernetes.example.yml) shows this pattern with `YENTE_AUTO_REINDEX` disabled on the workers and a separate `CronJob` running `yente reindex`.

## Load and refresh data

A fresh deployment needs around 20 minutes for the initial index of the default collection. After that, yente checks for new OpenSanctions releases every 30 minutes and reindexes when a new version is published. You should verify that the initial load completed and that subsequent reindex runs are succeeding. The `/catalog` endpoint reports the version of data currently in your index against the latest available version. See [Managing data updates](../reindex.md) for the refresh mechanics and [Monitoring yente](monitoring.md) for the fields to alert on.

## Secure the service

yente does not include authentication or rate-limiting; it expects to sit behind a load balancer, API gateway, or reverse proxy that enforces access control. The ElasticSearch index must never be exposed to untrusted networks; the default [`docker-compose.yml`](https://github.com/opensanctions/yente/blob/main/docker-compose.yml) binds it to `127.0.0.1` for that reason.

If you need to run inside a restrictive firewall, the [restrictive firewalls](firewall.md) guide covers how to keep data fresh without giving the container outbound internet access.

If you discover a vulnerability in yente itself, please follow our [responsible disclosure policy]({{ config.extra.opensanctions_url }}/docs/security/).

## Monitor availability and data freshness

Production deployments need two kinds of monitoring:

- **Service health.** yente exposes `/healthz` for liveness probes and `/readyz` for readiness probes. Wire these into your container orchestrator and external uptime monitoring.
- **Data freshness.** Poll the `/catalog` endpoint and alert when `index_stale` is `true` or when `updated_at` falls more than 24 hours behind. Without this check, a stalled indexer can serve outdated screening results indefinitely.

yente is also instrumented with OpenTelemetry, so request latency, error rates, and throughput can be exported to your existing observability stack. See [Monitoring yente](monitoring.md) for the recommended alerts and OpenTelemetry wiring.

## Schedule software upgrades

We release new versions of yente regularly to add features, evolve the data model, and improve the scoring algorithm. **Plan to upgrade at least twice a year.** Running a long-outdated version risks falling out of compatibility with the data model, which we evolve with a [published notice period]({{ config.extra.opensanctions_url }}/docs/data/changes/).

The [upgrade guide](upgrading.md) covers how to read the changelog, adapt your clients, and run a blue-green deployment for zero-downtime upgrades.

ElasticSearch has its own end-of-life cycle to plan around: version 8 reaches end-of-life in January 2027. Versions of yente released after May 2026 will only be compatible with Elasticsearch 9.

## Set disk and memory limits

The ElasticSearch index grows as new datasets are published and reindexes accumulate; without bounded disk space the node will eventually fail to write.

- Allocate at least 60 GB of disk for the index, more if you load custom datasets or run multiple index versions side by side for blue-green upgrades.
- Set explicit memory limits on both containers. The default `docker-compose.yml` reserves a 4 GB ElasticSearch heap; increase it for larger workloads.
- On Kubernetes, set resource `requests` and `limits` on both the API pods and the ElasticSearch nodes.
- Configure alerts on disk usage well below the partition's high-water mark. ElasticSearch enters read-only mode when its [flood-stage watermark](https://www.elastic.co/guide/en/elasticsearch/reference/current/disk-allocator.html) is hit, which will break reindexing before it breaks queries.

## Plan for traffic bursts

The `/match` endpoint is CPU-bound: scoring candidate entities is the bottleneck, not the search index. As a rough planning figure, assume around 50 ms of scoring time per request on the default `logic-v2` algorithm, which works out to roughly 20 requests per second per vCPU before queuing.

If your traffic is bursty (large overnight batch screens or periodic re-screening runs), deploy multiple yente instances behind a load balancer and configure horizontal autoscaling. Cloud Run supports scaling on both CPU utilization and `max_instance_request_concurrency`, which lets the load balancer spawn new instances quickly during request spikes.

See [Scaling yente](scaling.md) for concurrency, batching, and throughput numbers in detail.
