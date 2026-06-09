# Monitoring yente

We recommend you monitor your production deployment of yente and set up alerting in order to make sure your deployment is operational and serving up-to-date data.

## Service health endpoints

yente provides standard health check endpoints:

* `/healthz`: Returns `200 OK` if the Python application is responsive. Use this for basic liveness probes.
* `/readyz`: Returns `200 OK` if the search index is available and searchable. Use this for readiness probes to ensure the service doesn't receive traffic before the initial indexing is complete.

Note that `/readyz` will return `200 OK` even if the index is stale, as long as it is searchable. Read on for how to monitor data freshness.

## Metrics & traces using OpenTelemetry

Yente is instrumented with a fairly standard OpenTelemetry setup that exports metrics such as HTTP response times & codes. To enable it, run yente via the `opentelemetry-instrument` wrapper — see the [OpenTelemetry Python zero-code instrumentation docs](https://opentelemetry.io/docs/zero-code/python/) for the full list of configuration options.

Override the default command in your `docker-compose.yml`:

```yaml
services:
  app:
    command: ["opentelemetry-instrument", "yente", "serve"]
    environment:
      # Only enable if you want traces
      OTEL_TRACES_EXPORTER: none
```

In many cloud deployment scenarios you'll probably want to run an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) as a sidecar to handle your provider's quirks — batching, retries, authentication headers, and format translation.

### Monitoring index freshness

In addition to the standard OpenTelemetry instrumentation, yente exports a gauge that lets you alert on stale index data:

* `yente.data.indexed_dataset_version_time` — Unix timestamp (seconds) of the `last_export` of the dataset currently loaded into the index. Carries a `dataset` label.

If you want to alert when the index is older than some threshold, compare the gauge against the current time. As an example, here's what such an alert looks like on Google Cloud Managed Service for Prometheus, alerting on the `default` dataset being older than 12h:

```promql
time() - {"__name__"="yente.data.indexed_dataset_version_time", dataset="default"} > 12 * 60 * 60
```

Other monitoring solutions will likely express the same thing slightly differently.

## Monitoring catalog and index freshness via `/catalog`

If you don't have an OpenTelemetry-based monitoring stack, you can also monitor catalog and index freshness by polling the `/catalog` endpoint. It provides information about the datasets configured in your instance and their current indexing status. If you're running yente with the
default configuration, indexing the [default collection]({{ config.extra.opensanctions_url }}/datasets/default/)
 usually looks something like this:

```json
{
  "datasets": [
    {
      "name": "default",
      "title": "OpenSanctions",
      "updated_at": "2024-01-27T12:54:18",
      "version": "20260127125418-igl",
      "index_version": "20260127125418-igl",
      "index_current": true,
      "load": true
      // ...
    },
  ],
  "current": ["default"],
  "outdated": [],
  "index_stale": false
}
```

A few fields are of interest here:

* `updated_at`: The timestamp when the dataset was last updated, i.e. the timestamp of the latest available version in the catalog.
* `version`: The version identifier for the latest available data for this dataset.
* `index_version`: The version identifier of the data currently loaded in the search index.
* `index_current`: A boolean indicating if `index_version` matches `version`.
* `index_stale`: If any datasets configured to be indexed have `index_current: false`, `index_stale` will be `true` and those datasets will be in `outdated`

For monitoring purposes, we suggest you do the following:

- Check that `updated_at` is at most X time old. This will alert you if the catalog isn't being updated anymore (or the default collection hasn't published any new data in that time). We suggest 24 hours to account for any transient issues.
- Check if `index_stale`. This will let you know if something is preventing new data from being indexed and made searchable.

Note that both are required: if catalog updates are broken, yente will never know that a new version of a dataset has been published, and consequently never mark the index as stale.
