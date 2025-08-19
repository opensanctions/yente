---
hide:
  - toc
---

# Configuring yente

The Yente service is built to require a minimum of configuration, but several environment variables can be used to change its behavior.

## Index-related settings

| Env. variable | Default | Description |
| ------ | ------ | ------ |
| `YENTE_INDEX_URL`   | `http://index:9200`   | The URL of your search index provider backend. |
| `YENTE_INDEX_USERNAME` | - | Username for the search provider. **Required** if connection using Elastic Cloud. |
| `YENTE_INDEX_PASSWORD` | - | Elasticsearch password. **Required** if connection using Elastic Cloud. |
| `YENTE_INDEX_NAME`   | `yente`   | The prefix name that will be used for the search index. |
| `YENTE_INDEX_TYPE` | `elasticsearch` | Should be one of `elasticsearch` or [`opensearch`](opensearch.md), depending on what provider you use. |
| `YENTE_ELASTICSEARCH_CLOUD_ID`   | - | If you are using [Elastic Cloud](https://www.elastic.co/cloud) and want to use the ID rather than endpoint URL. |
| `YENTE_OPENSEARCH_REGION` | - | Specifies your region if [you are using AWS hosted OpenSearch](opensearch.md). |
| `YENTE_OPENSEARCH_SERVICE` | - | Should be `aoss` if [you are using Amazon OpenSearch](opensearch.md) Serverless Service and `es` if you are using the default Amazon OpenSearch Service. |

## Managing data updates

By default, `yente` will check for an updated build of the OpenSanctions database published at `data.opensanctions.org` every hour. If a fresh version is found, an indexing process will be spawned and load the data into the ElasticSearch index.

You can change this behavior using the following configuration options:

| Env. variable | Default | Description |
| ------ | ------ | ------ |
| `YENTE_MANIFEST`   | `manifests/default.yml`   | Specifies the path of the manifest that defines the datasets exposed by the service. This is used to [add extra datasets to the service](datasets.md) or to define custom scopes for entity screening.   |
| `YENTE_CRONTAB`   | `0 * * * *`   | Gives the frequency at which new data will be indexed as a [crontab](https://crontab.guru/).   |
| `YENTE_AUTO_REINDEX`   | `true`   | Can be set to ``false`` to disable automatic data updates. Data will only be refreshed and re-indexed when running ``yente reindex``.   |

When running more than one Yente container, `YENTE_AUTO_REINDEX` must be set to `false` to avoid multiple workers clashing. In this scenario, `yente reindex` should be run using an external cron mechanism. For more information, see the documentation on [how to deploy yente](deploy/).

## Other settings

| Env. variable | Default | Description |
| ------ | ------ | ------ |
| `YENTE_UPDATE_TOKEN`   | `unsafe-default`   | Should be set to a secret string. The token is used with a `POST` request to the `/updatez` endpoint to force an immediate re-indexing of the data. |
| `YENTE_HTTP_PROXY`| - | Set a proxy for Yentes outgoing HTTP requests. |
| `YENTE_MAX_BATCH` | `100` | How many entities to accept in a `/match` batch at most. |
| `YENTE_MATCH_PAGE` | `5` | How many results to return per `/match` query by default. |
| `YENTE_MAX_MATCHES` | `500` | How many results to return per `/match` query at most. |
| `YENTE_MATCH_CANDIDATES` | `10` | How many candidates to retrieve from the search as a multiplier of the `/match` limit. Note that increasing this parameter will also increase query cost, as each of these candidates scored after retrieval from the index.|
| `YENTE_MATCH_FUZZY` | `true` | Whether to run expensive Levenshtein queries inside ElasticSearch. |
| `YENTE_QUERY_CONCURRENCY` | `10` | How many match and search queries to run against ES in parallel. |
| `YENTE_DELTA_UPDATES` | `true` | When set to `false` Yente will download the entire dataset when refreshing the index. |
| `YENTE_STREAM_LOAD`   | `true`   | If set to `false`, will download the full data before indexing it. This can improve the stability of the indexer, especially when the network connection is a bit sketchy, but requires some local disk cache space.   |
