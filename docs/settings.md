---
hide:
  - toc
---

# Configuring yente

The Yente service is built to require a minimum of configuration, but several environment variables can be used to define the search provider to use, and to define a custom data manifest.

The API server has a few operations-related settings, which are passed as environment variables. The settings include:

| Env. variable | Default | Description |
| ------ | ------ | ------ |
| `YENTE_MANIFEST`   | `manifests/default.yml`   | Specifies the path of the manifest that defines the datasets exposed by the service. This is used to [add extra datasets to the service](/docs/yente/datasets/) or to define custom scopes for entity screening.   |
| `YENTE_CRONTAB`   | `0 * * * *`   | Gives the frequency at which new data will be indexed as a [crontab](https://crontab.guru/).   |
| `YENTE_AUTO_REINDEX`   | `true`   | Can be set to ``false`` to disable automatic data updates and force data to be re-indexed only via the command line (``yente reindex``).   |
| `YENTE_UPDATE_TOKEN`   | `unsafe-default`   | Should be set to a secret string. The token is used with a `POST` request to the `/updatez` endpoint to force an immediate re-indexing of the data. |
| `YENTE_INDEX_TYPE` | `elasticsearch` | Should be one of `elasticsearch` or [`opensearch`](/faq/83/opensearch/), depending on what provider you use. |
| `YENTE_INDEX_URL`   | `http://index:9200`   | The URL of your search index provider backend. |
| `YENTE_INDEX_NAME`   | `yente`   | The prefix name that will be used for the search index. |
| `YENTE_ELASTICSEARCH_CLOUD_ID`   | - | If you are using [Elastic Cloud](https://www.elastic.co/cloud) and want to use the ID rather than endpoint URL. |
| `YENTE_OPENSEARCH_REGION` | - | Specifies your region if [you are using AWS hosted OpenSearch](/faq/83/opensearch/). |
| `YENTE_OPENSEARCH_SERVICE` | - | Should be `aoss` if [you are using Amazon OpenSearch](/faq/83/opensearch/) Serverless Service and `es` if you are using the default Amazon OpenSearch Service. |
| `YENTE_INDEX_USERNAME` | - | Username for the search provider. **Required** if connection using Elastic Cloud. |
| `YENTE_INDEX_PASSWORD` | - | Elasticsearch password. **Required** if connection using Elastic Cloud. |
| `YENTE_HTTP_PROXY`| - | Set a proxy for Yentes outgoing HTTP requests. |
| `YENTE_MAX_BATCH` | `100` | How many entities to accept in a /match batch at most. |
| `YENTE_MATCH_PAGE` | `5` | How many results to return per /match query by default. |
| `YENTE_MAX_MATCHES` | `500` | How many results to return per /match query at most. |
| `YENTE_MATCH_CANDIDATES` | `10` | How many candidates to retrieve as a multiplier of the /match limit. |
| `YENTE_MATCH_FUZZY` | `true` | Whether to run expensive Levenshtein queries inside ElasticSearch. |
| `YENTE_QUERY_CONCURRENCY` | `10` | How many match and search queries to run against ES in parallel. |
| `YENTE_DELTA_UPDATES` | `true` | When set to `false` Yente will download the entire dataset when refreshing the index. |
| `YENTE_STREAM_LOAD`   | `true`   | If set to `false`, will download the full data before indexing it. This improves the stability of the indexer but requires some local disk cache space.   |


## Managing data updates

By default, `yente` will check for an updated build of the OpenSanctions database published at `data.opensanctions.org` every hour. If a fresh version is found, an indexing process will be spawned and load the data into the ElasticSearch index.

You can change this behavior in two ways:

* Specify a [crontab](https://crontab.guru/) for `YENTE_CRONTAB` in your environment in order to run the auto-update process at a different interval. Setting the environment variable `YENTE_AUTO_REINDEX` to `false` will disable automatic data updates entirely.
* If you wish to manually run an indexing process, you can do so by calling the script `yente reindex`. This command must be invoked inside the application container. For example, in a docker-compose based environment, the full command would be: `docker-compose run yente reindex`.

The production settings for api.opensanctions.org use these two options in conjunction to move reindexing to a separate Kubernetes CronJob that allows for stricter resource management.
