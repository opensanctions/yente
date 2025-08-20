# Using OpenSearch and ElasticSearch with yente

Yente supports both ElasticSearch and [OpenSearch](https://opensearch.org/) (a fork of ElasticSearch maintained by Amazon) as backends. This makes it easy to run the appliance on the AWS cloud. In order to switch the backend type from the default (`elasticsearch`), set the following environment variables in the deployment configuration of the `yente` container:

```
YENTE_INDEX_TYPE=opensearch
YENTE_INDEX_URL=https://[...].es.amazonaws.com/
```

If you have enabled username/password authentication for your OpenSearch index, you can supply these credentials using environment variables:

```
YENTE_INDEX_USERNAME=username
YENTE_INDEX_PASSWORD=password
```

You can also use the AWS credentials in the local environment (normally configured via `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) to access a hosted instance of OpenSearch. To use these credentials, set the following environment variables:

```
# Classic AWS OpenSearch:
YENTE_OPENSEARCH_SERVICE=es
# Or, for serverless:
YENTE_OPENSEARCH_SERVICE=aoss
YENTE_OPENSEARCH_REGION=eu-central-1
```

## Compatibility

While ElasticSearch and OpenSearch are based on the same initial code base, we expect the two products to deviate more and more as time progresses. Since our [hosted infrastructure]({{ config.extra.opensanctions_url }}/api/) is running on Elastic Cloud, we cannot make the following guarantees into the future:

* Search results returned by the `/search` API may not be identical, or in the same ranking order.
* The extended query syntax used by the `/search` API may deviate from the one exposed by ElasticSearch.
* The performance/query throughput of ElasticSearch and OpenSearch may deviate in the future.
