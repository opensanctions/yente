## All my HTTP requests return index_not_found_exception, what's wrong?

This probably means that the [initial index-building](reindex.md) never completed. Check the following:

1. That the machine you are running the indexer/yente app on is able to fetch data via HTTPS from `data.opensanctions.org`.
2. That a temporary, timestamped index (see above) was created in ElasticSearch (which means indexing has at least begun).
3. That the final `yente-entities-all` was created. If a timestamped index was created, but the final alias does not exist, it likely means that indexing was aborted half-way. This could be because a) the downloaded data could not be fetched or stored in its entirety, b) the indexing of entities was aborted, perhaps due to a lack of system memory or compute time.

While debugging this issue, you can use `http://yente-service:8000/updatez?token=UPDATE_TOKEN&force=true` to trigger a forced re-index of the data at any time. The `UPDATE_TOKEN` is a secret token you can define in the environment of the `yente` pod using the `YENTE_UPDATE_TOKEN` variable.
