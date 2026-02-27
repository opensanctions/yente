# Managing data updates

By default, `yente` will check for an updated build of the OpenSanctions database published at `delivery.opensanctions.com` every hour. New data releases are [published several times a day]({{ config.extra.opensanctions_url }}/faq/4/update-frequency/). If a fresh version is found, an indexing process will be spawned and the data will be loaded into the ElasticSearch index.

You can change this behavior using the following configuration options:

| Env. variable | Default | Description |
| ------ | ------ | ------ |
| `YENTE_MANIFEST`   | `/app/manifests/default.yml`   | Specifies the path of the manifest that defines the datasets exposed by the service. This is used to [add extra datasets to the service](datasets.md) or to define custom scopes for entity screening.   |
| `YENTE_CRONTAB`   | `0 * * * *`   | Gives the frequency at which new data will be indexed as a [crontab](https://crontab.guru/).   |
| `YENTE_AUTO_REINDEX`   | `true`   | Can be set to ``false`` to disable automatic data updates. Data will only be refreshed and re-indexed when running ``yente reindex``.   |

## Re-indexing in deployments with multiple yente instances.

In the single-instance default configuration, yente will set up an automatic refresh of its dataset every hour (governed by `YENTE_CRONTAB`).

However, when running more than one yente container, for example deploying to Kubernetes or a managed multi-instance environment (such as Google Cloud Run), you should set `YENTE_AUTO_REINDEX` to `false` — otherwise the workers will clash, as they will all attempt to re-index in parallel. In this scenario, `yente reindex` should be run using an external cron mechanism. For more information, see the documentation on [how to deploy yente](deploy/index.md).

## How does yente update its index?

Here is a very quick tour of how `yente` works:

* When the application starts, it will download a metadata file from `delivery.opensanctions.com` which states the latest version of the OpenSanctions data that was been released.
* If there is fresh data, it will create an ElasticSearch index with a timestamp that matches the latest release of the data (e.g. `yente-entities-default-00220221030xxxx`).
* It will then fetch the latest data from `data.opensanctions.org` (a 2GB+ JSON file) and push it into ElasticSearch in small batches.
* When all the data is indexed, `yente` will create an ES index alias from `yente-entities-default` to the latest snapshot of the index (e.g. `yente-entities-all-00220221030xxxx`) and delete all older snapshots of the index.
* Only once this has completed will the `/search` and `/match` APIs work correctly. On the plus side, any future updates to the data will be indexed first, and the switch-over to the new data will be instantaneous.
