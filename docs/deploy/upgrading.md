We release [new versions of yente](https://github.com/opensanctions/yente/releases) on a regular basis in order to add new features, expand our data model, and improve the scoring system. We recommend that all users of the on-premise version of the software schedule software upgrades at least **twice a year** in order to ensure proper functioning of the tool.

## How to upgrade

### Read the changelog

To find out what changed in yente, read [the changelog published on GitHub](https://github.com/opensanctions/yente/releases). As we evolve the API, we will add new fields and endpoints, as well as deprecate old ones.

Also read [the OpenSanctions technical changelog]({{ config.extra.opensanctions_url }}/changelog/) to find out about changes to the data model. As we evolve our data model, we regularly introduce new fields or entity types in the data model to capture additional dimensions of the entities we cover.

### Adapt your client applications

Ensure that your clients are compatible with the version of yente you're upgrading to and all the changes outlined in the changelog. Changes in both the data and yente functionality carry a notice period according to [our change policy]({{ config.extra.opensanctions_url }}/docs/data/changes/).

### Upgrade yente

The following instructions document the commands when running with Docker Compose. Adapt them to your specific deployment scenario.

1. Stop the yente container (`docker compose stop app`). During the upgrade, the index will be rebuilt in the format of the new version, which may lead to unexpected results or crashes.
2. Update the version of the docker image used in your deployment. The version of yente is determined by the active tag of the Docker container in operation: `ghcr.io/opensanctions/yente:<version>`. Simply changing that version tag will trigger an update. If you're running in Docker compose (the default), open `docker-compose.yml` and edit the line `services.app.image`, then run `docker compose pull` to fetch the latest container.
3. Rebuild the index using `yente reindex -f` (`docker compose run app yente reindex -f`)
4. Restart the yente container: `docker compose restart app`

## Advanced: Blue-green deployment strategy

If you'd like to upgrade yente with little to no downtime, we recommend running a [blue-green deployment strategy](https://en.wikipedia.org/wiki/Blue%E2%80%93green_deployment). The basic idea is to run two versions of yente with a different `YENTE_INDEX_NAME` and switch between them. What this looks like in your environment is up to you - it could be a manual process or a full-blown CD pipeline.

1. Keep the old yente running, for this example we'll assume it has `YENTE_INDEX_NAME=yente-green`.
1. Run `yente reindex -f` with the version of yente you're upgrading to with `YENTE_INDEX_NAME=yente-blue`.
1. Restart your serving yente with the new version and `YENTE_INDEX_NAME=yente-blue`. In a more advanced deployment scenario, one might switch over the load balancer to the new instance.
1. Ensure that your periodic reindexing jobs are also using the new version of yente and `YENTE_INDEX_NAME=yente-blue`

If something isn't working right after the upgrade, you may roll back to the previous version of yente by starting the old version of yente and `YENTE_INDEX_NAME=yente-green`.

At this point, your yente is running entirely from the `yente-blue` indices. Your Elastic will still have a bunch of "old" `yente-green` indices around - you may delete them. Alternatively, the next time you upgrade, you can do the same procedure but switching from `yente-blue` to `yente-green`. Your old `yente-green` indices will be cleaned up during the first reindex.

## Upgrading from Elasticsearch 8 to 9

Elasticsearch is the search index software underlying yente. Version 9 of Elasticsearch was released in April 2025, and version 8 will be end-of-life in January 2027. Versions of yente released in 2025 or later are already compatible with both version 8 and 9 of the Elastic server, but versions of yente released after May 2026 will only be compatible with Elastic server 9. See the [official documentation for more information](https://www.elastic.co/docs/reference/elasticsearch/clients/python#_compatibility).

When **upgrading from Elasticsearch 8 to 9** in the single-node setup that is used by default in the docker-compose based deployment outlined in [our documentation on deploying yente](index.md), we have observed no issues using the following path:

1. Upgrade Elasticsearch to the latest version in the 8.x series in `docker-compose.yml` (at the time of writing, that's `8.19.13`, check [Docker Hub](https://hub.docker.com/_/elasticsearch) for the latest)
2. Restart Elasticsearch and verify everything works as expected (`docker compose up -d index`)
3. Upgrade Elasticsearch to the latest version in the 9.x series in `docker-compose.yml`
4. Restart Elasticsearch and verify everything works as expected.

For more advanced deployments of Elasticsearch, refer to the [official documentation](https://www.elastic.co/docs/deploy-manage/upgrade/deployment-or-cluster/elasticsearch).

We provide no guarantees or support around this upgrade procedure. We recommend you plan for enough downtime to allow for a full reindex on a fresh Elastic cluster, in case something does go wrong. If you require upgrades with very low or zero downtime, we recommend you set up a second instance of yente and Elastic and only switch over once you have verified that the new instance is working as expected.
