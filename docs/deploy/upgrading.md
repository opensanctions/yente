We release [new versions of yente](https://github.com/opensanctions/yente/releases) on a regular basis in order to add new features, expand our data model and improve the scoring system. We recommend that all users of the on-premise version of the software schedule software upgrades at least **twice a year** in order to ensure proper functioning of the tool.

Some versions of yente introduce changes to the index format that require rebuilding the full index. This will happen automatically on the next re-index. We try to make new versions of yente compatible with at least the previous minor version's index format. This means that upgrading will usually incur little downtime (or even none, if deploying multiple instances behind a load balancer), as the new version will keep serving requests from the previous version's index until the new index is available. Please note that this is on a best-effort basis - if you depend on a zero-downtime upgrade, please perform a dry-run test of the upgrade for your specific upgrade path.

## How to upgrade

### Read the changelog

To find out what change in yente, read [the changelog published on GitHub](https://github.com/opensanctions/yente/releases). As we evolve the API, we will add new fields and endpoints, as well as deprecate old ones.

Also read [the OpenSanctions technical changelog]({{ config.extra.opensanctions_url }}/changelog/) to find out about changes to the data model. As we evolve our data model, we regularly introduce new fields or entity types in the data model to capture additional dimensions of the entities we cover.

## Adapt your client applications

Ensure that your clients are compatible with the version of yente you're upgrading to and all the changes outlined in the changelog. Changes in both the data and yente functionality are carry a notice period according to [our change policy]({{ extra.config.opensanctions_url }}/docs/data/changes/).

## Upgrade yente

To upgrade, simply update the version of the docker image used in your deployment. The version of yente is determined by the active tag of the Docker container in operation: `ghcr.io/opensanctions/yente:<version>`. Simply changing that version tag will trigger an update. If you're running in Docker compose (the default), open `docker-compose.yml` and edit the line `services.app.image`, then run `docker compose pull` to fetch the latest container. `docker compose restart app` will run the new container version.


## Ensure that the scoring system you use is still ideal for your purposes.

yente's `/match` API endpoint supports multiple [different algorithms]({{ config.extra.opensanctions_url }}/matcher/) used to score results. As new algorithms are released, keep an eye on those developments to make sure you're still using the best solution available in the tool. yente will not switch algorithms automatically and try to keep existing implementations stable, so making this change is up to you.
