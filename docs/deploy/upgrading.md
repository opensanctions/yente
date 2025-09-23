We release [new versions of yente](https://github.com/opensanctions/yente/releases) on a regular basis in order to add new features, expand our data model and improve the scoring system. We recommend that all users of the on-premise version of the software schedule software upgrades at least **twice a year** in order to ensure proper functioning of the tool.

## How to upgrade

### Read the changelog

To find out what change in yente, read [the changelog published on GitHub](https://github.com/opensanctions/yente/releases). As we evolve the API, we will add new fields and endpoints, as well as deprecate old ones.

Also read [the OpenSanctions technical changelog]({{ config.extra.opensanctions_url }}/changelog/) to find out about changes to the data model. As we evolve our data model, we regularly introduce new fields or entity types in the data model to capture additional dimensions of the entities we cover.

## Adapt your client applications

Ensure that your clients are compatible with the version of yente you're upgrading to and all the changes outlined in the changelog. Changes in both the data and yente functionality are carry a notice period according to [our change policy]({{ config.extra.opensanctions_url }}/docs/data/changes/).

## Upgrade yente

The following instructions document the commands when running with Docker Compose. Please adapt them to your specific deployment scenario

1. Stop the yente container (`docker compose stop app`). During the upgrade, the index will at some point be switched over to the new version, which may lead to unexpected results if still serving from.
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

At this point, your yente is running entirely from the `yente-blue` indices. Your Eliastic will still have a bunhc of of "old" `yente-green` indices around - you may delete them. Alternatively, the next time you upgrade, you can do the same procedure but switching from `yente-blue` to `yente-green`. Your old `yente-green` indices will be cleaned up during the first reindex.
