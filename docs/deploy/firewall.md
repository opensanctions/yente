# Running yente behind a restrictive firewall

Many (especially regulated) deployments of `yente` are behind a firewall. In order for this to work, both the software itself and the data it uses for screening need to be available inside the secure environment.

It's hard to give generalised advice on this that applies to all secured environments, but some general hints include:

* The yente software is distributed as a Docker container (`ghcr.io/opensanctions/yente`), which can be mirrored in a local Docker registry before being deployed.
* In order to update itself, yente needs to fetch fresh data releases from OpenSanctions. In order to do so, it will attempt to make HTTPS connections to `delivery.opensanctions.com` and `data.opensanctions.org`. Permitting the deployed yente container to access this host is the easiest firewall configuration option.
* Some of our customers have also chosen to use an internal CI/CD process to build custom Docker containers on top of the official yente releases. These can include copies the data files used by the software to index itself, and a modified manifest file that details the location of the static data.

### Using local data without an internet connection

By default, `yente` will regularly fetch metadata and data updates from the domain names listed above. If this is not an option, you can separate the data download from the operational environment used by `yente`. This requires some environment-specific design, but in general has two components:

1. You need to fetch the metadata and data files used by `yente` and place them in a location that is accessible to the Python application during its runtime. This could, for example, be a [docker volume mount](https://docs.docker.com/storage/volumes/) or by building a docker image layered on top of the official images which contains the data.
2. You'll need to define a custom manifest configuration file to make `yente` consume these local files instead of trying to access the internet.

The basic commands for fetching the metadata and data files are as follows:

```bash
wget -O index.json https://data.opensanctions.org/datasets/latest/index.json
wget -O entities.ftm.json https://data.opensanctions.org/datasets/latest/default/entities.ftm.json
```

Note that this needs to be repeated in a regular interval, e.g. using a crontab on a bridge/jump host.

You can then use a custom manifest file which points to the location where the docker container can access the files that have been fetched:

```yaml
catalogs:
  # The catalog file is loaded in order to get all the metadata for the datasets
  # included in the database.
  - url: "file:///path/to/dmz/index.json"
datasets:
  - name: offline
    title: Wrapper dataset for offline data
    children:
      - default
    entities_url: "file:///path/to/dmz/entities.ftm.json"
    load: true
```
