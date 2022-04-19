# yente

`yente` is the OpenSanctions match-making API. The service exposes endpoints to search, retrieve or match [FollowTheMoney entities](https://www.opensanctions.org/docs/entities/). 

The API is built to provide access to OpenSanctions data, it can also be used to search and match other data, such as the [ICIJ OffshoreLeaks](https://github.com/opensanctions/icij-offshoreleaks/blob/master/README.md).

While `yente` powers the [OpenSanctions API](https://api.opensanctions.org), it can also be run on-premises in KYC contexts so that no customer data leaves the deployment context. The software is distributed as a Docker image with a `docker-compose.yml` that also provisions the requisite ElasticSearch index.

* [Documentation](https://www.opensanctions.org/docs/api/)
  * [OpenAPI/ReDoc specification](https://api.opensanctions.org)
  * [Matching system](https://www.opensanctions.org/matcher/)
* Commercial data licensing: https://www.opensanctions.org/licensing/
* [Why is it called yente?](https://www.youtube.com/watch?v=jVGNdB6iEeA)

## Usage

*Please [contact the OpenSanctions team](https://www.opensanctions.org/contact/) if you prefer to use this API as a hosted service (SaaS).*

In order to use `yente` on your own servers, we recommend you use `docker-compose` (or another Docker orchestration tool) to pull and run the pre-built containers. For example, you can download the `docker-compose.yml` in this repository and use it to boot an instance of the system:

```bash
mkdir -p yente && cd yente
wget https://raw.githubusercontent.com/opensanctions/yente/main/docker-compose.yml
docker-compose up
```

This will make the service available on Port 8000 of the local machine.

If you run the container in a cluster management system like Kubernetes, you will need to run both of the containers defined in the compose file (the API and ElasticSearch instance). You will also need to assign the API container network policy permissions to fetch data from `data.opensanctions.org` once every hour so that it can update itself.

### Managing data updates

By default, `yente` will query `data.opensanctions.org` every 30 minutes to check for an updated build of the database. If an updated version is found, an indexing process will be spawned and load the data into the ElasticSearch index.

You can change this behaviour in two ways:

* Edit the [crontab](https://crontab.guru/) specification for `schedule` in your `manifest.yml` (see below) in order to run the auto-update process at a different interval. Setting the `schedule` to `null` will disable automatic data updates entirely.

* If you wish to manually run an indexing process, you can do so by calling the script `yente/reindex.py` in the application. For example, in a docker-compose based environment, the full command would be: `docker-compose run app python3 yente/reindex.py`.

OpenSanctions uses these two options in conjunction in order to move reindexing to a separate Kubernetes CronJob that allows for stricter resource management.

## Settings

The API server has a few operations-related settings, which are passed as environment variables. The settings include:

- ``YENTE_ENDPOINT_URL`` the URL which should be used to generate external links back to
  the API server, e.g. ``https://yente.mycompany.com``.
- ``YENTE_MANIFEST`` specify the path of the `manifest.yml` that defines the datasets exposed by the service.
- ``YENTE_UPDATE_TOKEN`` should be set to a secret string. The token is used with a `POST` request to the `/updatez` endpoint to force an immediate re-indexing of the data.
- ``YENTE_ELASTICSEARCH_URL``: Elasticsearch URL, defaults to `http://localhost:9200`.
- ``YENTE_ELASTICSEARCH_INDEX``: Elasticsearch index, defaults to `yente`.
- ``YENTE_ELASTICSEARCH_CLOUD_ID``: If you are using Elastic Cloud and want to use the ID rather than endpoint URL.
- ``YENTE_ELASTICSEARCH_USERNAME``: Elasticsearch username. **Required** if connection using ``YENTE_ES_CLOUD_ID``.
- ``YENTE_ELASTICSEARCH_PASSWORD``: Elasticsearch password. **Required** if connection using ``YENTE_ES_CLOUD_ID``.

### Adding custom datasets (`manifest.yml`)

With factory settings, `yente` will index and expose all datasets published by OpenSanctions every time they change (by polling an index file every 30 minutes). You can change this behaviour in several ways:

* Index additional datasets that should be checked by the matching API. This might include large public datasets, or in-house data (such as a customer blocklist, local PEPs list, etc.).
* Change the update interval (`schedule`) to check less frequently, e.g. only once a month.
* Index only a part of the OpenSanctions data, e.g. only the `sanctions` collection.

Such configuration is handled by a custom YAML file you can supply for `yente`. (The file needs to be accessible to the application, which may require the use of a Docker volume mount or a Kubernetes ConfigMap). An example manifest file might look like this:

```yaml
# Schedule is a crontab specification. Set it to `null` to disable automatic updates
# entirely:
schedule: "*/30 * * * *"
# Import external dataset specifications from OpenSanctions. This will fetch the dataset
# metadata from the given index and make them available to yente.
external:
  # nb. replace `latest` with a date stamp (e.g. 20220419) to fetch OpenSanctions data
  # for a particular day:
  url: "https://data.opensanctions.org/datasets/latest/index.json"
  # Limit the dataset scope of the entities which will be indexed into yente. Useful
  # values include `default`, `sanctions` or `peps`.
  scope: all
# The next section begins to specify non-OpenSanctions datasets that should be exposed
# in the API:
datasets:
  # Example A: fetch a public dataset from a URL and include it in the default search
  # scope defined upstream in OpenSanctions:
  - name: offshoreleaks
    title: ICIJ OffshoreLeaks
    url: https://data.opensanctions.org/contrib/icij-offshoreleaks/full-oldb.json
    # children:
    #   - all
    #   - offshore
  # Example B: a local dataset from a path that is visible within the container used
  # to run the service:
  - name: blocklist
    title: Customer Blocklist
    path: /data/customer-blocklist.json
    # Incrementing the version will force a re-indexing of the data. It must be
    # given as a monotonic increasing number.
    version: "20220419001"
  # Example C: a combined collection that allows querying all entities in its member
  # datasets at the same time:
  - name: full
    title: Full index
    datasets:
      - offshoreleaks
      - blocklist
      # include OpenSanctions collections:
      - sanctions
```

In order for `yente` to import a custom dataset, it must be formatted as a line-based feed of [FollowTheMoney](https://docs.alephdata.org/developers/followthemoney) entities. There are various ways to produce FtM data, but the most convenient is [importing structured data via a mapping specification](https://docs.alephdata.org/developers/mappings). This allows reading data from a CSV file or SQL database and converting each row into entities.

### Using the Statement API

The primary goal of the API is to serve entity-based data, but it also supports an endpoint to browse OpenSanctions data in its [statement-based form](https://www.opensanctions.org/docs/statements/). This exists in order to provide a backend for the [raw data explorer](https://www.opensanctions.org/statements/) on the OpenSanctions.org site.

**NOTE:** If you wish to fetch statements data in bulk, download the [CSV export](https://www.opensanctions.org/docs/statements/) instead of using this endpoint.

Because indexing and exposing the statement data makes no sense for on-premises deployments, it is disabled by default. You can use the environment variable ``YENTE_STATEMENT_API`` in order to enable the `/statements` endpoint.

Statement data support is experimental and may be moved to a separate API server in the future.

## Development

`yente` is implemented in asynchronous, typed Python using the FastAPI framework. We're happy to see any bug fixes, improvements or extensions from the community. For local development without Docker, install the package into a fresh virtual Python environment like this:

```bash
git clone https://github.com/opensanctions/yente.git
cd yente
pip install -e .
```

This will install a broad range of dependencies, including `numpy`, `scikit-learn` and `pyicu`, which are binary packages that may require a local build environment. For `pyicu` in particular, refer to the [package documentation](https://pypi.org/project/PyICU/).

### Running the server

Once you've set the ``YENTE_ELASTICSEARCH_URL`` environment variable to point to a running instance of ElasticSearch, you can run the web server like this:

```bash
python yente/server.py
```

### License and Support

``yente`` is licensed according to the MIT license terms documented in ``LICENSE``. Using the service in a commercial context may require a [data license for OpenSanctions data](https://www.opensanctions.org/licensing/).