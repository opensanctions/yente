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

### Adding custom datasets



### Using the Statement API

The primary goal of the API is to serve entity-based data, but it also supports an endpoint to browse OpenSanctions data in its [statement-based form](https://www.opensanctions.org/docs/statements/). This exists in order to provide a backend for the [raw data explorer](https://www.opensanctions.org/statements/) on the OpenSanctions.org site.

**NOTE:** If you wish to fetch statements data in bulk, download the [CSV export](https://www.opensanctions.org/docs/statements/) instead of using this endpoint.

Because indexing and exposing the statement data makes no sense for on-premises deployments, it is disabled by default. You can use the environment variable ``YENTE_STATEMENT_API`` in order to enable the `/statements` endpoint.

Statement data support is experimental and may be moved to a separate API server in the future.

## Development

`yente` is implemented in asynchronous, typed Python using the FastAPI framework. 

If you are fine working on the package while it is running Docker, use the Docker shell:

```bash
make shell
```

For development without Docker, install the Python package like this:

```bash
pip install -e .
```

### Running the server

Once you've set the ``YENTE_ELASTICSEARCH_URL`` environment variable to point to a running instance of ElasticSearch, you can run the web server like this:

```bash
python yente/server.py
```

### License and Support

``yente`` is licensed according to the MIT license terms documented in ``LICENSE``. Using the service in a commercial context may require a [data license for OpenSanctions data](https://www.opensanctions.org/licensing/).