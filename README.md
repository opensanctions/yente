# OpenSanctions Match-making API

This directory contains code and a Docker image for running an API to match data against
OpenSanctions. It is intended to be run on-premises in KYC contexts so that no customer
data leaves the deployment context.

* Documentation: https://www.opensanctions.org/docs/api/
* Commercial use: https://www.opensanctions.org/licensing/
* Demo instance: https://api.opensanctions.org

## Usage

In order to use the OpenSanctions API, we recommend running an on-premises instance on your own servers or in a data center. Updated images of the API with current data are built nightly and can be pulled from Docker hub:

```bash
mkdir -p yente && cd yente
wget https://raw.githubusercontent.com/opensanctions/yente/main/docker-compose.yml
docker-compose up
```

This will make the matching API available on Port 8000 of the local machine.

If you run the container in a cluster management system like Kubernetes, you will need to run both of the containers defined in the compose file (the API and ElasticSearch instance). You will also need to assign the API container network policy permissions to fetch data from `data.opensanctions.org` once every hour so that it can update itself.

Please [contact the OpenSanctions team](https://www.opensanctions.org/contact/) if you are interested in exploring a hosted solution for running the API.

### Settings

The API server has a few settings, which are passed as environment variables. The settings include:

- ``YENTE_ENDPOINT_URL`` the URL which should be used to generate external links back to
  the API server, e.g. ``https://yente.mycompany.com``.
- ``YENTE_UPDATE_TOKEN`` should be set to a secret string. The token is used with a `POST` request to the `/updatez` endpoint to force an immediate re-indexing of the data.
- ``YENTE_STATEMENT_API`` can be set to "true" in order to enable the optional statement API. This is not required for entity matching, but can be used to view and debug data provenance in the system and the web site.
- ``YENTE_SCOPE_DATASET`` can be used to define the main dataset being used. This is
  usually ``default``, but can be set e.g. to ``sanctions`` to load a more specific set
  of data.
- ``YENTE_ELASTICSEARCH_INDEX_URL``: Elasticsearch URL, defaults to `http://localhost:9200`.
- ``YENTE_ELASTICSEARCH_INDEX``: Elasticsearch index, defaults to `yente`.
- ``YENTE_ELASTICSEARCH_INDEX_CLOUD_ID``: If you are using elastic cloud and want to use the ID rather than endpoint URL.
- ``YENTE_ELASTICSEARCH_INDEX_USERNAME``: Elasticsearch username. **Required** if connection using ``YENTE_ES_CLOUD_ID``.
- ``YENTE_ELASTICSEARCH_INDEX_PASSWORD``: Elasticsearch password. **Required** if connection using ``YENTE_ES_CLOUD_ID``.

### Development

If you are fine working on the package while it is running docker, use the docker shell:

```bash
make shell
```

For development without docker, install the Python package like this:

```bash
pip install -e .
```

Once you've set the ``YENTE_ELASTICSEARCH_URL`` environment variable to point to a running instance of ElasticSearch, you can run an auto-reloading web server like this:

```bash
uvicorn yente.app:app --reload
```

### License and Support

``yente`` is licensed according to the MIT license terms documented in ``LICENSE``. Using the service in a commercial context may require a [data license for OpenSanctions data](https://www.opensanctions.org/licensing/).