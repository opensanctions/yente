# OpenSanctions Match-making API

This directory contains code and a Docker image for running an API to match data against
OpenSanctions. It is intended to be run on-premises in KYC contexts so that no customer
data leaves the deployment context.

## Demo instance

See https://api.opensanctions.org 

## Usage

In order to use the OpenSanctions API, we recommend running an on-premises instance on your own servers or in a data center. Updated images of the API with current data are built nightly and can be pulled from Docker hub:

```bash
mkdir -p yente && cd yente
wget https://github.com/opensanctions/yente/blob/main/docker-compose.yml
docker-compose up
```

This will make the matching API available on Port 8000 of the local machine.

If you run the container in a cluster management system like Kubernetes, you may want to find a way to pull a fresh container every night so that a new image with updated data will be pulled from the Docker registry. You will then also need to re-run the indexer, the equivalent of the last line in the example above.

Please [contact the OpenSanctions team](https://www.opensanctions.org/contact/) if you are interested in exploring a hosted solution for running the API.

### Settings

The API server has a few settings, which are passed as environment variables. The settings include:

* ``YENTE_ENDPOINT_URL`` the URL which should be used to generate external links back to
  the API server, e.g. ``https://osapi.mycompany.com``.
* ``YENTE_CACHED`` can be set to "true" in order to load all data to memory on startup.
  This will make the API incredibly fast, but consume 3-4GB of RAM.
* ``YENTE_SCOPE_DATASET`` can be used to define the main dataset being used. This is
  usually ``default``, but can be set e.g. to ``sanctions`` to load a more specific set
  of data.

### Development

For development, install package like this:

```bash
pip install -e .
```

Finally, you can run an auto-reloading web server like this:

```bash
uvicorn yente.app:app --reload
```
