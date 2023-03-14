# yente

`yente` is an open source data match-making API. The service provides several HTTP endpoints to search, retrieve or match [FollowTheMoney entities](https://www.opensanctions.org/docs/entities/), including people, companies or vessels that are subject to international sanctions. 

The yente API is built to provide access to [OpenSanctions data](https://www.opensanctions.org/datasets/), it can also be used to [search and match other data](https://www.opensanctions.org/docs/yente/datasets/), such as [company registries](https://www.opensanctions.org/kyb/) or [custom watchlists](https://www.opensanctions.org/docs/yente/datasets/).

While `yente` is the open source core code base for the [OpenSanctions API](https://api.opensanctions.org), it can also be run [on-premises as a KYC appliance](https://www.opensanctions.org/docs/self-hosted/) so that no customer data leaves the deployment context.

* [yente documentation](https://www.opensanctions.org/docs/yente) - install, configure and use the service.

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