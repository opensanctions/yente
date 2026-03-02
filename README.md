# yente

`yente` is an open source data match-making API. The service provides several HTTP endpoints to search, retrieve or match [FollowTheMoney entities](https://www.opensanctions.org/docs/entities/), including people, companies or vessels that are subject to international sanctions.

The yente API is built to provide access to [OpenSanctions data](https://www.opensanctions.org/datasets/), and can also be used to search and match other data, such as [company registries](https://www.opensanctions.org/datasets/kyb/) or [custom watchlists](https://www.opensanctions.org/docs/yente/datasets/).

While `yente` is the open source core code base for the [OpenSanctions API](https://www.opensanctions.org/api/), it can also be run [on-premises as a KYC appliance](https://www.opensanctions.org/docs/self-hosted/) so that no customer data leaves your infrastructure.

* [yente documentation](https://www.opensanctions.org/docs/yente/) - install, configure and use the service.

## Development

`yente` is implemented in asynchronous, typed Python using the FastAPI framework. We're happy to see any bug fixes, improvements or extensions from the community. To set up a local development environment, use `uv`:

```bash
git clone https://github.com/opensanctions/yente.git
cd yente
# Install runtime and development dependencies
uv sync
# Install pre-commit hooks with useful checks
prek install
# Activate the virtual environment
source .venv/bin/activate
```

This will install a broad range of dependencies, including `numpy`, `scikit-learn` and `pyicu`, which are binary packages that may require a local build environment. For `pyicu` in particular, refer to the [package documentation](https://pypi.org/project/PyICU/).

### Running the server

Once you've set the ``YENTE_INDEX_URL`` environment variable to point to a running instance of ElasticSearch or OpenSearch, you can run the web server like this:

```bash
yente serve
```


### Releasing

    bump2version --verbose minor # or patch
    git push && git push --tags

## License and Support

``yente`` is licensed according to the MIT license terms documented in ``LICENSE``. Using the service in a commercial context may require a [data license for OpenSanctions data](https://www.opensanctions.org/licensing/).
