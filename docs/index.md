# yente - the matchmaker

**yente:** **Intro** · [Deployment](/docs/yente/deploy/) · [Settings](/docs/yente/settings/) · [Custom datasets](/docs/yente/datasets/) · [FAQ](/faq/?section=yente&section=API)

The yente API is built to provide access to [OpenSanctions data](/datasets/), it can also be used to [search and match other data](/docs/yente/datasets/), such as [company registries](/kyb/) or [custom watchlists](/docs/yente/datasets/).

While `yente` is the open source core code base for the [OpenSanctions API](https://api.opensanctions.org), it can also be run [on-premises as a KYC appliance](/docs/self-hosted/) so that no customer data leaves the deployment context. The software is distributed as a Docker image with a pre-defined `docker-compose.yml` configuration that also provisions the requisite ElasticSearch index.

## Using the software

**Note:** this documentation is only relevant to users who plan to [self-host](/docs/on-premise/) the API. [Click here](/api/) if you'd prefer to use our hosted API service.

* [Deploy yente in your infrastructure](/docs/yente/deploy/)
* [Settings and configuration](/docs/yente/settings/)
* [Adding custom datasets](/docs/yente/datasets/)
* Frequently asked questions: [API functionality](/faq/?section=API), [yente Software](/faq/?section=yente)
* [GitHub repository](https://github.com/opensanctions/yente)
* [Report an issue](https://github.com/opensanctions/yente/issues/new)

## Using the API

* Tutorial: [Building a screening client](/docs/api/matching/)
* [API endpoints](https://api.opensanctions.org)
    * [openapi.json](https://api.opensanctions.org/openapi.json)