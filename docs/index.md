# yente - the [matchmaker](https://www.youtube.com/watch?v=jVGNdB6iEeA)

yente is an open source screening API service that powers the [hosted OpenSanctions API]({{ config.extra.opensanctions_url }}//api/). It provides functions search, retrieve or match FollowTheMoney entities, including people, companies or vessels that are subject to international sanctions.

Out of the box, it provides access to [OpenSanctions data]({{ config.extra.opensanctions_url}}/datasets/), but it can also be used to [search and match other data](datasets.md), such as [company registries]({{ config.extra.opensanctions_url }}/kyb/) or [custom watchlists](datasets.md). It can also be run [on-premises as a KYC appliance]({{ config.extra.opensanctions_url }}/docs/on-premise/) so that no customer data leaves the deployment context.

## Getting started

**Note:** this documentation is only relevant to users who plan to [self-host]({{ config.extra.opensanctions_url }}/docs/on-premise/) the API. [Click here]({{ config.extra.opensanctions_url }}/api/) if you'd prefer to use our hosted API service.

* [Set up yente on your local maching and in your infrastructure](deploy/index.md)
* [Configure OpenSanctions data](opensanctions-delivery.md)
* [Learn about advanced configuration options](settings.md)

## Using the API

To start using the API, see our documentation on [how to get started]({{ config.extra.opensanctions_url}}/docs/api/) and [how to build a simple client for the matching API]({{ config.extra.opensanctions_url }}). While the documentation focuses on the [hosted OpenSanctions API]({{ config.extra.opensanctions_url }}//api/), it also applies to self-hosted yente instances.

Full [reference documentation for the API endpoints](https://api.opensanctions.org/) is available at the root of your self-hosted yente instance.

Also see [FAQ on API functionality]({{ config.extra.opensanctions_url }}/faq/?section=API).

## Get involved

Yente is developed as an open source project [on GitHub](https://github.com/opensanctions/yente). We welcome [issue reports](https://github.com/opensanctions/yente/issues) and contributions!
