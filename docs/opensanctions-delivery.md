# Configuring OpenSanctions Data

By default, yente is configured to access the OpenSanctions database. We ask prospective commercial customers to create a Data Delivery Token to access the data. Please register at our [customer portal]({{ config.extra.opensanctions_url }}/account/) and find your Delivery Token in the Credentials Manager. Set `OPENSANCTIONS_DELIVERY_TOKEN` in your environment and off you go!

If you're [deploying yente using a Docker container](/deploy/), it would look something like this:

```yaml
services:
  [...]
  app:
    [...]
    environment:
      OPENSANCTIONS_DELIVERY_TOKEN: "65ee4bdac5b3421fb41324198cb951b3"
```

Using OpenSanctions data for non-commerical and evaluation purposes is free. Before using OpenSanctions data commercially, we ask you to [purchase a license]({{ config.extra.opensanctions_url }}/license/).

## ...for non-commercial users

Non-commercial users and [users exempted from commercial licensing]({{ config.extra.opensanctions_url }}/faq/32/exemptions/) may set `YENTE_MANIFEST: "manifests/non-commercial.yml"` to access OpenSanctions directly.
