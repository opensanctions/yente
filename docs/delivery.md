# Configuring OpenSanctions Data

By default, `yente` is configured to fetch the full and up-to-date OpenSanctions database. To help us understand how commercial partners use the dataset, we ask customers to create a delivery token. The token itself is not linked to a contract, it just helps us to provide relevant support. Please sign up at the [customer portal]({{ config.extra.opensanctions_url }}/account/) - a delivery token will be created automatically and immediately, which you can find in the [Data delivery service]({{ config.extra.opensanctions_url }}/account/bulk/) section. Set `OPENSANCTIONS_DELIVERY_TOKEN` in your environment and off you go!

If you're [deploying yente using a Docker container](/deploy/), your `docker-compose.yml` could contain the token like this:

```yaml
services:
  [...]
  app:
    [...]
    environment:
      OPENSANCTIONS_DELIVERY_TOKEN: "65ee4bdac5b3421fb41324198cb951b3"
```

Once you have decided to adopt the OpenSanctions database in a commercial setting, you need to [purchase a license subscription]({{ config.extra.opensanctions_url }}/licensing/).

## ...for non-commercial users

Non-commercial users and [users exempted from commercial licensing]({{ config.extra.opensanctions_url }}/faq/32/exemptions/) may set `YENTE_MANIFEST: "/app/manifests/civic.yml"` to access OpenSanctions directly. This setting can be applied inside the `docker-compose.yml` file when `docker compose` is used to run `yente`.
