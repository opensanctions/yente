# Configuring custom datasets

The yente API server is typically used to query OpenSanctions data, but it can also be used as a general purpose data matching API for other datasets about companies, people, property and so on.

The default configuration of `yente` will index and expose the datasets published by OpenSanctions every time they change. By adding a *manifest file*, you can change this behaviour in several ways:

* Index additional datasets that should be checked by the matching API. This might include large public datasets, or in-house data (such as a customer blocklist, local PEPs list, etc.) that you wish to vet alongside the OpenSanctions data.
* Index only a part of the OpenSanctions data, e.g. only the `sanctions` collection.

Side note: A **dataset** in `yente` is a logical unit that contains a set of entities. However, some datasets instead reference a list of other datasets which should be included in their scope. Datasets that contain other datasets (rather than their own source data) are called collections. For example, the dataset `us_ofac_sdn` (the US sanctions list) is included in the collections `sanctions` and `default`.

Multiple catalog or dataset entries should not include the same datasets or entities with the same IDs.
When overlapping collections or datasets are required, this could be done by running multiple yente instances with different `YENTE_INDEX_NAME` settings to distinguish their indexes.

## Generating FollowTheMoney data

In order for `yente` to import a custom dataset, it must be formatted as a line-based JSON feed of [FollowTheMoney entities]({{ config.extra.opensanctions_url }}/docs/entities/). Entities describe [semantic units like people, companies or airplanes]({{ config.extra.opensanctions_url }}/reference/) that underly the way that `yente` performs data matching.

There are multiple ways to produce FtM data, but the most convenient is to [import structured data via a mapping file](https://followthemoney.tech/docs/mappings/) using the `ftm` set of command-line tools. This allows reading data from a CSV file or SQL database and converting each row into entities.

There's also a [Python API](https://followthemoney.tech/python/) for creating FtM entities programmatically.

Don't forget to [`ftm aggregate`](https://followthemoney.tech/docs/fragments/) your datasets before indexing them in `yente`!

## Configuring a manifest file

Defining these extra indexing options is handled via a YAML manifest file read by `yente`. (The file needs to be accessible to the application, which may require the use of a [Docker volume](https://docs.docker.com/storage/volumes/) or a Kubernetes [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/#using-configmaps-as-files-from-a-pod)). The manifest file [can also be configured](settings.md) as a HTTP/HTTPS URL which the yente application will download upon startup. An example manifest might look like this:

```yaml
# Import external dataset specifications from OpenSanctions. This will fetch the dataset
# metadata from the given index and make them available to yente.
catalogs:
  - # nb. replace `latest` with a date stamp (e.g. 20220419) to fetch historical
    # OpenSanctions data for a particular day:
    url: "https://data.opensanctions.org/datasets/latest/index.json"
    # Limit the dataset scope of the entities which will be indexed into yente. Useful
    # values include `default`, `sanctions` or `peps`.
    scope: default
    resource_name: entities.ftm.json

  ## Alternative of above. auth_token will be sent in the Authorization header
  #- url: "https://delivery.opensanctions.com/datasets/latest/index.json"
  #  auth_token: "secretsecret" # $ENVIRONMENT_VARIABLE expansion supported
  #  scope: default
  #  resource_name: entities.ftm.json

  # Additional data catalogs can be specified. Using catalog entries for
  # additional datasets (rather than dataset entries shown below) has the advantage
  # that a catalog file will specify the latest update date of each dataset, and thus
  # changes to the datasets in the catalog will automatically trigger a re-index in yente:
  - url: "https://data.opensanctions.org/graph/catalog.json"
    # Make sure to limit the scope such that two catalog entries don't load datasets
    # with the same name. In this case the `graph` dataset contains all the datasets
    # in default so if this entry doesn't have a sufficiently specific scope constraint,
    # the catalog loading `default` above should be commented out.
    resource_name: entities.ftm.json

# The next section begins to specify non-OpenSanctions datasets that should be exposed
# in the API:
datasets:
  # Example A: fetch a public dataset from a URL and include it in the default search
  # scope defined upstream in OpenSanctions:
  - name: offshoreleaks
    title: ICIJ OffshoreLeaks
    entities_url: https://data.opensanctions.org/datasets/latest/icij_offshoreleaks/entities.ftm.json
  # Example B: a local dataset from a path that is visible within the container used
  # to run the service:
  - name: blocklist
    title: Customer Blocklist
    path: /data/customer-blocklist.json
    # Incrementing the version will force a re-indexing of the data. It must be
    # given as a monotonic increasing number.
    version: "20220419001"
  # Example C: a combined collection that allows querying all entities in its member
  # datasets at the same time. This can be used to specify a custom subset of lists
  # that should be considered in a particular screening process:
  - name: full
    title: Full index
    datasets:
      - offshoreleaks
      - blocklist
      # include OpenSanctions collections:
      - sanctions
```

### Learn more

* [FollowTheMoney documentation](https://followthemoney.tech/)
* FtM was designed for [Aleph](https://openaleph.org/), an investigations data platform
