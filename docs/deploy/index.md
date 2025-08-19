## Requirements

Running `yente` requires a server that can run host the main screening application (a lightweight Python application) and the ElasticSearch backend used to store and query entity information. In total, we anticipate 500 MB memory per Python service, and 2-4GB of memory plus 8-10GB of disk volume size for the ElasticSearch index. Running ElasticSearch on SSD-backed hard drives will produce a significant performance gain.

## Deploy using Docker containers

While it is possible to operate `yente` outside of Docker, we strongly encourage the use of containers as a simple means of dependency management and deployment. We provide pre-built containers of the latest released version of Yente at [`ghcr.io/opensanctions/yente:latest`](https://ghcr.io/opensanctions/yente).

### ...with docker-compose

For the `docker-compose` container orchestration tool, we provide an example [`docker-compose.yml`](https://github.com/opensanctions/yente/blob/main/docker-compose.yml) in the repository. You can use it to easily get started with Yente and later modify it to your individual needs.

```bash
mkdir -p yente && cd yente
wget https://raw.githubusercontent.com/opensanctions/yente/main/docker-compose.yml
docker-compose up
```

This will make the service available on Port 8000 of the local machine. You may have to wait for five to ten minutes until the service has finished indexing the data when it is first started.

**Next:** [Configure yente](settings.md)

### ...with Kubernetes

When scaling out, we recommend using Kubernetes or another managed cloud service (e.g. Google Cloud Run) to run multiple container instances of Yente. You will need to run both of the services defined in the compose file (the API and ElasticSearch instance). In this configuration, the yente workers must run with `YENTE_AUTO_REINDEX` disabled. You should configure a separate job that is launched periodically (for example, a Kubernetes CronJob) to perform reindexing to avoid multiple yente workers stepping on eachother's toes.

We provide an [example Kubernetes configuration](https://github.com/opensanctions/yente/blob/main/kubernetes.example.yml) in the repository. You may also need to assign the API container network policy permissions to fetch data from `data.opensanctions.org` once every hour so that it can update itself.
