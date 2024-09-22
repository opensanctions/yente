FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

LABEL org.opencontainers.image.title="OpenSanctions yente"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.source="https://github.com/opensanctions/yente"

RUN apt-get -qq -y update \
    && apt-get -y upgrade \
    && apt-get -y install locales ca-certificates tzdata curl python3-pip \
    python3-icu python3-cryptography python3-venv libicu-dev pkg-config \
    libleveldb-dev libleveldb1d \
    && apt-get -qq -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
    && ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && groupadd -g 10000 -r app \
    && useradd -m -u 10000 -s /bin/false -g app app

ENV LANG="en_US.UTF-8" \
    TZ="UTC"

RUN python3 -m venv /venv
ENV PATH="/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
RUN /venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel
RUN mkdir -p /app
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -e /app

USER app:app
CMD ["yente", "serve"]