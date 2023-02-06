FROM ubuntu:23.04
ENV DEBIAN_FRONTEND noninteractive

LABEL org.opencontainers.image.title "OpenSanctions yente"
LABEL org.opencontainers.image.licenses MIT
LABEL org.opencontainers.image.source https://github.com/opensanctions/yente

RUN apt-get -qq -y update \
    && apt-get -qq -y upgrade \
    && apt-get -qq -y install locales ca-certificates tzdata curl python3-pip \
    python3-icu python3-cryptography libicu-dev pkg-config \
    && apt-get -qq -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
    && ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && groupadd -g 1000 -r app \
    && useradd -m -u 1000 -s /bin/false -g app app

ENV LANG='en_US.UTF-8' \
    TZ="UTC"

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -q -r /tmp/requirements.txt
RUN mkdir -p /app
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -q -e /app

USER app:app
CMD ["yente", "serve"]