# Define the base image to use for both stages
ARG python_image=python:3.12-slim

# ------------------------------------------------------------------------
# Stage 1: Build and install dependencies
# ------------------------------------------------------------------------
FROM ${python_image} AS build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    locales \
    libicu-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Build locale definition
RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

# Install python package dependencies
ENV PATH="/opt/venv/bin:$PATH"
COPY . /app
RUN python -m venv /opt/venv
RUN pip install --no-cache-dir -e /app

# ------------------------------------------------------------------------
# Stage 2: Export a final runtime image
# ------------------------------------------------------------------------
FROM ${python_image}

LABEL org.opencontainers.image.title="OpenSanctions yente"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.source="https://github.com/opensanctions/yente"

# Set environment variables
ENV LANG="en_US.UTF-8"
ENV TZ="UTC"
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get update && apt-get install -y \
      libicu72 \
      ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


RUN useradd -u 10000 -s /bin/false app
USER app

WORKDIR /app
COPY --from=build /opt/venv /opt/venv
COPY --from=build /app /app
# Install locale definition - test with `locale -a`
COPY --from=build /usr/lib/locale/locale-archive /usr/lib/locale/locale-archive

CMD ["yente", "serve"]
