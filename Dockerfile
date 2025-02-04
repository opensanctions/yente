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
    libicu-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

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

RUN useradd -u 10000 -s /bin/false app
USER app

WORKDIR /app
COPY --from=build /opt/venv /opt/venv
COPY --from=build /app /app

CMD ["yente", "serve"]
