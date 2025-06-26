FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye 

ARG DUCKDB_VERSION=1.3.1
# Install Node.js
ARG INSTALL_NODE="true"
ARG NODE_VERSION="lts/*"
RUN if [ "${INSTALL_NODE}" = "true" ]; then su vscode -c "source /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi
RUN apt-get update
RUN apt-get install -y gcc musl-dev unzip
ENV PYTHONUNBUFFERED 1

# Install UV via pip
RUN pip install uv==0.7.15 --no-cache-dir

# Install DuckDB CLI based on architecture
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        curl -L -o duckdb_cli.zip https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-aarch64.zip; \
    elif [ "$ARCH" = "x86_64" ]; then \
        curl -L -o duckdb_cli.zip https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    unzip duckdb_cli.zip -d /usr/local/bin && \
    rm duckdb_cli.zip && \
    chmod +x /usr/local/bin/duckdb
