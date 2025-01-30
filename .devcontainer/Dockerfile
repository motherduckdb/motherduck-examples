FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye 

# Install Node.js
ARG INSTALL_NODE="true"
ARG NODE_VERSION="lts/*"
RUN if [ "${INSTALL_NODE}" = "true" ]; then su vscode -c "source /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi
RUN apt-get update
RUN apt-get install -y gcc musl-dev
ENV PYTHONUNBUFFERED 1

# Install UV via pip
RUN pip install uv==0.5.25 --no-cache-dir
