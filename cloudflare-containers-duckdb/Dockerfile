# Cloudflare Workers Containers image that runs Bun + DuckDB native.
FROM debian:12-slim

ARG BUN_VERSION=1.1.38
ENV BUN_INSTALL=/usr/local/bun
ENV PATH="${BUN_INSTALL}/bin:${PATH}"

RUN apt-get update && \
  apt-get install -y curl ca-certificates unzip && \
  rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://bun.sh/install | bash -s "bun-v${BUN_VERSION}" \
  && ln -s ${BUN_INSTALL}/bin/bun /usr/local/bin/bun \
  && bun --version

WORKDIR /app

COPY package.json bun.lockb ./
RUN bun install --frozen-lockfile

COPY .env ./
COPY src ./src

EXPOSE 3000
CMD ["bun", "run", "src/index.ts"]
