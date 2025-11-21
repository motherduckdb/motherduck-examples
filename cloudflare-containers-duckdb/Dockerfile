# Cloudflare Workers Containers image that runs Bun + DuckDB native.
FROM oven/bun:1.1.38

WORKDIR /app

COPY package.json bun.lockb ./
RUN bun install --frozen-lockfile

COPY src ./src

EXPOSE 3000
CMD ["bun", "run", "src/index.ts"]
