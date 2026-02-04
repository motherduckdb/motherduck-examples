# Node.js MotherDuck examples

This folder shows how to connect to MotherDuck using the [DuckDB Neo driver](https://duckdb.org/docs/clients/node_neo/overview) for Node.js. See our [documentation](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/connecting-to-motherduck/) for more details.

There are two example scripts:
- **`src/basic.js`** - Simple connection, queries, parameterized statements, and aggregations
- **`src/connection-pool.js`** - Connection pooling for concurrent queries using `generic-pool`

## Requirements

- Node.js 22+ (uses native ESM modules)
- npm
- [MotherDuck token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#creating-an-access-token) - Rename `.env.template` to `.env` and add your token

## Setup

```bash
# Install dependencies
npm install

# Copy environment template and add your token
cp .env.template .env
# Edit .env and add your MOTHERDUCK_TOKEN
```

## Commands

```bash
npm run basic  # Run basic.js - simple queries and examples
npm run pool   # Run connection-pool.js - concurrent queries with pooling
```

## Examples

### Basic connection

```javascript
import { DuckDBInstance } from "@duckdb/node-api";

const token = process.env.MOTHERDUCK_TOKEN;
const instance = await DuckDBInstance.create(`md:my_db?motherduck_token=${token}`);
const connection = await instance.connect();

const reader = await connection.runAndReadAll("SELECT 42 AS answer");
console.log(reader.getRowObjects()); // [{ answer: 42 }]
```

### Parameterized queries

```javascript
const prepared = await connection.prepare("SELECT * FROM users WHERE id = $1");
prepared.bindInteger(1, 42);
const reader = await prepared.runAndReadAll();
console.log(reader.getRowObjects());
```

### Connection pooling

For applications with concurrent queries, use a connection pool to manage multiple connections efficiently. See `src/connection-pool.js` for a complete implementation using `generic-pool`.

## Documentation

- [MotherDuck Node.js connection guide](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/connecting-to-motherduck/)
- [MotherDuck connection pooling](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/multithreading-and-parallelism/multithreading-and-parallelism-nodejs/)
- [DuckDB Neo driver overview](https://duckdb.org/docs/clients/node_neo/overview)
