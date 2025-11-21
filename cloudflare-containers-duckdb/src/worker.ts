import { Container, getRandom } from '@cloudflare/containers'
import type { DurableObjectNamespace } from '@cloudflare/workers-types'

export class DuckDBContainer extends Container {
  defaultPort = 3000
  requiredPorts = [3000]
  sleepAfter = '30m'
}

type Env = {
  DUCKDB_CONTAINER: DurableObjectNamespace
}

export default {
  async fetch(request: Request, env: Env) {
    const instance = await getRandom(env.DUCKDB_CONTAINER)
    return instance.fetch(request)
  }
}
