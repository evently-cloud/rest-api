import { pino } from "pino"

import * as api from "./api/index.ts"
import * as pg from "./db/sql.ts"
import * as eventSource from "./event-source/index.ts"
import * as eventStore from "./event-store/index.ts"
import * as eventLedgers from "./ledgers/index.ts"
import * as notifyChannels from "./notify/channels-mem.ts"
import * as eventRegistry from "./registry/index.ts"
import { initShutdownRegistrar } from "./shutdown.ts"


export async function initAll(): Promise<string> {
  const inProduction = process.env.NODE_ENV === "production"
  const level = process.env.LOG_LEVEL ?? "trace"

  const logger = inProduction
    ? pino({ level })
    : pino({
      level,
      transport: {
        target: "pino-pretty",
        options: {
          colorize:       true,
          ignore:         "hostname,pid",
          translateTime:  "SYS:standard"
        }
      }
    })

  logger.info("Evently Consumer API")

  const shutdown = initShutdownRegistrar(logger)

  // create db pool
  const sql = await pg.init(shutdown, logger)
  // create ledgers
  const ledgers = eventLedgers.init(logger, sql)
  // create event source
  const source = eventSource.init(logger, sql)
  // create registry
  const registry = eventRegistry.init(logger, sql)
  // create event store
  const store = eventStore.init(logger, sql, registry)
  // create the notification channels
  const channels = await notifyChannels.init(shutdown, logger, sql)

  // launch API
  return api.launch(shutdown, logger, ledgers, registry, source, store, channels)
}
