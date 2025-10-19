import createHttpError from "http-errors"
import P from "pino"
import PG, { Sql } from "postgres"

import { CreateLedgerForm } from "../api/type/create-ledger-form.ts"
import { filterSelectorToSql } from "../db/selector-sql.ts"
import { maybeFromEventIdString } from "../eventId-utils.ts"
import { stringToBytes } from "../hex-utils.ts"
import { EventID, FilterSelector, Ledger } from "../types.ts"
import { clearCacheForLedger } from "./ledger-cache.ts"


export async function allLedgers(sql: Sql): Promise<Ledger[]> {
  return sql<Ledger[]>`SELECT id, name FROM evently.ledgers`
}


export async function createLedger(logger:      P.Logger,
                                   sql:         Sql,
                                   ledgerForm:  CreateLedgerForm): Promise<Ledger | undefined> {
  const {
    name,
    description
  } = ledgerForm
  logger.info("Creating ledger '%s'", name)

  let ledgerId

  try {
    const [{ ledger_id }] = await sql`SELECT evently.create_ledger(${name}, ${description}) AS ledger_id`
    ledgerId = ledger_id
  } catch (err) {
    if (err instanceof PG.PostgresError && err.code === "23505") {
      logger.info(`Ledger '${name}' already exists`)
      const [{ id }] = await sql`SELECT id FROM evently.ledgers WHERE name = ${name}`
      ledgerId = id
    } else {
      throw err
    }
  }
  clearCacheForLedger(ledgerId)
  return await ledgerForId(logger, sql, ledgerId)
}


const genesisEventSelector: FilterSelector = {
  events: {
    "📒𒃻": {
      query: "$"
    }
  },
  limit: 1
}
const selectorBytes = stringToBytes(filterSelectorToSql(genesisEventSelector))

export async function ledgerForId(logger: P.Logger,
                                  sql:    Sql,
                                  id:     string): Promise<Ledger | undefined> {
  logger.debug("looking up ledger %s", id)

  try {
    const results = await sql`SELECT timestamp, checksum, data
    FROM evently.run_selector(${id}::TEXT, 0::BIGINT, 0::BIGINT, 1::INT, ${selectorBytes}::BYTEA, 1::INT)`

    if (results.length > 1) {
      // don't need header row
      const event = results[1]
      const {
        timestamp,
        checksum,
        data: {
          name,
          description
        }
      } = event

      return {
        id,
        name,
        description,
        genesis: {
          ledgerId: id,
          timestamp,
          checksum: checksum.toUnsigned().toInt()
        }
      }
    }
  } catch (err) {
    if (err instanceof PG.PostgresError && err.code === "42P01") {
      logger.warn(`Ledger '${id}' does not exist`)
      return undefined
    } else {
      throw err
    }
  }
}


export async function ledgerEventCount(sql:   Sql,
                                       {id}:  Ledger): Promise<number> {
  const [{count}] = await sql`SELECT evently.ledger_event_count(${id}) AS count`
  return count.toNumber()
}


export async function resetEvents(logger: P.Logger,
                                  sql:    Sql,
                                  ledger: Ledger,
                                  after?: string): Promise<any> {
  const {id, name} = ledger
  const afterInstance = maybeFromEventIdString(after)

  let ts
  if (afterInstance) {
    await checkAfterExists(sql, ledger, afterInstance)
    ts = afterInstance.timestamp
  } else {
    ts = ledger.genesis.timestamp
  }

  logger.info("Resetting ledger %s[%s] after %o", name, id, ts)

  return sql`SELECT evently.reset_ledger_events(${id}::TEXT, ${ts}::BIGINT)`
}


export async function removeLedger(logger: P.Logger,
                                   sql:    Sql,
                                   ledger: Ledger): Promise<void> {
  const {id, name} = ledger
  logger.info("Removing ledger %s[%s]", name, id)

  await sql`SELECT evently.remove_ledger(${id}::TEXT)`

  clearCacheForLedger(id)
}


async function checkAfterExists(sql:    Sql,
                                ledger: Ledger,
                                after:  EventID) {
  const {ledgerId, checksum, timestamp} = after

  if (ledgerId !== ledger.id) {
    throw createHttpError.BadRequest(`Cannot reset with 'after' ${after} because it is from a different Ledger.`)
  }

  try {
    await sql`SELECT evently.after_exists((${timestamp}::BIGINT, ${checksum}::BIGINT, ${ledgerId}::TEXT))`
  } catch (e) {
    throw createHttpError.BadRequest(`Cannot reset with 'after' ${after} because it does not exist in the Ledger.`)
  }
}
