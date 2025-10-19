import { Instant } from "@js-joda/core"
import createHttpError from "http-errors"
import { concat, flatMap, pipeAsync } from "iter-ops"
import Long from "long"
import P from "pino"
import PG, { Sql } from "postgres"
import { Readable } from "node:stream"

import { PersistedEvent } from "../api/type/persisted-event.ts"
import { filterSelectorToSql, unknownSelectorToSql } from "../db/selector-sql.ts"
import { toEventIdString, eventIdToString } from "../eventId-utils.ts"
import { stringToBytes } from "../hex-utils.ts"
import { EventID, FilterSelector, Ledger, Selector, UnknownObject } from "../types.ts"
import { SelectorResult } from "./index.ts"



const EVENT_BATCH_SIZE = 100

type EventRow = {
  timestamp:  Long
  checksum:   Long
  event:      string
  entities:   Record<string, string[]>
  meta:       UnknownObject
  data:       any
}


export function allEvents(logger:   P.Logger,
                          sql:      Sql,
                          ledger:   Ledger,
                          selector: Selector) {
  return executeSelector(logger, sql, ledger, selector, "true")
}


export function runFilter(logger:    P.Logger,
                          sql:       Sql,
                          ledger:    Ledger,
                          selector:  FilterSelector) {
  const query = filterSelectorToSql(selector)
  return executeSelector(logger, sql, ledger, selector, query)
}


export async function latestEventId(logger:    P.Logger,
                                    sql:       Sql,
                                    ledger:    Ledger,
                                    selector:  Selector): Promise<EventID> {
  const {limit, after} = selector
  validateAfterInLedger(after, ledger)
  const {id: ledgerId} = ledger
  let query = unknownSelectorToSql(selector)
  const selectorBytes = stringToBytes(query)
  const result = await sql`SELECT timestamp, checksum FROM
       evently.fetch_event_id(${ledgerId}, ${selectorBytes}, ${after?.timestamp || null}, ${limit || null})`

  const idRow = result.shift()
  if (idRow) {
    return {
      ledgerId,
      timestamp:  idRow.timestamp,
      checksum:   idRow.checksum.toUnsigned().toInt()
    }
  }
  return after ?? ledger.genesis
}


async function executeSelector(logger:    P.Logger,
                               sql:       Sql,
                               ledger:    Ledger,
                               selector:  Selector,
                               query:     string): Promise<SelectorResult> {
  const {after, limit = 0} = selector
  validateAfterInLedger(after, ledger)
  const selectorBytes = stringToBytes(query)
  const [eventId, results] = await tryExecuteSelector(sql, ledger, limit, selectorBytes, after)

  // no matching events
  if (results.length === 0) {
    return {
      position:     eventId,
      eventStream:  Readable.from([])
    }
  }

  return continueSelector(logger, sql, selectorBytes, eventId, limit, results)
}


async function tryExecuteSelector(sql:      Sql,
                                  ledger:   Ledger,
                                  limit:    number,
                                  selector: Uint8Array,
                                  after?:   EventID): Promise<[EventID, EventRow[]]> {
  const {id: ledgerId} = ledger
  try {
    // limit has to be passed in as the selector declared, so the returned header row will have the right ledger position.
    const rows = await sql<EventRow[]>`
      SELECT timestamp, checksum, event, entities, meta, data 
      FROM evently.run_selector(
        ${ledgerId}::TEXT,
        ${after?.timestamp ?? Long.ZERO}::BIGINT,
        ${after?.checksum ?? 0}::BIGINT,
        ${limit}::INT,
        ${selector}::BYTEA,
        ${EVENT_BATCH_SIZE}::INT)`

    const idRow = rows.shift()
    if (idRow) {
      const {timestamp, checksum} = idRow
      const position = {
          ledgerId,
          timestamp,
          checksum: checksum.toUnsigned().toInt()
      }
      return [position, rows]
    }
    return [after ?? ledger.genesis, []]
  } catch (err) {
    if (err instanceof PG.PostgresError) {
      // syntax error
      if (err.code === "42601") {
        throw createHttpError.BadRequest(err.message)
      }
      if (err.message.startsWith("AFTER not found")) {
        // after already checked for undefined
        const afterStr = eventIdToString(after as EventID)
        throw createHttpError.BadRequest(`after '${afterStr}' not found in this ledger.`)
      }
    }
    throw err
  }
}


async function continueSelector(logger:         P.Logger,
                                sql:            Sql,
                                selectorBytes:  Uint8Array,
                                position:        EventID,
                                limit:          number,
                                firstBatch:     EventRow[]): Promise<SelectorResult> {
  const {ledgerId} = position
  const rowToEvent = (row: EventRow) => eventRowToPersistedEvent(ledgerId, row)
  const allEventsPresent = firstBatch.length < EVENT_BATCH_SIZE

  let events
  if (allEventsPresent) {
    events = firstBatch.map(rowToEvent)
  } else {
    const {timestamp, checksum} = firstBatch.at(-1) as EventRow
    const startPosition = {
      ledgerId,
      timestamp,
      checksum: checksum.toUnsigned().toInt()
    }
    /*
      Problem with this approach is that AsyncGenerator does not buffer or read ahead. I want a push stream
      to fetch more rows while the previous ones are being sent to the client.
     */
    const batchIterator = selectEvents(sql, startPosition, limit, selectorBytes)
    events = pipeAsync(
      firstBatch,
      concat(batchIterator),
      flatMap(rowToEvent))
  }

  const eventStream = Readable.from(events)
  return {
    position,
    eventStream
  }
}


async function* selectEvents(sql:           Sql,
                             start:         EventID,
                             totalLimit:    number,
                             selectorBytes: Uint8Array): AsyncGenerator<EventRow> {
  const {ledgerId, timestamp} = start
  if (totalLimit === 0) {
    // Number.POSITIVE_INFINITY will never decrease
    totalLimit = Number.POSITIVE_INFINITY
  }
  let after = timestamp
  let rows: EventRow[] = []

  while (totalLimit > 0) {
    rows = await fetchRows(sql, ledgerId, after, selectorBytes, totalLimit)
    totalLimit -= rows.length

    if (rows.length) {
      after = (rows.at(-1) as EventRow).timestamp
      for (const row of rows) {
        yield row
      }
    } else {
      break
    }
  }
}


async function fetchRows(sql:           Sql,
                         ledgerId:      string,
                         after:         Long,
                         selectorBytes: Uint8Array,
                         totalLimit:    number): Promise<EventRow[]> {
  const limit = Math.min(totalLimit, EVENT_BATCH_SIZE)

  return sql<EventRow[]>`SELECT timestamp, checksum, event, entities, meta, data 
    FROM evently.fetch_selected(
        ${ledgerId}::TEXT, 
        ${after}::BIGINT, 
        ${limit}::INT, 
        ${selectorBytes}::BYTEA)`
}


function validateAfterInLedger(after: EventID | undefined, ledger: Ledger) {
  if (after && ledger.id !== after.ledgerId) {
    throw createHttpError.BadRequest("Ledger ID in 'after' does not match the current ledger. Are you using the correct authorization token for this request?")
  }
}


function eventRowToPersistedEvent(ledgerId: string, row: EventRow): PersistedEvent {
  const {timestamp: ts, checksum, event, entities, meta, data} = row
  const eventId = toEventIdString(ts, checksum.toUnsigned().toInt(), ledgerId)
  const tsInstant = Instant.ofEpochMicro(ts.toNumber())
  const timestamp = tsInstant.toString()
  return {
    event,
    eventId,
    timestamp,
    entities,
    meta,
    data
  }
}
