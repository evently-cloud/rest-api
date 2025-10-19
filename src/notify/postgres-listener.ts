import { Instant } from "@js-joda/core"
import Long from "long"
import P from "pino"
import { Sql } from "postgres"

import { fromPostgresString } from "../db/selector-sql.ts"
import { eventIdToString } from "../eventId-utils.ts"
import { EventListener, EventListenerRegistrar } from "./notify.ts"
import { PersistedEvent } from "../api/type/persisted-event.ts"
import { EventID, ShutdownHookRegistrar, UnknownObject } from "../types.ts"


export async function init(shutdown:  ShutdownHookRegistrar,
                           logger:    P.Logger,
                           sql:       Sql): Promise<EventListenerRegistrar> {
  logger.info("init Postgres Event Notifier")
  const listeners: EventListener[] = []

  // will re-attach listener if DB goes down and then comes back up
  const listener = await sql.listen("ALL_EVENTS", (r) => handleEventNotify(logger, sql, listeners, r))
  shutdown("Postgres Event Listener", listener.unlisten)

  return {
    addEventListener: (l) => addEventListener(listeners, l),
    removeEventListener: (l) => removeEventListener(listeners, l)
  }
}


async function addEventListener(listeners: EventListener[], listener: EventListener) {
  listeners.push(listener)
}


function removeEventListener(listeners: EventListener[], listener: EventListener) {
  const pos = listeners.indexOf(listener)
  if (pos > -1) {
    listeners.splice(pos, 1)
  }
}


type NotifyRow = {
  eventId:  EventID
  event:    string
  entities: Record<string, string[]>
  meta?:    UnknownObject
  data?:    any
}


function toPersistedEvent(row: NotifyRow): PersistedEvent {
  const {
    eventId: eventIdIn,
    event,
    entities,
    meta = {},
    data = ""
  } = row

  const eventId = eventIdToString(eventIdIn)
  const tsInstant = Instant.ofEpochMicro(eventIdIn.timestamp.toNumber())
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


async function fetchMissingEventColumns(sql:      Sql,
                                        eventRow: NotifyRow,
                                        needMeta: boolean): Promise<NotifyRow> {
  const {
    eventId: {
      ledgerId,
      timestamp
    }
  } = eventRow

  const result = await sql`SELECT evently.fetch_missing_data(${ledgerId}::TEXT, ${timestamp}::BIGINT, ${needMeta}::BOOLEAN)`

  const {meta, data} = result[0]
  if (needMeta) {
    eventRow.meta = meta
  }
  eventRow.data = data

  return eventRow
}


async function handleEventNotify(logger:    P.Logger,
                                 sql:       Sql,
                                 listeners: EventListener[],
                                 row = "") {
  // don't send event if no listeners
  if (listeners.length > 0) {
    let eventRow = rowToEventRow(row)
    const missingMeta = eventRow.meta === undefined
    const missingData = eventRow.data === undefined
    if (missingMeta || missingData) {
      logger.debug("  missingMeta: %s, missingData: %s", missingMeta, missingData)
      eventRow = await fetchMissingEventColumns(sql, eventRow, missingMeta)
    }

    const event = toPersistedEvent(eventRow)
    listeners.forEach((l) => l(event))
  }
}


/*
  Incoming notification is a CSV:

  0: ledger_id
  1: timestamp
  2: checksum
  3: event
  4: entities
  following are present if space allows
  5: meta
  6: data
 */

const fieldPattern = /[E]?'((?:[^']|'')*)'|[^,]+/g

function rowToEventRow(row: string): NotifyRow {
  const fields = []
  let field
  while ((field = fieldPattern.exec(row)) !== null) {
    const [whole, quoted] = field
    if (quoted === undefined) {
      fields.push(whole)
    } else {
      let converted = fromPostgresString(quoted)
      if (whole.startsWith("E")) {
        converted = converted.replaceAll("\\\\", "\\")
      }
      fields.push(converted)
    }
  }

  const [
    ledgerId,
    tsStr,
    chkStr,
    event,
    entitiesStr,
    metaStr,
    dataStr
  ] = fields

  return {
    eventId: {
      ledgerId,
      timestamp:  Long.fromString(tsStr),
      checksum:   Long.fromString(chkStr, true).toInt()
    },
    event,
    entities: maybeParseJson(entitiesStr) as Record<string, string[]>,
    meta:     maybeParseJson(metaStr),
    data:     maybeParseJson(dataStr)
  }
}


function maybeParseJson(input: string | undefined): any {
  return input === undefined
    ? input
    : JSON.parse(input)
}
