import { createId } from "@paralleldrive/cuid2"
import createHttpError from "http-errors"
import Long from "long"
import P from "pino"
import { Sql } from "postgres"
import { LruMap } from "toad-cache"
import { runFilter } from "../event-source/postgres-selector.ts"

import { toEventIdString } from "../eventId-utils.ts"
import { stringToBytes } from "../hex-utils.ts"
import { FilterSelector, LEDGER, Ledger } from "../types.ts"
import { EventType } from "./index.ts"


const EVENT_REGISTERED = "🟧𒍪"    // "event" emoji (orange sticky), "know" Sumerian
const EVENT_UNREGISTERED = "🟧𒁁"  // "event" emoji (orange sticky), "complete" Sumerian

// key = ledger.hex, value = EventType[]
const cache = new LruMap<Promise<EventType[]>>(1_000, 10_000)


function cachedAllEvents(logger: P.Logger,
                         sql:    Sql,
                         ledger: Ledger): Promise<EventType[]> {
  const key = ledger.id
  let eem = cache.get(key)
  if (!eem) {
    eem = reduceAllEvents(logger, sql, ledger)
    cache.set(key, eem)
  }
  return eem
}


async function reduceAllEvents(logger: P.Logger,
                               sql:    Sql,
                               ledger: Ledger): Promise<EventType[]> {
  const selector: FilterSelector = {
    entities: {
      [LEDGER]: [ledger.id]
    },
    events: {
      [EVENT_REGISTERED]: {
        query: "$"
      },
      [EVENT_UNREGISTERED]: {
        query: "$"
      }
    }
  }

  const results = await runFilter(logger, sql, ledger, selector)

  const eventEntitiesMap = new Map<string, string[]>()

  for await (const {event: action, data} of results.eventStream) {
    const {event, entities} = data
    switch (action) {
      case EVENT_REGISTERED:
        eventEntitiesMap.set(event, entities)
        break
      case EVENT_UNREGISTERED:
        eventEntitiesMap.delete(event)
        break
      default:
        throw new Error(`Unknown event in stream: ${event}`)
    }
  }

  return [...eventEntitiesMap].reduce((acc, [event, entities]) => {
    acc.push({event, entities})
    return acc
  }, [] as EventType[])
}


export async function eventsInLedger(logger:  P.Logger,
                                     sql:     Sql,
                                     ledger:  Ledger): Promise<EventType[]> {
  return cachedAllEvents(logger, sql, ledger)
}


export async function getEvent(logger:  P.Logger,
                               sql:     Sql,
                               ledger:  Ledger,
                               event:   string): Promise<EventType | undefined> {
  const allEvents = await cachedAllEvents(logger, sql, ledger)
  return allEvents.find(e => e.event === event)
}


export async function entitiesInLedger(logger:  P.Logger,
                                       sql:     Sql,
                                       ledger:  Ledger): Promise<string[]> {
  const allEvents = await cachedAllEvents(logger, sql, ledger)
  const allEntities = allEvents.reduce((acc, event) => {
    event.entities.forEach(e => acc.add(e))
    return acc
  }, new Set<string>)
  return [...allEntities]
}


export async function eventsForEntity(logger: P.Logger,
                                      sql:    Sql,
                                      ledger: Ledger,
                                      entity: string): Promise<EventType[]> {
  const entityEvents = await cachedAllEvents(logger, sql, ledger)
  return entityEvents.filter(event => event.entities.includes(entity))
}


const NONE = stringToBytes("false")

export async function registerEventType(logger:   P.Logger,
                                        sql:      Sql,
                                        ledger:   Ledger,
                                        event:    string,
                                        entities: string[]): Promise<void> {
  if (entities.includes(LEDGER)) {
    throw createHttpError.Forbidden(`Cannot register events to the '${LEDGER}' entity.`)
  }

  // see if this event is already added
  const events = await reduceAllEvents(logger, sql, ledger)

  // Check if event already exists with same entities
  const existingEvent = events.find(et =>
    et.event === event
    && entities.length === et.entities.length
    && et.entities.every(entity => entities.includes(entity))
  )

  if (existingEvent) {
    // ignore registering the same event again
    return
  }

  const {id} = ledger
  // this means one can redefine an event's entities at any time without unregistering it first
  logger.info("ledgerId %s registering event type %s for entities %o", id, event, entities)

  const previousId = toEventIdString(Long.ZERO, 0, id)
  const ledgerEntity = sql.json({
    [LEDGER]: [id]
  })
  const meta = sql.json({})
  const data = sql.json({event, entities})

  await sql`SELECT evently.append_event(
     ${previousId}::UUID, ${EVENT_REGISTERED}::TEXT, ${ledgerEntity}::JSONB, ${meta}::JSONB, ${data}::JSONB, ${createId()}::TEXT, ${NONE}::BYTEA)`

  cache.delete(id)
}


export async function unregisterEventType(logger: P.Logger,
                                          sql:    Sql,
                                          ledger: Ledger,
                                          event:  string): Promise<void> {

  // see if this event is already added
  const events = await reduceAllEvents(logger, sql, ledger)
  const registeredEvent = events.find((et) => et.event === event)

  if (registeredEvent) {
    const {id} = ledger
    const previousId = toEventIdString(Long.ZERO, 0, id)
    const ledgerEntity = sql.json({
      [LEDGER]: [id]
    })
    const meta = sql.json({})
    const data = sql.json({event})

    await sql`SELECT evently.append_event(
       ${previousId}::UUID, ${EVENT_UNREGISTERED}::TEXT, ${ledgerEntity}::JSONB, ${meta}::JSONB, ${data}::JSONB, ${createId()}::TEXT, ${NONE}::BYTEA)`

    cache.delete(id)
  } else {
    throw createHttpError.NotFound(`Event type '${event}' does not exist.`)
  }
}
