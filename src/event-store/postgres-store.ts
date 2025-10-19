import { createId } from "@paralleldrive/cuid2"
import createHttpError from "http-errors"
import { isEqual } from "lodash-es"
import Long from "long"
import P from "pino"
import PG, { Sql } from "postgres"

import { PATHS as REG_PATHS } from "../api/registry-endpoints.ts"
import { unknownSelectorToSql } from "../db/selector-sql.ts"
import { toEventIdString } from "../eventId-utils.ts"
import { stringToBytes } from "../hex-utils.ts"
import { Registry } from "../registry/index.ts"
import { EventID, Ledger, Selector } from "../types.ts"
import { AppendEvent, AppendResult, Result } from "./index.ts"


/**
 * In order of insertion to evently.append_event
 */
type AppendEventPostgres = {
  previousEventId:  string,
  event:            string,
  entities:         Record<string, string[]>,
  meta:             any,
  data:             any,
  appendKey:        string,
  selector:         Uint8Array
}


export function appendFact(logger:    P.Logger,
                           sql:       Sql,
                           registry:  Registry,
                           ledger:    Ledger,
                           event:     AppendEvent): Promise<AppendResult> {
  return appendEvent(logger, sql, registry, ledger, event, "false")
}


export function appendWithSelector(logger:   P.Logger,
                                   sql:      Sql,
                                   registry: Registry,
                                   ledger:   Ledger,
                                   event:    AppendEvent,
                                   selector: Selector): Promise<AppendResult> {
  const selectorQuery = unknownSelectorToSql(selector)
  const {after} = selector

  return appendEvent(logger, sql, registry, ledger, event, selectorQuery, after)
}


async function appendEvent(logger:      P.Logger,
                           sql:         Sql,
                           registry:    Registry,
                           ledger:      Ledger,
                           event:       AppendEvent,
                           selectorSql: string,
                           after?:      EventID): Promise<AppendResult> {

  await validateEvent(registry, ledger, event)

  const {
    event: eventName,
    entities,
    meta = null,
    data = null,
    idempotencyKey
  } = event

  const selectorBytes = stringToBytes(selectorSql)
  const afterTimestamp = after?.timestamp ?? Long.ZERO
  const afterChecksum = after?.checksum ?? 0
  const previousEventId = toEventIdString(afterTimestamp, afterChecksum, ledger.id)

  const values: AppendEventPostgres = {
    previousEventId,
    event: eventName,
    entities,
    meta,
    data,
    appendKey: idempotencyKey || createId(),
    selector: selectorBytes

  }
  return tryAppendResult(logger, sql, ledger, event, values)
}


async function validateEvent(registry:  Registry,
                             ledger:    Ledger,
                             eventIn:   AppendEvent) {
  const {entities, event} = eventIn

  const eventDef = await registry.getEvent(ledger, event)
  if (!eventDef) {
    throw createHttpError.UnprocessableEntity(`event '${event}' not found in this ledger. Visit ${REG_PATHS.REGISTER_EVENT} to register new events.`)
  }

  const errors: string[] = []
  Object.keys(entities).forEach((entity) => {
    if (!eventDef.entities.includes(entity)) {
      errors.push(`entity '${entity}' is not registered to be part of this event.`)
    }
  })

  if (errors.length) {
    throw createHttpError.UnprocessableEntity(errors.join("\n"))
  }
}

async function tryAppendResult(logger:          P.Logger,
                               sql:             Sql,
                               ledger:          Ledger,
                               appendEvent:     AppendEvent,
                               appendValues:    AppendEventPostgres): Promise<AppendResult> {
  const {
    previousEventId,
    event,
    entities,
    meta,
    data,
    appendKey,
    selector
  } = appendValues

  try {
    const [{event_id: eventId}] = await sql`SELECT evently.append_event(
  ${previousEventId}::UUID,
  ${event}::TEXT,
  ${sql.json(entities)}::JSONB,
  ${sql.json(meta)}::JSONB,
  ${sql.json(data)}::JSONB,
  ${appendKey}::TEXT,
  ${selector}::BYTEA
) AS event_id`

    return {
      status: Result.SUCCESS,
      ok: {
        eventId,
        idempotencyKey: appendKey
      }
    }
  } catch (err) {
    // https://www.enterprisedb.com/edb-docs/d/postgresql/reference/manual/12.3/errcodes-appendix.html
    if (err instanceof PG.PostgresError) {
      if (err.message.startsWith("RACE CONDITION")) {
        const raceResult = {
          status: Result.RACE,
          error:  "Race Condition! Entity has newer events. Please GET /SELECTOR for the most recent events."
        }
        return appendKey
          ? maybeIdempotent(logger, sql, ledger, appendEvent, raceResult)
          : raceResult
      }
      if (appendKey && err.constraint_name?.endsWith("_append_key_key")) {
        return maybeIdempotent(logger, sql, ledger, appendEvent, {
          status: Result.ERROR,
          message: "idempotencyKey reused for a different event. Please generate a new idempotencyKey and repeat the append request."
        })
      }
      if (err.message === "previous can only be genesis for first event") {
        return {
          status: Result.ERROR,
          message: "Ledger already has events, must send most recent selector event id as 'after' in append body."
        }
      }
      if (err.message === "previous_id must exist in the ledger") {
        return {
          status: Result.ERROR,
          message: `Previous Event ID is not found in ledger.`
        }
      }
      if (err.message.startsWith("AFTER not found")) {
        return {
          status: Result.ERROR,
          message: `'after' value not found in ledger.`
        }
      }
    }

    const refNum = `ref#${createId()}`
    logger.error(err as Error, `unhandled store error, ${refNum}`)
    const httpMessage = `Something went wrong on our side. Please contact us with ${refNum} for investigation.`
    throw createHttpError.InternalServerError(httpMessage)
  }
}


async function maybeIdempotent(logger:        P.Logger,
                               sql:           Sql,
                               ledger:        Ledger,
                               newEvent:      AppendEvent,
                               appendResult:  AppendResult): Promise<AppendResult> {
  const {
    idempotencyKey = "NO_IDEMPOTENCY_KEY",
    event,
    entities,
    meta,
    data
  } = newEvent
  const {id: ledgerId} = ledger

  const result = await sql`SELECT timestamp, checksum, event, entities, meta, data
    FROM evently.find_with_append_key(${ledgerId}::TEXT, ${idempotencyKey}::TEXT)`

  if (result.count) {
    // get the event, compute event ID to return idempotent result
    const [{
      timestamp:  storedTimestamp,
      checksum:   storedChecksum,
      event:      storedEvent,
      entities:   storedEntities,
      meta:       storedMeta,
      data:       storedData,
    }] = result

    if (event === storedEvent
        && isEqual(storedEntities, entities)
        && isEqual(storedMeta, meta)
        && isEqual(storedData, data)) {

      const eventId = toEventIdString(storedTimestamp, storedChecksum.toUnsigned().toInt(), ledgerId)
      appendResult = {
        status: Result.SUCCESS,
        ok: {
          eventId,
          idempotencyKey
        }
      }
    } else {
      throw createHttpError.UnprocessableEntity(`Event does not match the event originally appended with idempotencyKey: '${idempotencyKey}'`)
    }
  }
  return appendResult
}
