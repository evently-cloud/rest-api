import P from "pino"
import { Sql } from "postgres"

import { wrapWithCatch } from "../db/sql.ts"
import { Ledger } from "../types.ts"
import {
  unregisterEventType,
  entitiesInLedger,
  eventsForEntity,
  eventsInLedger,
  getEvent,
  registerEventType
} from "./postgres-registry.ts"


export type EventType = {
  event:    string,
  entities: string[]
}


export interface Registry {
  allEvents(ledger: Ledger): Promise<EventType[]>
  getEvent(ledger: Ledger, event: string): Promise<EventType | undefined>
  entities(ledger: Ledger): Promise<string[]>
  eventsForEntity(ledger: Ledger, entity: string): Promise<EventType[]>
  registerEventType(ledger: Ledger, event: string, entities: string[]): Promise<void>
  deleteEvent(ledger: Ledger, event: string): Promise<void>
}


export function init(logger: P.Logger, sql: Sql): Registry {
  return {
    allEvents:         (l) => wrapWithCatch(logger, eventsInLedger(logger, sql, l)),
    getEvent:          (l, e) => wrapWithCatch(logger, getEvent(logger, sql, l, e)),
    entities:          (l) => wrapWithCatch(logger, entitiesInLedger(logger, sql, l)),
    eventsForEntity:   (l, e) => wrapWithCatch(logger, eventsForEntity(logger, sql, l, e)),
    registerEventType: (l, en, ev) => wrapWithCatch(logger, registerEventType(logger, sql, l, en, ev)),
    deleteEvent:       (l, e) => wrapWithCatch(logger, unregisterEventType(logger, sql, l, e))
  }
}
