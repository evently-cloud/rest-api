import P from "pino"
import { Sql } from "postgres"

import { AppendEventForm } from "../api/type/append-event-form.ts"
import { wrapWithCatch } from "../db/sql.ts"
import { Registry } from "../registry/index.ts"
import { appendFact, appendWithSelector } from "./postgres-store.ts"
import {Ledger, Selector} from "../types.ts"


export interface AppendEvent extends AppendEventForm {
  readonly idempotencyKey?: string
}

export interface AppendResult {
  readonly  status:   Result
  readonly  ok?:      AppendedEvent
            message?: string
}

export interface AppendedEvent {
  readonly eventId:         string
  readonly idempotencyKey?: string
}

export enum Result {
  SUCCESS = "SUCCESS",
  RACE    = "RACE CONDITION",
  FAIL    = "RULE FAILURE",
  ERROR   = "ERROR"
}

export interface EventStore {
  appendFactualEvent(ledger: Ledger, event: AppendEvent): Promise<AppendResult>
  appendAtomicEvent(ledger: Ledger, event: AppendEvent, selector: Selector): Promise<AppendResult>
}


export function init(logger: P.Logger, sql: Sql, registry: Registry): EventStore {
  return {
    appendFactualEvent: (l, e) => wrapWithCatch(logger, appendFact(logger, sql, registry, l, e)),
    appendAtomicEvent:  (l, e, s) => wrapWithCatch(logger, appendWithSelector(logger, sql, registry, l, e, s))
  }
}
