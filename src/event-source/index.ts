import P from "pino"
import { Sql } from "postgres"
import { Readable } from "stream"

import { wrapWithCatch } from "../db/sql.ts"
import { EventID, FilterSelector, Ledger, Selector } from "../types.ts"
import { allEvents, latestEventId, runFilter } from "./postgres-selector.ts"



export type SelectorResult = {
  // the point in the ledger that this selector read up to
  position:     EventID
  eventStream:  Readable
}


export interface EventSource {
  all(ledger: Ledger, select: Selector): Promise<SelectorResult>
  filter(ledger: Ledger, select: FilterSelector): Promise<SelectorResult>
  // used by HEAD for etag
  latestEventId(ledger: Ledger, selector: FilterSelector): Promise<EventID>
}


export function init(logger: P.Logger, sql: Sql): EventSource {
  return {
    all:            (l, s) => wrapWithCatch(logger, allEvents(logger, sql, l, s)),
    filter:         (l, s) => wrapWithCatch(logger, runFilter(logger, sql, l, s)),
    latestEventId: (l, s) => wrapWithCatch(logger, latestEventId(logger, sql, l, s))
  }
}
