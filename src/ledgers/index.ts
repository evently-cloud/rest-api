import P from "pino"
import { Sql } from "postgres"

import { CreateLedgerForm } from "../api/type/create-ledger-form.ts"
import { wrapWithCatch } from "../db/sql.ts"
import { allLedgers, createLedger, ledgerEventCount, removeLedger, resetEvents } from "./postgres-ledgers.ts"
import { cachedLedgerForId } from "./ledger-cache.ts"
import { Ledger } from "../types.ts"


export interface Ledgers {
  allLedgers(): Promise<Ledger[]>
  createLedger(ledger: CreateLedgerForm): Promise<Ledger | undefined>
  forLedgerId(ledgerId: string): Promise<Ledger | undefined>
  eventsCount(ledger: Ledger): Promise<number>
  resetLedger(ledger: Ledger, after?: string): Promise<void>
  removeLedger(ledger: Ledger): Promise<void>
}


export function init(logger: P.Logger, sql: Sql): Ledgers {
  return {
    allLedgers:   () => wrapWithCatch(logger, allLedgers(sql)),
    createLedger: (l) => wrapWithCatch(logger, createLedger(logger, sql, l)),
    forLedgerId:  (l) => wrapWithCatch(logger, cachedLedgerForId(logger, sql, l)),
    eventsCount:  (l) => wrapWithCatch(logger, ledgerEventCount(sql, l)),
    resetLedger:  (l, a) => wrapWithCatch(logger, resetEvents(logger, sql, l, a)),
    removeLedger: (l) => wrapWithCatch(logger, removeLedger(logger, sql, l))
  }
}
