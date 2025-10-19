import P from "pino"
import { Sql } from "postgres"
import { LruMap } from "toad-cache"

import { ledgerForId } from "./postgres-ledgers.ts"
import { Ledger } from "../types.ts"


const cache = new LruMap<Ledger>(1_000, 5_000)


export async function cachedLedgerForId(logger:   P.Logger,
                                        sql:      Sql,
                                        ledgerId: string): Promise<Ledger | undefined> {
  let ledger = cache.get(ledgerId)
  if (!ledger) {
    ledger = await ledgerForId(logger, sql, ledgerId)
    if (ledger) {
      cache.set(ledgerId, ledger)
    }
  }
  return ledger
}

export function clearCacheForLedger(ledgerId: string) {
  cache.delete(ledgerId)
}
