import Long from "long"


export const LEDGER = "📒"

export type ShutdownHookRegistrar = (name: string, shutdownFn: () => any) => void

export type Ledger = {
  readonly id:          string
  readonly name:        string
  readonly description: string
  readonly genesis:     EventID
}

export type Selector = {
  after?: EventID
  limit?: number
}

export type FilterSelector = Selector & {
  entities?: EntitiesRecord
  meta?: JsonpathFilter
  events?: DataFilter
}

export type EventID = {
  readonly timestamp: Long
  readonly checksum:  number
  readonly ledgerId:  string
}


export type EntitiesRecord = Record<string, string[]>

export type UnknownObject = Record<string, unknown>

export type JsonpathFilter = {
  query:  string
  vars?:  UnknownObject
}

export type DataFilter = Record<string, JsonpathFilter>


export function bufferToLong(buf: Buffer, offset: number): Long {
  // Long factory method is (lo, hi) but unsigned hex string is hi + lo
  return Long.fromBits(
    buf.readInt32BE(offset + 4),
    buf.readInt32BE(offset))
}

export function longToBuffer(long: Long, buf: Buffer, offset: number) {
  // unsigned hex string is hi + lo
  buf.writeInt32BE(long.getHighBits(), offset)
  buf.writeInt32BE(long.getLowBits(), offset + 4)
}
