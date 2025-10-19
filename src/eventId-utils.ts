import createHttpError from "http-errors"
import Long from "long"

import { bufferToLong, EventID, longToBuffer } from "./types.ts"


export function toEventIdBytes(eventId: EventID): Buffer {
  const {timestamp, checksum, ledgerId} = eventId
  return eventIdPartsToBuffer(timestamp, checksum, ledgerId)
}


export function toEventIdString(timestamp: Long, checksum: number, ledgerId: string): string {
  return eventIdPartsToBuffer(timestamp, checksum, ledgerId)
    .toString("hex")
}


export function eventIdToString(eventId: EventID): string {
  const {timestamp, checksum, ledgerId} = eventId
  return toEventIdString(timestamp, checksum, ledgerId)
}


export function maybeFromEventIdString(eventId?: string): EventID | undefined {
  if (eventId !== undefined) {
    if (eventId.length === 32) {
      const buf = Buffer.from(eventId, "hex")
      return fromEventIdBytes(buf)
    }
    throw createHttpError.BadRequest(`Invalid 'eventId' value: ${eventId}`)
  }
}

export function fromEventIdBytes(buf: Buffer): EventID {
  if (buf.byteLength === 16) {
    return {
      timestamp:  bufferToLong(buf, 0),
      checksum:   buf.readUInt32BE(8),
      ledgerId:   buf.readUInt32BE(12)
                    .toString(16)
                    .padStart(8, "0")
    }
  }
  throw createHttpError.BadRequest("Invalid 'eventId' buffer value")
}

function eventIdPartsToBuffer(timestamp: Long, checksum: number, ledgerId: string): Buffer {
  // event ID will always be 16 bytes, fully-written, so allocUnsafe is actually safe.
  const buf = Buffer.allocUnsafe(16)
  longToBuffer(timestamp, buf, 0)
  buf.writeUInt32BE(checksum, 8)
  buf.writeUInt32BE(Number.parseInt(ledgerId, 16), 12)
  return buf
}
