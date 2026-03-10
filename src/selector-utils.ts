import createHttpError from "http-errors"
import { constrain, Constraint, optional, Spec, Type, verify } from "specified"

import { fromURIPart, nonEmptyStringArraySpec, toURIPart } from "./api/api-utils.ts"
import { fromEventIdBytes, toEventIdBytes } from "./eventId-utils.ts"
import { DataFilter, EntitiesRecord, FilterSelector, JsonpathFilter, Selector, UnknownObject } from "./types.ts"


export function decodeSelector(encodedSelector: string): Selector {
  const compressed = <any>fromURIPart(encodedSelector)
  if (compressed.e || compressed.m || compressed.d) {
    return compressedFilterToSelector(compressed)
  }
  return compressedSelectorToSelector(compressed)
}


export function isPlainSelector(selector: any): selector is Selector {
  return !isFilterSelector(selector)
}


export function isFilterSelector(selector: any): selector is FilterSelector {
  return selector.entities !== undefined
    || selector.meta !== undefined
    || selector.events !== undefined
}


export function encodeUnknownSelector(selector: Selector): string {
  return isFilterSelector(selector)
    ? encodeFilterSelector(selector)
    : encodeSelector(selector)
}


export function encodeFilterSelector(selector: FilterSelector): string {
  const {entities, meta, events, after, limit: l} = selector
  const e = entities && sortObject(entities)
  const m = meta && jsonpathToCompressedJsonpath(meta)
  const d = events && eventsToCompressedData(events)
  const a = after && toEventIdBytes(after)
  const compressed = prepareCompressed({e, m, d, a, l})
  return toURIPart(compressed)
}


function encodeSelector(selector: Selector): string {
  const {after, limit: l} = selector
  const a = after && toEventIdBytes(after)
  const compressed = prepareCompressed({a, l})
  return toURIPart(compressed)
}


type CompressedSelectorForm = {
  a?: Buffer  // after Event ID in bytes
  l?: number  // limit
}

type CompressedJsonpath  = {
  q:  string,
  v?: UnknownObject
}

type CompressedData = Record<string, CompressedJsonpath>

type CompressedFilterForm = CompressedSelectorForm & {
  e:  EntitiesRecord         // entities and keys
  m?: CompressedJsonpath  // meta filter
  d?: CompressedData      // data filter
}


const afterSpec = optional(Type.instance(Buffer))
const limitSpec = optional(constrain(Type.number, [
  Constraint.number.integer,
  Constraint.number.above(0)
]))

const compressedSelectorSpec = Type.object({
  a: afterSpec,
  l: limitSpec
})

const compressedJsonpathSpec = Type.object({
  q: Type.string,
  v: optional(Type.map(Type.string, Type.unknown))
})

const compressedFilterSpec = Type.object({
  e: optional(Type.map(Type.string, nonEmptyStringArraySpec)),
  m: optional(compressedJsonpathSpec),
  d: optional(Type.map(Type.string, compressedJsonpathSpec)),
  a: afterSpec,
  l: limitSpec
})


function jsonpathToCompressedJsonpath(jsonpath: JsonpathFilter): CompressedJsonpath {
  const {query: q, vars} = jsonpath
  const v = vars && sortObject(vars)
  const compressed = prepareCompressed({q, v}) as unknown as CompressedJsonpath
  return Object.freeze(compressed)
}

function compressedJsonpathToJsonpath(compressed: CompressedJsonpath): JsonpathFilter {
  const {q: query, v: vars} = compressed
  return {
    query,
    vars
  }
}


function compressedSelectorToSelector(compressed: CompressedSelectorForm): Selector {
  verifyCompressed(compressedSelectorSpec, compressed)
  const {a, l: limit} = compressed
  const after = a && fromEventIdBytes(a)
  return {
    after,
    limit
  }
}


function compressedDataToEvents(compressed: CompressedData): DataFilter {
  const events: DataFilter = {}
  for (const [event, query] of Object.entries(compressed)) {
    events[event] = compressedJsonpathToJsonpath(query)
  }
  return events
}


function eventsToCompressedData(events: DataFilter): CompressedData {
  const compressed: CompressedData = {}
  for (const [event, query] of Object.entries(events)) {
    compressed[event] = jsonpathToCompressedJsonpath(query)
  }
  return sortObject(compressed)
}


function compressedFilterToSelector(compressed: CompressedFilterForm): FilterSelector {
  verifyCompressed(compressedFilterSpec, compressed)
  const {e, m, d, a, l: limit} = compressed
  const entities = e
  const meta = m && compressedJsonpathToJsonpath(m)
  const events = d && compressedDataToEvents(d)
  const after = a && fromEventIdBytes(a)
  return {
    entities,
    meta,
    events,
    after,
    limit
   }
}


function prepareCompressed(object: UnknownObject): UnknownObject {
  const compressed: UnknownObject = {}
  for (const key of Object.keys(object)) {
    if (includeInCompressed(object[key])) {
      compressed[key] = object[key]
    }
  }
  return compressed
}

function includeInCompressed(value: any): boolean {
  // Number check is for limit, which must be a positive integer
  return Number.isInteger(value)
    ? value > 0
    : isNotEmpty(value)
}


function verifyCompressed(spec: Spec<any>, compressed: object) {
  const verified = verify(spec, compressed)
  if (verified.err) {
    throw createHttpError.BadRequest(verified.err.message)
  }
}



// todo rename this so it's understood to also freeze and ignore frozen state
export function sortObject<T extends UnknownObject>(inObject: T): T {
  if (Object.isFrozen(inObject)) {
    return inObject
  }
  const newObject: UnknownObject = {}
  for (const key of Object.keys(inObject).sort()) {
    newObject[key] = sortUnknown(inObject[key])
  }

  return Object.freeze(newObject) as T
}

export function sortUnknown(value: unknown): unknown {
  if (Object.isFrozen(value)) {
    return value
  }

  if (value !== null && typeof value === "object") {
    value = Array.isArray(value)
      ? value.map(sortUnknown)
      : sortObject(value as UnknownObject)
  }
  return Object.freeze(value)
}


export function isNotEmpty<T>(value: T | null | undefined): value is T {
  if (value === null || value === undefined) {
    return false
  }
  if (typeof value === "object") {
    const length = Array.isArray(value)
      ? value.length
      : Object.keys(value).length
    return length > 0
  }
  return true
}


function isObject(value: unknown): value is object {
  return value != null && typeof value === "object"
}

export function areEqual(v1: unknown, v2: unknown): boolean {
  return isObject(v1) && isObject(v2)
    ? objectsEqual(v1, v2)
    : v1 === v2
}

function objectsEqual(o1: Record<string, any> | any[], o2: Record<string, any> | any[]): boolean {
  const isArr = Array.isArray(o1)
  if (isArr !== Array.isArray(o2)) {
    return false
  }
  if (isArr) {
    return arraysEqual(o1, o2 as any[])
  }
  const keys1 = Object.keys(o1)
  if (keys1.length !== Object.keys(o2).length) {
    return false
  }
  for (const key of keys1) {
    // @ts-ignore o1 and o2 are objects
    if (!areEqual(o1[key], o2[key])) {
      return false
    }
  }
  return true
}

function arraysEqual(a1: any[], a2: any[]) {
  if (a1.length !== a2.length) {
    return false
  }
  for (let i = 0; i < a1.length; i++) {
    if (!areEqual(a1[i], a2[i])) {
      return false
    }
  }
  return true
}
