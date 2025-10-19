import { FastifyReply, FastifyRequest } from "fastify"

import { EventSource, SelectorResult } from "../event-source/index.ts"
import { eventIdToString } from "../eventId-utils.ts"
import {
  decodeSelector,
  encodeUnknownSelector,
  isFilterSelector,
  isPlainSelector
} from "../selector-utils.ts"
import { EventID, Ledger, Selector } from "../types.ts"
import { getLedgerFromRequestCtx, stringifyPersistedEvent } from "./api-utils.ts"
import * as AppendEndpoint from "./append-endpoints.ts"
import { CONTENT_TYPES, HEADERS, L3_PROFILES, L3_RELS } from "./constants.ts"
import { addNdJsonExt, toNdJsonTransformer, trimNdJsonExt } from "./ndjson-utils.ts"
import { LEDGER_ID_PLACEHOLDER, PATHS as LEDGER_PATHS } from "./ledgers-endpoints.ts"
import { PATHS as SELECTOR_PATHS } from "./selectors-endpoints.ts"
import { SelectorHeaders } from "./type/selector-headers.ts"
import { SelectorPathParameter } from "./type/selector-path-param.ts"


export const SELECT_PLACEHOLDER = ":select"


/**
 * Parses the body into a valid Selector object. Will throw an http error if the selector is incorrect.
 */
export type SelectorLookupƒ = (body: any) => Selector


interface SelectorUris {
  startUri:   string
  currentUri: string
}


export function initHandleHeadSelector(source: EventSource) {
  return async (request:  FastifyRequest<{Params: SelectorPathParameter}>,
                reply:    FastifyReply) => {
    const ledger = getLedgerFromRequestCtx(request)
    const select = trimNdJsonExt(request.params.select)
    const selector = decodeSelector(select)
    const after = await source.latestEventId(ledger, selector)
    const currentSelector = toCurrentSelector(selector, after)
    const uris = createSelectorUris(ledger, selector, currentSelector)

    handleReplyEtagAndCacheControl(reply, eventIdToString(after))
    handleReplyBoilerplate(selector, uris, reply)

    return reply.send()
  }
}


export function initHandlePostSelectorLookup(source:    EventSource,
                                             selectorƒ: SelectorLookupƒ) {
  return async (request:  FastifyRequest<{Headers: SelectorHeaders}>,
                reply:    FastifyReply) => {
    const {headers, body} = request
    const {prefer} = headers
    const ledger = getLedgerFromRequestCtx(request)

    const selector = selectorƒ(body)
    const selectorUri = toSelectorEventUri(ledger, selector)

    if (prefer === "return=representation") {
      reply.header(HEADERS.CONTENT_LOCATION, selectorUri)
      return handleGetSelector(source, request, reply, selector)
    }

    if (prefer) {
      reply.header(HEADERS.PREFERENCE_APPLIED, prefer)
    }

    // 303 changes http method from POST to GET
    // https://en.wikipedia.org/wiki/HTTP_303
    return reply.redirect(selectorUri, 303)
  }
}


export function initHandleGetSelector(source: EventSource) {
  return async (request: FastifyRequest<{Headers: SelectorHeaders,
                                         Params:  SelectorPathParameter}>,
                reply: FastifyReply) => {
    const {
      headers: {
        [HEADERS.IF_NONE_MATCH]: ifNoneMatch
      },
      params: {
        select
      }
    } = request
    const ledger = getLedgerFromRequestCtx(request)
    const encodedSelector = trimNdJsonExt(select)
    const selector = decodeSelector(encodedSelector)

    if (ifNoneMatch) {
      const result = await fetchSelectorResult(source, request, selector)

      const {position, eventStream} = result
      const etag = toEtag(eventIdToString(position))

      if (etag === ifNoneMatch) {
        eventStream.destroy()
        const currentSelector = toCurrentSelector(selector, position)
        const uris = createSelectorUris(ledger, selector, currentSelector)
        handleReplyEtagAndCacheControl(reply, etag)
        handleReplyBoilerplate(selector, uris, reply)
        return reply
          .status(304)
          .send()
      }
      return replyWithSelectorResult(request, reply, selector, result)
    }

    return handleGetSelector(source, request, reply, selector)
  }
}


async function fetchSelectorResult(source:    EventSource,
                                   request:   FastifyRequest,
                                   selector:  Selector): Promise<SelectorResult> {
  const ledger = getLedgerFromRequestCtx(request)
  const sourceƒ = isFilterSelector(selector)
    ? source.filter
    : source.all

  return sourceƒ(ledger, selector)
}


async function handleGetSelector(source:    EventSource,
                                 request:   FastifyRequest,
                                 reply:     FastifyReply,
                                 selector:  Selector) {
  const result = await fetchSelectorResult(source, request, selector)

  return replyWithSelectorResult(request, reply, selector, result)
}


function replyWithSelectorResult(request:   FastifyRequest,
                                 reply:     FastifyReply,
                                 selector:  Selector,
                                 result:    SelectorResult) {
  const {
    headers: {
      prefer
     }
  } = request
  const {
    position,
    eventStream
  } = result

  const ledger = getLedgerFromRequestCtx(request)
  const currentSelector = toCurrentSelector(selector, position)
  const afterStr = eventIdToString(position)
  const uris = createSelectorUris(ledger, selector, currentSelector)

  // content-location means the selector is being returned in the POST response.
  // this is not a URI that needs etags.
  if (!reply.hasHeader(HEADERS.CONTENT_LOCATION)) {
    handleReplyEtagAndCacheControl(reply, toEtag(afterStr))
  }

  if (prefer) {
    reply.header(HEADERS.PREFERENCE_APPLIED, prefer)
  }

  handleReplyBoilerplate(selector, uris, reply)

  const ndJsonStream = toNdJsonTransformer(stringifyPersistedEvent, eventStream)
  return reply.send(ndJsonStream)
}


function toCurrentSelector(selector: Selector, after: EventID): Selector {
  return {
    ...selector,
    after
  }
}


function handleReplyEtagAndCacheControl(reply: FastifyReply, etag: string) {
  if (!etag.endsWith("\"")) {
    etag = toEtag(etag)
  }
  reply.headers({
    /*
      Selector content will change (etag) when new matching events append. Until that time, the result can be cached
      privately. These cache-control values manage this caching goal.

      private     Because requests use the Authorization header, caches will only store the representation in local
                  cache; this instructs them to be sure not to share.
      max-age=0   Revalidate on every call; similar to 'no-cache' except that one lets the browser decide
                  how long to reuse a representation before revalidating, sort of like "max-age=you-decide". Using
                  max-age=0 ensures the cache will revalidate immediately and not rely on client heuristics.
     */
    [HEADERS.CACHE_CONTROL]: "private,max-age=0",
    etag
  })
}


function toEtag(tagStr: string): string {
  return `"${tagStr}"`
}


function toSelectorEventUri(ledger:   Ledger,
                            selector: Selector): string {
  const isPlain = isPlainSelector(selector)
  const uriTemplate = isPlain
    ? LEDGER_PATHS.DOWNLOAD_LEDGER
    : SELECTOR_PATHS.FETCH_SELECTOR
  const selectorPart = encodeUnknownSelector(selector)
  let path = uriTemplate
    .replace(SELECT_PLACEHOLDER, selectorPart)
  if (isPlain) {
    path = path.replace(LEDGER_ID_PLACEHOLDER, ledger.id)
  }
  return addNdJsonExt(path)
}


function createSelectorUris(ledger:   Ledger,
                            selector: Selector,
                            current:  Selector): SelectorUris {
  const {
    after,
    ...start
  } = selector
  const startUri = toSelectorEventUri(ledger, start)
  const currentUri = toSelectorEventUri(ledger, current)
  return {
    startUri,
    currentUri
  }
}


function handleReplyBoilerplate(selector:     Selector,
                                uris:         SelectorUris,
                                reply:        FastifyReply) {
  const {
    startUri,
    currentUri
  } = uris
  const links = [
    `<${startUri}>; rel="start"`,
    `<${currentUri}>; rel="current"`
  ]
  if (isFilterSelector(selector)) {
    links.push(
      `<${SELECTOR_PATHS.SELECTORS_LOOKUP}>; title="Select Other Events"; rel="${L3_RELS.LOOKUP}"`,
      `<${AppendEndpoint.PATHS.APPEND}>; title="Atomically Append an Event With This Selector"; rel="${L3_RELS.FORM}"`
    )
  }

  reply
    .type(CONTENT_TYPES.ND_JSON)
    .headers({
      [HEADERS.LINK]:     links,
      [HEADERS.PROFILE]:  [
        L3_PROFILES.INFO,
        L3_PROFILES.ENTITY
      ]
    })
}
