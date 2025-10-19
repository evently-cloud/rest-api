import { FastifyInstance, FastifyReply, FastifyRequest } from "fastify"
import createHttpError from "http-errors"

import { AppendResult, EventStore, Result } from "../event-store/index.ts"
import { maybeFromEventIdString } from "../eventId-utils.ts"
import { encodeUnknownSelector, isPlainSelector } from "../selector-utils.ts"
import { Selector } from "../types.ts"
import { formattedJSON, getLedgerFromRequestCtx } from "./api-utils.ts"
import { authHeadersSchema, Authorization, Permission } from "./auth/index.ts"
import { CONTENT_TYPES, HEADERS, L3_PROFILES } from "./constants.ts"
import * as LEDGER from "./ledgers-endpoints.ts"
import { addNdJsonExt } from "./ndjson-utils.ts"
import * as REGISTRY from "./registry-endpoints.ts"
import AppendEventFormSchema from "../../schema/append-event-form.json" with { type: "json" }
import * as SELECTOR from "./selectors-endpoints.ts"
import { SELECT_PLACEHOLDER } from "./selectors-shared.ts"
import { AppendEventForm } from "./type/append-event-form.ts"


export enum PATHS {
  APPEND = "/append"
}


const APPEND_URI = "APPEND_URI"


export function initAppendEndpoints(server: FastifyInstance,
                                    authz:  Authorization,
                                    store:  EventStore): void {
  server.get(
    PATHS.APPEND,
    authz.readPublic,
    initHandleGetSchema(AppendEventFormSchema, PATHS.APPEND))

  server.post<{Body: AppendEventForm}>(
    PATHS.APPEND, {
      schema: {
        ...authHeadersSchema.schema,
        body: AppendEventFormSchema
      },
      preHandler: authz.allowed(Permission.AppendEvent)
    },
    initHandlePostAppend(store)
  )
}


function initHandleGetSchema(schema:  object,
                             uri:     string) {
  const getSchemaResponse = {
    ...schema,
    // @ts-ignore schema.description not in an interface
    description: schema.description.replace(APPEND_URI, uri)
  }
  const formattedSchemaResponse = formattedJSON(getSchemaResponse)

  return (request:  FastifyRequest,
          reply:    FastifyReply) => {
    reply
      .type(CONTENT_TYPES.JSON_SCHEMA)
      .header(HEADERS.PROFILE, L3_PROFILES.FORM)
      .send(formattedSchemaResponse)
  }
}


function initHandlePostAppend(store: EventStore) {
  return async (request:  FastifyRequest<{Body: AppendEventForm}>,
                reply:    FastifyReply) => {
    const ledger = getLedgerFromRequestCtx(request)
    const appendForm = request.body
    const {
      entities,
      selector: selectorIn
    } = appendForm

    let selector, result
    if (selectorIn) {
      const after = maybeFromEventIdString(selectorIn.after)
      selector = {
        ...selectorIn,
        after
      }
      if (isPlainSelector(selector)) {
        throw createHttpError.BadRequest("Cannot use a download selector to append. Please send a valid selector query")
      }
      result = await store.appendAtomicEvent(ledger, appendForm, selector)
    } else {
      result = await store.appendFactualEvent(ledger, appendForm)
      selector = {
        entities
      } as Selector
    }

    return replyForResult(selector, result, reply)
  }
}


function replyForResult(selector: Selector,
                        result:   AppendResult,
                        reply:    FastifyReply) {
  const {
    status,
    ok: newEvent,
    message = ""
  } = result

  let responseBody

  switch (status) {
    case Result.SUCCESS:
      const after = maybeFromEventIdString(newEvent?.eventId)
      const newSelector = {
        ...selector,
        after
      }
      const newSelectorUri = toSelectorUri(newSelector)
      reply
        .status(201)
        .type(CONTENT_TYPES.JSON)
        .header(HEADERS.LOCATION, newSelectorUri)
      responseBody = newEvent
      break
    case Result.RACE:
      let raceMsg = message
      if (message.includes("/SELECTOR")) {
        const currentSelectorUri = toSelectorUri(selector)
        raceMsg = message.replace("/SELECTOR", currentSelectorUri)
        responseBody = formattedJSON({
          message: raceMsg,
          current: currentSelectorUri
        })
        reply.type(CONTENT_TYPES.JSON)
      }
      reply.status(409)
      break
    case Result.FAIL:
      let failMsg = message
      if (message.includes("/RESET")) {
        const ledgerId = getLedgerFromRequestCtx(reply.request)
        failMsg = message
          .replace(LEDGER.LEDGER_ID_PLACEHOLDER, ledgerId.id)
          .replace("/RESET", LEDGER.PATHS.RESET)
      } else if (message.includes("/REGISTER")) {
        failMsg = message.replace("/REGISTER", REGISTRY.PATHS.REGISTER_EVENT)
    }
      reply.log.warn(`append failure: ${failMsg}`)
      responseBody = message
      reply.status(403)
      break
    default:
      reply.log.warn(`append error: ${message}`)
      responseBody = message
      reply.status(400)
  }

  return reply.send(responseBody)
}

function toSelectorUri(selector: Selector): string {
  const selectPart = encodeUnknownSelector(selector)
  const uri = SELECTOR.PATHS.FETCH_SELECTOR.replace(SELECT_PLACEHOLDER, selectPart)
  return addNdJsonExt(uri)
}
