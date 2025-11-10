import {  FastifyInstance, FastifyReply, FastifyRequest } from "fastify"
import createHttpError from "http-errors"

import { EventSource } from "../event-source/index.ts"
import { maybeFromEventIdString } from "../eventId-utils.ts"
import { Ledgers } from "../ledgers/index.ts"
import { Selector } from "../types.ts"
import { formattedJSON, linkProfile } from "./api-utils.ts"
import { authHeadersSchema, Authorization, Permission } from "./auth/index.ts"
import { CONTENT_TYPES, HEADERS, L3_PATTERNS, L3_PROFILES, L3_RELS, REQUEST_CTX } from "./constants.ts"
import CreateLedgerFormSchema from "../../schema/create-ledger-form.json" with { type: "json" }
import DownloadLedgerParametersSchema from "../../schema/download-ledger-path-params.json" with { type: "json" }
import LedgerParameterSchema from "../../schema/ledger-path-param.json" with { type: "json" }
import ResetLedgerFormSchema from "../../schema/reset-ledger-form.json" with { type: "json" }
import DownloadLedgerLookupSchema from "../../schema/download-ledger-lookup.json" with { type: "json" }
import SelectorHeadersSchema from "../../schema/selector-headers.json" with { type: "json" }
import {
  initHandleGetSelector,
  initHandleHeadSelector,
  initHandlePostSelectorLookup
} from "./selectors-shared.ts"
import { CreateLedgerForm } from "./type/create-ledger-form.ts"
import { DownloadLedgerLookup } from "./type/download-ledger-lookup.ts"
import { DownloadLedgerPathParameters } from "./type/download-ledger-path-params.ts"
import { HALObject, LinkObject } from "./type/hal.ts"
import { LedgerPathParameter } from "./type/ledger-path-param.ts"
import { ResetLedgerForm } from "./type/reset-ledger-form.ts"
import { SelectorHeaders } from "./type/selector-headers.ts"


export enum PATHS {
  LEDGERS         = "/ledgers",  // list of /ledgers/:id
  CREATE          = "/ledgers/create-ledger",
  LEDGER          = "/ledgers/:id",
  RESET           = "/ledgers/:id/reset",
  DOWNLOAD_LOOKUP = "/ledgers/:id/download",
  DOWNLOAD_LEDGER = "/ledgers/:id/download/:select"
}

export const LEDGER_ID_PLACEHOLDER = ":id"

const RESET_URI = "RESET_URI"
const DOWNLOAD_URI = "DOWNLOAD_URI"


export function initLedgersEndpoints(server:  FastifyInstance,
                                     authz:   Authorization,
                                     ledgers: Ledgers,
                                     source:  EventSource) {
  server.get(
    PATHS.LEDGERS, {
      ...authHeadersSchema,
      preHandler: authz.allowed(Permission.ReadLedgers)
    },
    initHandleGetLedgers(ledgers)
  )

  server.get(
    PATHS.CREATE, {
      ...authHeadersSchema,
      preHandler: authz.allowed(Permission.ReadLedgers)
    },
    handleGetCreateLedger
  )

  server.post<{Body: CreateLedgerForm}>(
    PATHS.CREATE, {
      schema: {
        ...authHeadersSchema.schema,
        body: CreateLedgerFormSchema
      },
      preHandler: authz.allowed(Permission.CreateLedger)
    },
    initHandlePostCreateLedger(ledgers)
  )

  server.get<{Params: LedgerPathParameter}>(
    PATHS.LEDGER, {
      schema: {
        ...authHeadersSchema,
        params: LedgerParameterSchema
      },
      preHandler: authz.allowed(Permission.ReadLedgers)
    },
    initHandleGetLedger(ledgers)
  )

  server.delete<{Params: LedgerPathParameter}>(
    PATHS.LEDGER, {
      schema: {
        ...authHeadersSchema,
        params: LedgerParameterSchema
      },
      preHandler: authz.allowed(Permission.DeleteLedger)
    },
    initHandleDeleteLedger(ledgers)
  )

  server.get<{Params: LedgerPathParameter}>(
    PATHS.RESET, {
      ...authHeadersSchema,
      preHandler: authz.allowed(Permission.ResetLedger),
    },
    initHandleGetResetLedger(ledgers)
  )

  server.post<{Params:  LedgerPathParameter,
               Body:    ResetLedgerForm}>(
    PATHS.RESET, {
      schema: {
        ...authHeadersSchema.schema,
        body: ResetLedgerFormSchema
      },
      preHandler: authz.allowed(Permission.ResetLedger)
    },
    initHandlePostResetLedger(ledgers)
  )

  server.get<{Params: LedgerPathParameter}>(
    PATHS.DOWNLOAD_LOOKUP, {
      ...authHeadersSchema,
      preHandler: authz.allowed(Permission.DownloadLedger),
    },
    initHandleGetDownloadLedgerLookup(ledgers)
  )

  server.post<{Headers: SelectorHeaders,
               Params:  LedgerPathParameter}>(
    PATHS.DOWNLOAD_LOOKUP, {
      schema: {
        headers:  SelectorHeadersSchema,
        params:   LedgerParameterSchema,
        body:     DownloadLedgerLookupSchema
      },
      preHandler: authz.allowed(Permission.DownloadLedger)
    },
    initHandleLedgerInPathSelector(ledgers, initHandlePostSelectorLookup(source, downloadSelector))
  )

  server.head<{Headers: SelectorHeaders,
               Params:  DownloadLedgerPathParameters}>(
    PATHS.DOWNLOAD_LEDGER, {
      schema: {
        headers:  SelectorHeadersSchema,
        params:   DownloadLedgerParametersSchema
      },
      preHandler: authz.allowed(Permission.DownloadLedger)
    },
    initHandleLedgerInPathSelector(ledgers, initHandleHeadSelector(source))
  )

  server.get<{Headers: SelectorHeaders,
              Params:  DownloadLedgerPathParameters}>(
    PATHS.DOWNLOAD_LEDGER, {
      schema: {
        headers:  SelectorHeadersSchema,
        params:   DownloadLedgerParametersSchema
      },
      preHandler: authz.allowed(Permission.DownloadLedger)
    },
    initHandleLedgerInPathSelector(ledgers, initHandleGetSelector(source))
  )
}


function initHandleGetLedgers(ledgers: Ledgers) {
  return async (request:  FastifyRequest,
                reply:    FastifyReply) => {
    reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, [
        L3_PROFILES.HOME,
        L3_PATTERNS.LIST_RESOURCE
      ])

    const ledgerList = await ledgers.allLedgers()
    const ledgerLinks: LinkObject[] = ledgerList.map(({id, name}) => ({
      name,
      href:     PATHS.LEDGER.replace(LEDGER_ID_PLACEHOLDER, id),
      profile:  L3_PROFILES.NEXUS
    }))
    const response: HALObject = {
      description: "List of Ledgers",
      _links: {
        [L3_RELS.LIST_ENTRY]: ledgerLinks,
        [L3_RELS.ADD_ENTRY]: {
          title:    "Create a new Ledger",
          href:     PATHS.CREATE,
          profile:  linkProfile(L3_PROFILES.FORM)
        }
      }
    }
    return reply.send(formattedJSON(response))
  }
}


const createLedgerSchema = formattedJSON({
  ...CreateLedgerFormSchema,
  description: CreateLedgerFormSchema.description.replace(RESET_URI, PATHS.CREATE)
})

function handleGetCreateLedger(request:  FastifyRequest,
                               reply:    FastifyReply) {
  return reply
      .type(CONTENT_TYPES.JSON_SCHEMA)
      .header(HEADERS.PROFILE, L3_PROFILES.FORM)
      .send(createLedgerSchema)
}


function initHandlePostCreateLedger(ledgers: Ledgers) {
  return async (request:  FastifyRequest<{Body: CreateLedgerForm}>,
                reply:    FastifyReply) => {
    const ledger = await ledgers.createLedger(request.body)
    if (ledger) {
      const {id, name} = ledger
      const ledgerUri = PATHS.LEDGER.replace(LEDGER_ID_PLACEHOLDER, id)

      return reply
        .status(201)
        .header(HEADERS.LOCATION, ledgerUri)
        .send(`created Ledger '${name}' with id '${id}'`)
    }
    throw createHttpError.Forbidden(`Cannot create ledger '${request.body.name}'.`)
  }
}


function initHandleGetLedger(ledgers: Ledgers) {
  return async (request:  FastifyRequest<{Params: LedgerPathParameter}>,
                reply:    FastifyReply) => {
    const ledger = await ledgerFromPath(ledgers, request)
    const count = await ledgers.eventsCount(ledger)
    const {id, name, description} = ledger
    const getLedgersResponse: HALObject = {
      id,
      name,
      description,
      count,
      _links: {
        reset: {
          title:    "Reset the Ledger's Events",
          href:     PATHS.RESET.replace(LEDGER_ID_PLACEHOLDER, id),
          profile:  linkProfile(L3_PROFILES.FORM)
        },
        download: {
          title:    "Download the Ledger's Events",
          href:     PATHS.DOWNLOAD_LOOKUP.replace(LEDGER_ID_PLACEHOLDER, id),
          profile:  linkProfile(L3_PROFILES.LOOKUP)
        }
      }
    }

    return reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, L3_PROFILES.HOME)
      .send(formattedJSON(getLedgersResponse))
  }
}


function initHandleDeleteLedger(ledgers: Ledgers) {
  return async (request:  FastifyRequest<{Params: LedgerPathParameter}>,
                reply:    FastifyReply) => {
    const ledger = await ledgerFromPath(ledgers, request)
    await ledgers.removeLedger(ledger)

    return reply
      .send(`Removed ledger '${ledger.name}' with id '${ledger.id}'`)
  }
}


function initHandleGetResetLedger(ledgers: Ledgers) {
  return async (request:  FastifyRequest<{Params: LedgerPathParameter}>,
                reply:    FastifyReply) => {
    const ledger = await ledgerFromPath(ledgers, request)

    const resetLedgerSchema = {
      ...ResetLedgerFormSchema,
      description: ResetLedgerFormSchema.description.replace(RESET_URI, PATHS.RESET.replace(LEDGER_ID_PLACEHOLDER, ledger.id))
    }

    return reply
      .type(CONTENT_TYPES.JSON_SCHEMA)
      .header(HEADERS.PROFILE, L3_PROFILES.FORM)
      .send(formattedJSON(resetLedgerSchema))
  }
}


function initHandlePostResetLedger(ledgers: Ledgers) {
  return async (request:  FastifyRequest<{Params: LedgerPathParameter,
                                          Body:   ResetLedgerForm}>,
                reply:    FastifyReply) => {
    const ledger = await ledgerFromPath(ledgers, request)
    const {after} = request.body
    await ledgers.resetLedger(ledger, after)

    return reply
      .header(HEADERS.LOCATION, PATHS.LEDGER.replace(LEDGER_ID_PLACEHOLDER, ledger.id))
      .status(204)
      .send()
  }
}


function initHandleGetDownloadLedgerLookup(ledgers: Ledgers) {
  return async (request:  FastifyRequest<{Params: LedgerPathParameter}>,
                reply:    FastifyReply)=> {
    const ledger = await ledgerFromPath(ledgers, request)
    const description = DownloadLedgerLookupSchema.description
      .replace(DOWNLOAD_URI, PATHS.DOWNLOAD_LOOKUP.replace(LEDGER_ID_PLACEHOLDER, ledger.id))
    const downloadLedgerLookupSchema = {
      ...DownloadLedgerLookupSchema,
      description
    }

    return reply
      .type(CONTENT_TYPES.JSON_SCHEMA)
      .header(HEADERS.PROFILE, [
          L3_PROFILES.LOOKUP,
          L3_PROFILES.REPRESENTATION
        ])
      .send(formattedJSON(downloadLedgerLookupSchema))
  }
}


// path params are different for the selectorHandlers
type SelectorHandler = (request: FastifyRequest<{Headers: SelectorHeaders,
                                                 Params:  any}>, // selectorHandler doesn't always require a selector param
                        reply: FastifyReply) => Promise<FastifyReply>

function initHandleLedgerInPathSelector(ledgers:          Ledgers,
                                        selectorHandler:  SelectorHandler) {
  return async (request:  FastifyRequest<{Headers:  SelectorHeaders,
                                          Params:   LedgerPathParameter}>,
                reply:    FastifyReply) => {
    const ledger = await ledgerFromPath(ledgers, request)
    request.requestContext.set(REQUEST_CTX.LEDGER, ledger)
    return selectorHandler(request, reply)
  }
}

function downloadSelector(body: DownloadLedgerLookup): Selector {
  const {
    after: afterStr,
    limit
  } = body
  const after = afterStr && afterStr.length > 0
    ? maybeFromEventIdString(afterStr)
    : undefined

  return {
    after,
    limit
  }
}


async function ledgerFromPath(ledgers: Ledgers,
                              request: FastifyRequest<{Params: LedgerPathParameter}>) {
  const {
    params: {
      id
    }
  } = request

  const ledger = await ledgers.forLedgerId(id)
  if (!ledger) {
    throw createHttpError.NotFound(`Ledger '${id}' not found`)
  }

  return ledger
}
