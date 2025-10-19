import { FastifyInstance, FastifyReply, FastifyRequest } from "fastify"
import createHttpError from "http-errors"

import { EventSource } from "../event-source/index.ts"
import { maybeFromEventIdString } from "../eventId-utils.ts"
import { Registry } from "../registry/index.ts"
import { sortObject } from "../selector-utils.ts"
import { FilterSelector } from "../types.ts"
import { formattedJSON, getLedgerFromRequestCtx } from "./api-utils.ts"
import { Authorization, Permission } from "./auth/index.ts"
import { CONTENT_TYPES, HEADERS, L3_PROFILES } from "./constants.ts"
import SelectorFilterLookupSchema from "../../schema/selector-filter-lookup.json" with { type: "json" }
import SelectorHeadersSchema from "../../schema/selector-headers.json" with { type: "json" }
import SelectorParameterSchema from "../../schema/selector-path-param.json" with { type: "json" }
import {
  initHandleGetSelector,
  initHandleHeadSelector,
  initHandlePostSelectorLookup
} from "./selectors-shared.ts"
import { SelectorFilterLookup } from "./type/selector-filter-lookup.ts"
import { SelectorHeaders } from "./type/selector-headers.ts"
import { SelectorPathParameter } from "./type/selector-path-param.ts"


export enum PATHS {
  SELECTORS_LOOKUP = "/selectors",
  FETCH_SELECTOR   = "/selectors/:select"
}

const FILTER_URI = "FILTER_URI"
const STRICT = "strict "


export function initSelectorsEndpoints(server:    FastifyInstance,
                                       authz:     Authorization,
                                       registry:  Registry,
                                       source:    EventSource): void {
  server.get(
    PATHS.SELECTORS_LOOKUP, {
      preHandler: authz.allowed(Permission.ReplayEvents),
    },
    initHandleGetSelectorLookup(registry)
  )

  server.post<{Headers: SelectorHeaders}>(
    PATHS.SELECTORS_LOOKUP, {
      schema: {
        headers:  SelectorHeadersSchema,
        body:     SelectorFilterLookupSchema
      },
      preHandler: authz.allowed(Permission.ReplayEvents)
    },
    initHandlePostSelectorLookup(source, lookupToSelector))

  server.head<{Params: SelectorPathParameter}>(
    PATHS.FETCH_SELECTOR, {
      schema: {
        headers:  SelectorHeadersSchema,
        params:   SelectorParameterSchema
      },
      preHandler: authz.allowed(Permission.ReplayEvents)
    },
    initHandleHeadSelector(source)
  )

  server.get<{Headers: SelectorHeaders,
              Params:  SelectorPathParameter}>(
      PATHS.FETCH_SELECTOR, {
      schema: {
        headers: SelectorHeadersSchema,
        params:  SelectorParameterSchema
      },
      preHandler: authz.allowed(Permission.ReplayEvents)
    },
    initHandleGetSelector(source)
  )
}


function initHandleGetSelectorLookup(registry: Registry) {
  return async (request:  FastifyRequest,
                reply:    FastifyReply) => {
    const ledger = getLedgerFromRequestCtx(request)
    const eventTypes = await registry.allEvents(ledger)
    const entityNames = new Set()
    const eventSchemas = eventTypes
      .reduce((acc, {entities, event}) => {
        entities.forEach((entity) => entityNames.add(entity))
        acc[event] = {
          $ref: "#/definitions/jsonpathQuery"
        }
        return acc
      }, {} as Record<string, object>)

    const schema = {
      ...SelectorFilterLookupSchema,
      description: SelectorFilterLookupSchema.description.replace(FILTER_URI, PATHS.SELECTORS_LOOKUP),
      // add in the event schemas
      events: {
        ...SelectorFilterLookupSchema.definitions.dataQuery,
        // additionalProperties must be here instead of JSON schema, otherwise fastify erases `data` from POST
        additionalProperties: false,
        properties:           eventSchemas
      },
      entities: {
        ...SelectorFilterLookupSchema.definitions.entities,
        propertyNames: {
          enum: [...entityNames]
        }
      }
    }

    return reply
      .type(CONTENT_TYPES.JSON_SCHEMA)
      .header(HEADERS.PROFILE, [
        L3_PROFILES.LOOKUP,
        L3_PROFILES.REPRESENTATION
      ])
      .send(formattedJSON(schema))
  }
}


function lookupToSelector(lookup: SelectorFilterLookup): FilterSelector {
  const {
    entities,
    meta,
    events,
    after: afterStr,
    limit
  } = lookup

  if (meta?.query.trimStart().startsWith(STRICT)) {
    throw createHttpError.UnprocessableEntity("'strict' keyword unsupported in Evently's SQL JSONPath meta filter")
  }
  const after = maybeFromEventIdString(afterStr)

  const errors: string[] = []

  let data
  if (events) {
    for (const event of Object.keys(events)) {
      const { query } = events[event]
      if (query.trimStart().startsWith(STRICT)) {
        errors.push(`events.${event}.query 'strict' keyword unsupported in Evently's SQL JSONPath data filter`)
      }
    }
    data = sortObject(events)
  }

  if (errors.length) {
    const fullMsg = errors.join("\n")
    throw createHttpError.UnprocessableEntity(fullMsg)
  }

  return {
    entities,
    meta,
    events: data,
    after,
    limit
  }
}
