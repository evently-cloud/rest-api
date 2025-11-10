import { FastifyInstance, FastifyReply, FastifyRequest } from "fastify"
import createHttpError from "http-errors"

import { Registry } from "../registry/index.ts"
import { formattedJSON, getLedgerFromRequestCtx, linkProfile } from "./api-utils.ts"
import { authHeadersSchema, Authorization, Permission } from "./auth/index.ts"
import { CONTENT_TYPES, HEADERS, L3_PATTERNS, L3_PROFILES, L3_RELS } from "./constants.ts"
import EntityPathParameterSchema from "../../schema/registry-entity-path-param.json" with { type: "json" }
import EventPathParameterSchema from "../../schema/registry-event-path-param.json" with { type: "json" }
import RegisterEventBodySchema from "../../schema/register-event-form.json" with { type: "json" }
import { RegistryEntityPathParameter } from "./type/registry-entity-path-param.ts"
import { RegistryEventPathParameter } from "./type/registry-event-path-param.ts"
import { HALObject, LinkObject } from "./type/hal.ts"
import { RegisterEntityEventForm } from "./type/register-event-form.ts"


export enum PATHS {
  REGISTRY              = "/registry",
  REGISTER_EVENT        = "/registry/register-event",
  REGISTRY_EVENTS       = "/registry/events",
  REGISTRY_EVENT        = "/registry/events/:event",
  REGISTRY_ENTITIES     = "/registry/entities",
  REGISTRY_ENTITY       = "/registry/entities/:entity"
}

const ENTITY_PLACEHOLDER = ":entity"
const EVENT_PLACEHOLDER = ":event"

const REGISTER_URI = "REGISTER_URI"


export function initRegistryEndpoints(server:   FastifyInstance,
                                      authz:    Authorization,
                                      registry: Registry): void {
  server.get(
    PATHS.REGISTRY,
    authz.readPublic,
    handleGetRegistry)

  server.get(
    PATHS.REGISTER_EVENT,
    authz.readPublic,
    handleGetNewEventTypeForm)

  server.post<{Body: RegisterEntityEventForm}>(
    PATHS.REGISTER_EVENT, {
      schema: {
        ...authHeadersSchema.schema,
        body: RegisterEventBodySchema
      },
      preHandler: authz.allowed(Permission.RegisterEvent)
    },
    initHandlePostNewEventTypeForm(registry)
  )

  server.get(
    PATHS.REGISTRY_ENTITIES, {
      ...authHeadersSchema,
      preHandler: authz.allowed(Permission.ReadRegistry)
    },
    initHandleGetEntities(registry)
  )

  server.get<{Params: RegistryEntityPathParameter}>(
    PATHS.REGISTRY_ENTITY, {
      schema: {
        ...authHeadersSchema.schema,
        params: EntityPathParameterSchema
      },
      preHandler: authz.allowed(Permission.ReadRegistry)
    },
    initGetEntity(registry)
  )

  server.get(
    PATHS.REGISTRY_EVENTS, {
      ...authHeadersSchema,
      preHandler: authz.allowed(Permission.ReadRegistry)
    },
    initHandleGetEvents(registry)
  )

  server.get<{Params: RegistryEventPathParameter}>(
    PATHS.REGISTRY_EVENT, {
      schema: {
        ...authHeadersSchema.schema,
        params: EventPathParameterSchema
      },
      preHandler: authz.allowed(Permission.ReadRegistry)
    },
    initHandleGetEvent(registry)
  )

  server.delete<{Params: RegistryEventPathParameter}>(
    PATHS.REGISTRY_EVENT, {
      schema: {
        ...authHeadersSchema.schema,
        params: EventPathParameterSchema
      },
      preHandler: authz.allowed(Permission.UnregisterEvent)
    },
    initHandleDeleteEvent(registry)
  )
}


const getRegistryResponse: HALObject = {
  title: "Event Registry API",
  description: "Register event types so they can be appended to the ledger. Evently asks you to do this so you can avoid common programming errors that have life-long impacts. Events are immutable, just like tattoos, which work best when your love's name is spelled correctly, and is actually the right person's name.",
  _links: {
    "register": {
      title:    "Register an Event",
      href:     PATHS.REGISTER_EVENT,
      profile:  linkProfile(L3_PROFILES.FORM)
    },
    "entities": {
      title:    "Registered Events scoped by Entity",
      href:     PATHS.REGISTRY_ENTITIES,
      profile:  linkProfile(L3_PATTERNS.LIST_RESOURCE)
    },
    "events": {
      title:    "All Registered Events",
      href:     PATHS.REGISTRY_EVENTS,
      profile:  linkProfile(L3_PATTERNS.LIST_RESOURCE)
    }
  }
}

function handleGetRegistry(request:  FastifyRequest,
                           reply:    FastifyReply) {
  reply
    .type(CONTENT_TYPES.HAL)
    .header(HEADERS.PROFILE, L3_PROFILES.HOME)
    .send(formattedJSON(getRegistryResponse))
}


const getNewEventTypeFormSchema = {
  ...RegisterEventBodySchema,
  description: RegisterEventBodySchema.description.replace(REGISTER_URI, PATHS.REGISTER_EVENT)
}

function handleGetNewEventTypeForm(request: FastifyRequest,
                                   reply:   FastifyReply) {
  reply
    .type(CONTENT_TYPES.JSON_SCHEMA)
    .headers({
        [HEADERS.PROFILE]: [
          L3_PROFILES.FORM,
          L3_PATTERNS.ADD_ENTRY_RESOURCE
        ],
        [HEADERS.LINK]: `<${PATHS.REGISTRY_EVENTS}>; rel="${L3_RELS.ADDS_TO_LIST}"; title="List of Registered Events"`
      })
    .send(formattedJSON(getNewEventTypeFormSchema))
}


function initHandlePostNewEventTypeForm(registry: Registry) {
  return async (request:  FastifyRequest<{Body: RegisterEntityEventForm}>,
                reply:    FastifyReply) => {
    const {event, entities} = request.body
    const ledger = getLedgerFromRequestCtx(request)

    await registry.registerEventType(ledger, event, entities)

    const eventUri = PATHS.REGISTRY_EVENT
      .replace(EVENT_PLACEHOLDER, encodeURIComponent(event))

    return reply
      .status(201)
      .header(HEADERS.LOCATION, eventUri)
      .send(`registered ${event}`)
  }
}


function initHandleGetEntities(registry: Registry) {
  return async (request:  FastifyRequest,
                reply:    FastifyReply) => {
    const ledger = getLedgerFromRequestCtx(request)
    reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, [
        L3_PROFILES.INFO,
        L3_PATTERNS.LIST_RESOURCE
      ])

    const entities = await registry.entities(ledger)

    const entityLinks: LinkObject[] = entities.map((type) => ({
      name:     type,
      href:     PATHS.REGISTRY_ENTITY.replace(ENTITY_PLACEHOLDER, encodeURIComponent(type)),
      profile:  linkProfile(L3_PROFILES.INFO)
    }))
    const response: HALObject = {
      description:  "Entities that have events registered for them.",
      _links: {
        [L3_RELS.LIST_ENTRY]: entityLinks,
        [L3_RELS.ADD_ENTRY]: {
          title:    "Register an Event",
          href:     PATHS.REGISTER_EVENT,
          profile:  linkProfile(L3_PROFILES.FORM)
        }
      }
    }
    return reply.send(formattedJSON(response))
  }
}


function initGetEntity(registry: Registry) {
  return async (request:  FastifyRequest<{Params: RegistryEntityPathParameter}>,
                reply:    FastifyReply) => {
    const {entity} = request.params
    const ledger = getLedgerFromRequestCtx(request)

    const events = await registry.eventsForEntity(ledger, entity)
    if (events.length === 0) {
      throw createHttpError.NotFound(`entity '${entity}' does not exist.`)
    }

    reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, [
        L3_PROFILES.INFO,
        L3_PATTERNS.LIST_RESOURCE,
        L3_PATTERNS.ENTRY_RESOURCE
      ])

    const eventLinks: LinkObject[] = events.map(({event}) => ({
      name:     event,
      href:     PATHS.REGISTRY_EVENT.replace(EVENT_PLACEHOLDER, encodeURIComponent(event)),
      profile:  linkProfile(L3_PROFILES.DATA)
    }))

    const response: HALObject = {
      name:   entity,
      _links: {
        [L3_RELS.LIST_ENTRY]: eventLinks,
        [L3_RELS.ADD_ENTRY]: {
          title:    "Register an Event",
          href:     PATHS.REGISTER_EVENT,
          profile:  linkProfile(L3_PROFILES.FORM)
        },
        [L3_RELS.LIST]: {
          href:     PATHS.REGISTRY_ENTITY.replace(ENTITY_PLACEHOLDER, encodeURIComponent(entity)),
          title:    "List of Registered Entities with Registered Events",
          profile:  linkProfile(L3_PATTERNS.LIST_RESOURCE)
        }
      }
    }

    return reply.send(formattedJSON(response))
  }
}


function initHandleGetEvents(registry: Registry) {
  return async (request:  FastifyRequest,
                reply:    FastifyReply) => {
    const ledger = getLedgerFromRequestCtx(request)

    const events = await registry.allEvents(ledger)

    reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, [
        L3_PROFILES.INFO,
        L3_PATTERNS.LIST_RESOURCE
      ])

    const eventLinks: LinkObject[] = events.map(({event}) => ({
      name:     event,
      href:     PATHS.REGISTRY_EVENT.replace(EVENT_PLACEHOLDER, encodeURIComponent(event)),
      profile:  linkProfile(L3_PROFILES.DATA)
    }))

    const response: HALObject = {
      _links: {
        [L3_RELS.LIST_ENTRY]: eventLinks,
        [L3_RELS.ADD_ENTRY]: {
          title:    "Register an Event",
          href:     PATHS.REGISTER_EVENT,
          profile:  linkProfile(L3_PROFILES.FORM)
        }
      }
    }

    return reply.send(formattedJSON(response))
  }
}


function initHandleGetEvent(registry: Registry) {
  return async (request:  FastifyRequest<{Params: RegistryEventPathParameter}>,
                reply:    FastifyReply) => {
    const {event} = request.params
    const ledger = getLedgerFromRequestCtx(request)

    const eventData = await registry.getEvent(ledger, event)

    if (!eventData) {
      throw createHttpError.NotFound(`Event '${event}' does not exist.`)
    }

    const response: HALObject = {
      ...eventData,
      _links: {
        "append-event": {
          href:     "/append",
          title:    "Append Events API",
          profile:  linkProfile(L3_PROFILES.FORM)
        },
        [L3_RELS.LIST]: {
          href:     PATHS.REGISTRY_EVENTS,
          title:    "List of Registered Events",
          profile:  linkProfile(L3_PATTERNS.LIST_RESOURCE)
        }
      }
    }

    return reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, [
          L3_PROFILES.DATA,
          L3_PATTERNS.ENTRY_RESOURCE
        ])
      .send(formattedJSON(response))
  }
}


function initHandleDeleteEvent(registry: Registry) {
  return async (request:  FastifyRequest<{Params: RegistryEventPathParameter}>,
                reply:    FastifyReply): Promise<void> => {
    const {event} = request.params
    const ledger = getLedgerFromRequestCtx(request)

    await registry.deleteEvent(ledger, event)

    return reply
      .status(204)
      .send()
  }
}
