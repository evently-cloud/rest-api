import {
  FastifyInstance,
  FastifyReply,
  FastifyRequest,
} from "fastify"

import { EventSource } from "../event-source/index.ts"
import { EventStore } from "../event-store/index.ts"
import { Ledgers } from "../ledgers/index.ts"
import { Channels } from "../notify/notify.ts"
import { Registry } from "../registry/index.ts"
import { formattedJSON, linkProfile } from "./api-utils.ts"
import {  Authorization } from "./auth/index.ts"
import { CONTENT_TYPES, HEADERS, L3_PROFILES } from "./constants.ts"
import { initAppendEndpoints } from "./append-endpoints.ts"
import { initLedgersEndpoints } from "./ledgers-endpoints.ts"
import { initNotifyEndpoints } from "./notify-endpoints.ts"
import { initRegistryEndpoints } from "./registry-endpoints.ts"
import { initSelectorsEndpoints } from "./selectors-endpoints.ts"
import { HALObject } from "./type/hal.ts"

enum PATHS {
  ROOT = "/"
}


export function initRestEndpoints(server:       FastifyInstance,
                                  authz:        Authorization,
                                  ledgers:      Ledgers,
                                  registry:     Registry,
                                  source:       EventSource,
                                  store:        EventStore,
                                  channels:     Channels): void {
  server.get(
    PATHS.ROOT,
    authz.readPublic,
    handleGetRoot)

  initLedgersEndpoints(server, authz, ledgers, source)
  initRegistryEndpoints(server, authz, registry)
  initSelectorsEndpoints(server, authz, registry, source)
  initAppendEndpoints(server, authz, store)
  initNotifyEndpoints(server, authz, channels)
}


const getRootResponse: HALObject = {
  title: "Evently Client API",
  description: "Event Sourcing service that stores and replays business events. Event types have to be registered before use. Append events as facts, or atomically to safely modify entity state. Use selectors to fetch event sets for your read models and projections.",
  _links: {
    registry: {
      title:    "Register Entity Events for the Ledger",
      href:     "/registry",
      profile:  linkProfile(L3_PROFILES.HOME)
    },
    append: {
      title:  "Append Events to the Ledger",
      href:   "/append",
      profile:  linkProfile(L3_PROFILES.FORM)
    },
    selectors: {
      title:  "Selects Events From the Ledger",
      href:   "/selectors",
      profile:  linkProfile(L3_PROFILES.LOOKUP)
    },
    ledgers: {
      title:  "Manage the Ledger",
      href:   "/ledgers",
      profile:  linkProfile(L3_PROFILES.HOME)
    },
    notifications: {
      title:  "Event notifications from Selectors",
      href:   "/notify",
      profile:  linkProfile(L3_PROFILES.HOME)
    }
  }
}

const formattedRoot = formattedJSON(getRootResponse)


function handleGetRoot(request: FastifyRequest,
                       reply:   FastifyReply) {
  reply
    .type(CONTENT_TYPES.HAL)
    .header(HEADERS.PROFILE, L3_PROFILES.HOME)
    .send(formattedRoot)
}
