import fastify, { FastifyInstance } from "fastify"
import fastifyCompress from "@fastify/compress"
import fastifyCors from "@fastify/cors"
import { FastifyAllowPlugin } from "fastify-allow"
import { FastifyHttpsAlwaysPlugin } from "fastify-https-always"
import { FastifySSEPlugin } from "fastify-sse-v2"
import requestContext from "@fastify/request-context"
import P from "pino"
import zlib from "node:zlib"

import { EventSource } from "../event-source/index.ts"
import { EventStore } from "../event-store/index.ts"
import { Ledgers } from "../ledgers/index.ts"
import { Channels } from "../notify/notify.ts"
import { Registry } from "../registry/index.ts"
import { Ledger, ShutdownHookRegistrar } from "../types.ts"
import { initAuth, Role } from "./auth/index.ts"
import { HEADERS } from "./constants.ts"
import { initRestEndpoints } from "./rest-endpoints.ts"
import AuthorizationHeaderSchema from "../../schema/authorization-header.json" with { type: "json" }


// Add ledger access to request context.
declare module "@fastify/request-context" {
  interface RequestContextData {
    ledger: Ledger,
    roles:  Role[]
  }
}


export function launch(shutdown:  ShutdownHookRegistrar,
                       logger:    P.Logger,
                       ledgers:   Ledgers,
                       registry:  Registry,
                       source:    EventSource,
                       store:     EventStore,
                       channels:  Channels): Promise<string> {
  const port = Number.parseInt(process.env.PORT ?? "4802")

  const server = fastify({
    loggerInstance:         logger,
    disableRequestLogging:  true,
    exposeHeadRoutes:       true,
    trustProxy:             true,
    routerOptions: {
      caseSensitive:          false,
      ignoreTrailingSlash:    true,
      maxParamLength:         2048  // default is 100
    }
  }) as unknown as FastifyInstance  // fastify type def is horrendous

  server.addSchema(AuthorizationHeaderSchema)

  server.register(fastifyCors, {
    maxAge:           -1,  // Using -1 for browser testing; 1 day = 86_400 (firefox), 10 minutes = 600 (chrome)
    methods:          ["HEAD", "GET", "POST", "DELETE"],
    strictPreflight:  false, // allows a plain OPTIONS call to return the CORS headers
/*
  Safe-listed by CORS:
    * Accept
    * Accept-Language
    * Content-Language
    * Content-Type [application/x-www-form-urlencoded, multipart/form-data, text/plain]
 */
    allowedHeaders: [
      HEADERS.AUTHORIZATION,
      HEADERS.CONTENT_TYPE,  // added to allow any content type (like application/json), not just the safe list
      HEADERS.PREFER
    ],
/*
  Safe-listed by CORS:
    * Cache-Control
    * Content-Language
    * Content-Length,
    * Content-Type,
    * Expires,
    * Last-Modified
    * Pragma
 */
    exposedHeaders: [
      HEADERS.CONTENT_LOCATION,
      HEADERS.LAST_EVENT_ID,
      HEADERS.LINK,
      HEADERS.LOCATION,
      HEADERS.PREFERENCE_APPLIED,
      HEADERS.PROFILE,
      HEADERS.WWW_AUTHENTICATE
    ]
  })

  const compressOptions = {
    customTypes:    /application\/x-ndjson/,
    brotliOptions:  {
      params: {
        // utf8
        [zlib.constants.BROTLI_PARAM_MODE]:     zlib.constants.BROTLI_MODE_TEXT,
        // best for dynamic data; good balance between speed and size
        [zlib.constants.BROTLI_PARAM_QUALITY]:  4
      }
    }
  }

  server.register(fastifyCompress, compressOptions)

  server.register(FastifySSEPlugin)

  server.register(FastifyAllowPlugin)

  server.register(FastifyHttpsAlwaysPlugin)

  server.register(requestContext)

  const authz = initAuth(server, ledgers)

  server.after(() => {
      initRestEndpoints(server, authz, ledgers, registry, source, store, channels)
    })

  // Add security headers to all responses.
  server.addHook("onRequest", (req, reply, done) => {
    if (req.protocol === "https") {
      // require https for two years, the maximum
      reply.header(HEADERS.HSTS, "max-age=63072000; includeSubDomains; preload")
    }
    // https://infosec.mozilla.org/guidelines/web_security#content-security-policy
    reply.header(HEADERS.CSP, "upgrade-insecure-requests; default-src https:")
    // don't guess the content type, just use the content-type header.
    reply.header(HEADERS.XCTO, "nosniff")
    done()
  })

  shutdown("REST Service", server.close)

  // Will not launch in Cloud services without this explicit server address
  return server.listen({port, host: "0.0.0.0"})
}
