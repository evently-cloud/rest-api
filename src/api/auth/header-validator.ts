import { FastifyReply, FastifyRequest } from "fastify"
import { optional, Type, verify } from "specified"

import { Ledgers } from "../../ledgers/index.ts"
import { formattedJSON, nonEmptyStringArraySpec, nonEmptyStringSpec } from "../api-utils.ts"
import { CONTENT_TYPES, HEADERS, REQUEST_CTX } from "../constants.ts"
import { Role } from "./index.ts"



// https://datatracker.ietf.org/doc/html/rfc6750
const BEARER = "Bearer "
const EVENTLY_REALM = "evently"


const claimsSpec = Type.object({
  ledger: optional(nonEmptyStringSpec),
  roles:  nonEmptyStringArraySpec
})


export async function processAuth(ledgers: Ledgers,
                                  request: FastifyRequest,
                                  reply:   FastifyReply): Promise<void> {

  const { authorization } = request.headers

  if (!authorization) {
    return reply
      .header(HEADERS.WWW_AUTHENTICATE, `${BEARER}realm="${EVENTLY_REALM}"`)
      .status(401)
      .type(CONTENT_TYPES.JSON)
      .send(formattedJSON({
        error:      "Unauthorized",
        message:    "Authorization Bearer token is required.",
        statusCode: 401
      }))
  }

  const authStr = Array.isArray(authorization)
    ? authorization[0]
    : authorization

  try {
    const text = Buffer
      .from(authStr.slice(BEARER.length), "base64url")
      .toString("utf8")

    //this will throw an error on parse failure and validation
    const claims = verify(claimsSpec, JSON.parse(text)).value()

    // Not validating roles contents.
    request.requestContext.set(REQUEST_CTX.ROLES, claims.roles as Role[])

    if (claims.ledger) {
      const ledger = await ledgers.forLedgerId(claims.ledger)
      if (ledger) {
        request.requestContext.set(REQUEST_CTX.LEDGER, ledger)
      }
    }
    return
  } catch (e) {
    // Various failures
    request.log.debug(e, "Failed to process authorization header")
  }

  return reply
    .header(HEADERS.WWW_AUTHENTICATE, `${BEARER}realm="${EVENTLY_REALM}", error="invalid_token"`)
    .status(401)
    .type(CONTENT_TYPES.JSON)
    .send(formattedJSON({
      error:      "Unauthorized",
      message:    "Invalid Authorization Bearer token.",
      statusCode: 401
    }))
}
