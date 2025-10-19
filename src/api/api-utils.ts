import fastJson from "fast-json-stringify"
import { FastifyRequest } from "fastify"
import createHttpError from "http-errors"
import { FLOAT32_OPTIONS, Packr } from "msgpackr/pack"
import { constrain, Constraint, Type } from "specified"

import { Ledger } from "../types.ts"
import { REQUEST_CTX } from "./constants.ts"
import PersistedEventSchema from "../../schema/persisted-event.json" with { type: "json" }


const packer = new Packr({
  useRecords:       false,
  variableMapSize:  true,
  useFloat32:       FLOAT32_OPTIONS.DECIMAL_FIT
})

export function formattedJSON(body: any): string {
  return JSON.stringify(body, null, 2)
}

export function toURIPart(params: any): string {
  return packer
    .pack(params)
    .toString("base64url")
}

export function fromURIPart<T>(param: string): T {
  try {
    const bytes = Buffer.from(param, "base64url")
    return packer.unpack(bytes)
  } catch (e) {
    throw createHttpError.BadRequest("invalid URI part")
  }
}

export function linkProfile(profile: string): string {
  return profile.slice(1, -1)
}

export function getLedgerFromRequestCtx(request: FastifyRequest): Ledger {
  const ledger = request.requestContext.get(REQUEST_CTX.LEDGER)
  if (!ledger) {
    throw new createHttpError.Unauthorized("Authorization requires ledger ID.")
  }
  return ledger
}


export const stringifyPersistedEvent = fastJson(PersistedEventSchema)

export const nonEmptyStringSpec = constrain(Type.string, [Constraint.string.notEmpty])
export const nonEmptyStringArraySpec = constrain(Type.array(nonEmptyStringSpec), [Constraint.array.length({ min: 1 })])
