import { RBAC } from "fast-rbac"
import { FastifyInstance, preHandlerHookHandler, RouteShorthandOptions } from "fastify"
import createHttpError from "http-errors"

import { Ledgers } from "../../ledgers/index.ts"
import { REQUEST_CTX } from "../constants.ts"
import { processAuth } from "./header-validator.ts"


export enum Permission {
  ReadPublic              = "read-public",
  ReadLedgers             = "read-ledgers",
  CreateLedger            = "create-ledger",
  DeleteLedger            = "delete-ledger",
  ResetLedger             = "reset-ledger",
  DownloadLedger          = "download-ledger",
  ReadRegistry            = "read-registry",
  RegisterEvent           = "register-event",
  UnregisterEvent         = "unregister-event",
  ReplayEvents            = "replay-events",
  AppendEvent             = "append-event",
  SubscribeNotifications  = "subscribe-notifications"
}


export enum Role {
  Public    = "public",
  Admin     = "admin",
  Registrar = "registrar",
  Client    = "client",
  Reader    = "reader",
  Appender  = "appender"
}



// this is encoded in the token
export type Claims = {
  ledger?:  string,
  roles:    Role[]
}


export const RbacConfig: RBAC.Options = {
  roles: {
    [Role.Public]: {
      can: [Permission.ReadPublic]
    },
    [Role.Admin]: {
      inherits: [Role.Public],
      can: [Permission.ReadLedgers, Permission.CreateLedger, Permission.DeleteLedger, Permission.ResetLedger, Permission.DownloadLedger]
    },
    [Role.Registrar]: {
      inherits: [Role.Public],
      can: [Permission.ReadRegistry, Permission.RegisterEvent, Permission.UnregisterEvent]
    },
    [Role.Reader]: {
      inherits: [Role.Public],
      can: [Permission.ReplayEvents]
    },
    [Role.Appender]: {
      inherits: [Role.Public],
      can: [Permission.AppendEvent]
    },
    [Role.Client]: {
      can: [Permission.SubscribeNotifications],
      inherits: [Role.Reader, Role.Appender]
    }
  }
}


export interface Authorization {
  readPublic: RouteShorthandOptions,
  allowed(permission: Permission): preHandlerHookHandler
}


export const authHeadersSchema = {
  schema: {
    headers: {
      $ref: "auth-token"
    }
  }
}


export function initAuth(fastify: FastifyInstance, ledgers: Ledgers): Authorization {

  fastify.addHook("preValidation", async (req, rep) => {
    return processAuth(ledgers, req, rep)
  })

  const rbac = new RBAC(RbacConfig)

  function verifyRolePermission(permission: Permission, roles?: Role[]) {
    if (roles) {
      return roles.length === 1
        ? rbac.can(roles[0], permission)
        : roles.find(role => rbac.can(role, permission))
    }
  }

  const allowed = (p: Permission): preHandlerHookHandler => {
    return (req, res, done) => {
      const roles = req.requestContext.get(REQUEST_CTX.ROLES)
      if (verifyRolePermission(p, roles)) {
        done()
      } else {
        done(createHttpError.Unauthorized())
      }
    }
  }

  return {
    allowed,
    readPublic: {
      ...authHeadersSchema,
      preHandler: allowed(Permission.ReadPublic)
    }
  }
}


