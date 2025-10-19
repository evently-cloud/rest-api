import createHttpError, { HttpError } from "http-errors"
import { omitBy } from "lodash-es"
import Long from "long"
import P from "pino"
import PG, { Options, Sql } from "postgres"

import { ShutdownHookRegistrar } from "../types.ts"

declare module "postgres" {
  // 1. Expand the parameter type to allow Long
  type ExtendedParameter = SerializableParameter<any> | Long

  // 2. Use module augmentation to extend the call signature, matching the original generics
  interface Sql<TTypes extends Record<string, unknown> = {}> {
    <T extends readonly (object | undefined)[] = Row[]>(
      strings: TemplateStringsArray,
      ...parameters: readonly ExtendedParameter[]
    ): PendingQuery<T>
  }
}


export async function init(shutdown:  ShutdownHookRegistrar,
                           logger:    P.Logger): Promise<Sql> {
  let ssl
  if (process.env.PGSSL) {
    ssl = {
      rejectUnauthorized: false
    }
  }

  const longType = {
    ...PG.BigInt,
    serialize:  (l: Long) => l.toString(),
    parse:      Long.fromString
  }
  const typeOpts = {
    ssl,
    types: {
      bigint: longType
    }
  }
  const config = gatherConfig(logger, typeOpts)

  const sql = typeof config === "string"
    ? PG(config, typeOpts)
    : PG(config)

  shutdown("Postgres Pool", sql.end)

  await dumpUserInfo(logger, sql)

  return sql
}

async function dumpUserInfo(logger: P.Logger, sql: Sql) {
  const [{current_user}] = await sql`SELECT current_user`
  const [{search_path}] = await sql`SHOW SEARCH_PATH`
  logger.info("$user: %s SEARCH_PATH: %s", current_user, search_path)


}

function gatherConfig(logger: P.Logger, typeOpts: Options<any>): string | Options<any> {
  const url = process.env.DATABASE_URL
  if (url) {
    return url
  }

  const dbPrefix = process.env.DB_PREFIX ?? "DB"  // "RDS" for Amazon
  logger.info("using db prefix %s", dbPrefix)
  const database = process.env[`${dbPrefix}_DATABASE`] ?? "evently"
  const user = process.env[`${dbPrefix}_USER`] ?? ""
  const password = process.env[`${dbPrefix}_PASSWORD`] ?? ""
  const host = process.env[`${dbPrefix}_HOST`] ?? ""
  const port = Number.parseInt(process.env[`${dbPrefix}_PORT`] ?? "5432")

  const envConfig = {
    ...typeOpts,
    database,
    user,
    password,
    host,
    port
  }

  //pg checks for the existence of keys, so take out the undefined and empty keys.
  const config = omitBy(envConfig, (v) => !v)
  // don't log the password, but show that one was supplied if present
  const loggable = {...config}
  if (loggable.password) {
    loggable.password = "••••••••"
  }
  logger.info("pg config: %j", loggable)

  return config
}


export async function wrapWithCatch<T>(logger: P.Logger, promise: Promise<T>): Promise<T> {
  try {
    return await promise
  } catch (err) {
    if (!(err instanceof HttpError)) {
      logger.error(err)
      // @ts-ignore PG has other Error types that have a code property
      if (err instanceof PG.PostgresError || err.code === "ECONNREFUSED") {
        err = createHttpError.ServiceUnavailable("Please try your call later.")
      } else {
        err = createHttpError.InternalServerError("Internal server error, sorry about that.")
      }
    }

    throw err
  }
}
