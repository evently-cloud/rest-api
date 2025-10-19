import closeWithGrace from "close-with-grace"
import P from "pino"

import {ShutdownHookRegistrar} from "./types.ts"



export type ShutdownHook = {
  name: string
  hook: () => any
}

export function initShutdownRegistrar(logger: P.Logger): ShutdownHookRegistrar {

  const hooks: ShutdownHook[] = []

  const closeƒ: closeWithGrace.CloseWithGraceAsyncCallback = async ({err: closeError}) => {
    if (closeError) {
      logger.fatal(`Error while shutting down: ${closeError.message}`)
    } else {
      for (const {name, hook} of hooks) {
        logger.info(`Shutting down ${name}`)
        try {
          const hookResult = hook()
          if (hookResult instanceof Promise) {
            await hookResult
          }
        } catch (err) {
          logger.fatal(err)
        }
      }
    }
  }
  closeWithGrace(closeƒ)

  return (name, shutdownFn) => {
    // unshift because shutdown order is LIFO
    hooks.unshift({name, hook: shutdownFn})
  }
}
