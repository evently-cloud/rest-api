/**
 * Copied from:
 *   https://github.com/withspectrum/callback-to-async-iterator
 *   https://github.com/DefinitelyTyped/DefinitelyTyped/tree/master/types/callback-to-async-iterator
 * License: MIT
 *
 * The project is archived, and the type definitions do not work in an ESM, so it is unusable as-is in
 * this project.
 */

export interface AsyncifyOptions<MESSAGE, LISTENER> {
  onClose?: (listener: LISTENER) => void | MESSAGE
  onError?: (err: Error) => Error
  buffering?: boolean
}


export type MessagePush<MESSAGE> = (message: MESSAGE) => void
export type ListenerProvider<MESSAGE, LISTENER> = (push: MessagePush<MESSAGE>) => Promise<LISTENER>


type IteratorResultConsumer<T> = (result: IteratorResult<T>) => void


const defaultOnError = (err: Error) => {
  throw err
}

export function asyncify<MESSAGE, LISTENER>(provider: ListenerProvider<MESSAGE, LISTENER>,
                                            options?: AsyncifyOptions<MESSAGE, LISTENER>): AsyncIterableIterator<MESSAGE> {
  const { onError = defaultOnError, buffering = true, onClose } = options ?? {}
  try {
    let pullQueue: IteratorResultConsumer<MESSAGE>[] = []
    let pushQueue: MESSAGE[] = []
    let listening = true
    let listener: LISTENER
    // Start listener
    provider(value => pushValue(value))
      .then(l => {
        listener = l
      })
      .catch(err => {
        onError(err)
      })

    function pushValue(value: MESSAGE) {
      const resolve = pullQueue.shift()
      if (resolve) {
        resolve({ value, done: false })
      } else if (buffering) {
        pushQueue.push(value)
      }
    }

    function pullValue(): Promise<IteratorResult<MESSAGE>> {
      return new Promise(resolve => {
        const value = pushQueue.shift()
        if (value !== undefined) {
          resolve({ value, done: false })
        } else {
          pullQueue.push(resolve)
        }
      })
    }

    function emptyQueue() {
      if (listening) {
        listening = false
        pullQueue.forEach(resolve => resolve({ value: undefined, done: true }))
        pullQueue = []
        pushQueue = []
        onClose && onClose(listener)
      }
    }

    // Have to put this in a constant so next() can call return() without TS complaining
    const asyncIterable = {
      next() {
        return listening ? pullValue() : asyncIterable.return()
      },
      return(value?: any) {
        emptyQueue()
        return Promise.resolve({ value, done: true })
      },
      throw(error?: any) {
        emptyQueue()
        onError(error)
        return Promise.reject(error)
      },
      [Symbol.asyncIterator]() {
        return this
      },
    }
    return asyncIterable
  } catch (err) {
    onError(err as Error)
    return {
      next() {
        return Promise.reject(err)
      },
      return() {
        return Promise.reject(err)
      },
      throw(error) {
        return Promise.reject(error)
      },
      [Symbol.asyncIterator]() {
        return this
      }
    }
  }
}
