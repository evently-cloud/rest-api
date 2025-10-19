import { Readable, Transform, TransformCallback } from "node:stream"


type Stringify = (entry: any) => string


function addLine(stringify: Stringify, buffer: string, entry: any): string {
  return `${buffer}${stringify(entry)}\n`
}


export function toNdJsonTransformer(stringify:  Stringify,
                                    input:      Readable): Transform {
  let buffer = ""

  const ndjsonTransform = new Transform({
    writableObjectMode: true,

    transform(entry:    any,
              encoding: BufferEncoding,
              sink:     TransformCallback) {
      buffer = addLine(stringify, buffer, entry)
      if (buffer.length > this.readableHighWaterMark) {
        sink(null, buffer)
        buffer = ""
      } else {
        sink()
      }
    },

    final(done: TransformCallback) {
      if (buffer) {
        this.push(buffer)
      }
      done()
    },

    destroy(err:  any,
            done: TransformCallback) {
      input.destroy(err)
      done(err)
    }
  })

  return input.pipe(ndjsonTransform)
}


const NDJSON_EXT = ".ndjson"


export function addNdJsonExt(uriPart: string): string {
  return `${uriPart}${NDJSON_EXT}`
}


export function trimNdJsonExt(select: string): string {
  return select.slice(0, -NDJSON_EXT.length)
}
