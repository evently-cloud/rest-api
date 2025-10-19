
const encoder = new TextEncoder()

export function stringToBytes(str: string): Uint8Array {
  return encoder.encode(str)
}
