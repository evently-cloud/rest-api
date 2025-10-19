console.time("startup")

import { initAll } from "./init-all.ts"


try {
  await initAll()
  console.timeEnd("startup")
} catch (err) {
  console.error(err)
}
