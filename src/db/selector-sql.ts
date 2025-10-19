import { isEmpty } from "lodash-es"

import { isFilterSelector } from "../selector-utils.ts"
import { DataFilter, EntitiesRecord, FilterSelector, JsonpathFilter, Selector } from "../types.ts"


export function unknownSelectorToSql(selector: Selector): string {
  return isFilterSelector(selector)
    ? filterSelectorToSql(selector)
    : ""
}


/*
  TODO eliminate jsonb_path_exists usage so the indexes will be hit with queries. To do this,
   replace the named vars in the query with their values in var. This is easier said than done,
   because a named var string like $me could also be a string value in another part of the query:
      $ ? (@ == $me AND @ != "I do not eat $meat")
   Only want to replace the first $me and leave the second $me in the string alone.
 */
function jsonpathToSql(field: string, jsonpath: JsonpathFilter) {
  const {query, vars} = jsonpath
  const qStr = toPostgresString(query)
  const vStr = vars && toPostgresJson(vars)
  return vStr
    ? `jsonb_path_exists(${field}, ${qStr}, ${vStr})`
    : `${field} @? ${qStr}`
}


export function filterSelectorToSql(selector: FilterSelector): string {
  const {
    entities,
    meta,
    events
  } = selector

  const queries = []

  if (!isEmpty(entities)) {
    queries.push(generateEntitiesQuery(entities))
  }

  if (!isEmpty(meta)) {
    queries.push(jsonpathToSql("meta", meta))
  }

  if (!isEmpty(events)) {
    queries.push(generateEventsQuery(events))
  }

  return `(${queries.join("\nAND ")})`
}

/*
Use @? operator statements to pick up entity index
entities: [{
  name: "cart",
  keys: ["1","2"]
}, {
  name: "wishlist",
  keys: ["a", "b"]
}],
     entities @? '$."cart" ? (@==1 || @==2))'
  OR entities @? '$."wishlist" ? (@=="a" || @=="b")'

  SQL/JSONPath is sadly missing a right-side array unwrapping for statements, so you can't do this:
     entities @? '$."cart" ? (@==[1,2])'
  It will do this with named vars, however. That requires using jsonb_path_exists, so it uses seq scan instaed
  of the GIN index.
 */
function generateEntitiesQuery(entities: EntitiesRecord) {
  const entityQueries: string[] = []
  for (const [name, keys] of Object.entries(entities)) {
    const keyStmts = keys.map((key) => `@==${toJsonpathString(key)}`)
    entityQueries.push(`entities @? '$.${toJsonpathString(name)} ? (${keyStmts.join(" || ")})'`)
  }
  return `(${entityQueries.join("\nOR ")})`
}


/*
data: [{
  "game-started": {
    "query": "$ ? (@.player1Key==\"Eugene_Harris\"||@.player2Key==\"Eugene_Harris\")"
  },
  "game-completed": {
    "query": "$.winnerKey ? (@==$player)",
    "vars": {
      "player": "Eugene_Harris"
    }
  }
}]

WHERE
  (event = 'game-started' AND data @? '$ ? (@.player1Key=="Eugene_Harris"||@.player2Key=="Eugene_Harris")')
  OR (event = 'game-competed' AND jsonb_path_exists(data, '$.winnerKey ? (@==$player)', {"player":"Eugene_Harris"}))
*/
function generateEventsQuery(dataFilter: DataFilter): string {
  const queries = []
  const events = Object.keys(dataFilter)
  const justEvents = []

  for (const event of events) {
    const filter = dataFilter[event]
    // skip JSONPath exists test for "$" queries
    if (filter.query === "$") {
      justEvents.push(event)
    } else {
      queries.push(`(event = ${toPostgresString(event)} AND ${jsonpathToSql("data", filter)})`)
    }
  }
  if (justEvents.length) {
    queries.push(singleOrAny("event", justEvents))
  }
  return queries.join("\nOR ")
}

/*
  For list comparisons, ANY is faster than IN: https://pganalyze.com/blog/5mins-postgres-performance-in-vs-any
  Also in this blog, single-element comparisons are faster if the query does not use single-element lists.
 */
function singleOrAny(column: string, values: string[]): string {
  const {length} = values
  switch (length) {
    case 0:
      throw new Error(`Empty values for ${column}`)
    case 1:
      return `${column} = ${toPostgresString(values[0])}`
    default:
      return `${column} = ANY(${stringArrayToPostgresArray(values)})`
  }
}

function stringArrayToPostgresArray(strings: string[]): string {
  const values = []
  for (const str of strings) {
    values.push(toArrayElement(str))
  }
  return `'{${values.join(",")}}'`
}


function toArrayElement(str: string): string {
  // Two calls is faster than replaceAll and single regex.
  str = str.replaceAll("\\", "\\\\")
           .replaceAll('"', '\\"')
  return `"${escapeSingleQuote(str)}"`
}


function escapeSingleQuote(str: string): string {
  return str.replaceAll("'", "''")
}

function toJsonpathString(str: string): string {
  return escapeSingleQuote(JSON.stringify(str))
}

export function toPostgresString(str: string): string {
  return `'${escapeSingleQuote(str)}'`
}

export function fromPostgresString(str: string): string {
  return str.replaceAll("''", "'")
}

function toPostgresJson(input: any): string {
  return toPostgresString(JSON.stringify(input))
}
