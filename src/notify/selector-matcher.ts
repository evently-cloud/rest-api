import * as sjp from "sql-jsonpath-js"

import { PersistedEvent } from "../api/type/persisted-event.ts"
import { isFilterSelector } from "../selector-utils.ts"
import { EntitiesRecord, FilterSelector, JsonpathFilter, Selector } from "../types.ts"


export type SelectorMatcher = (event: PersistedEvent) => boolean


export function createMatcher(selector: Selector): SelectorMatcher {
  return isFilterSelector(selector)
    ? createFilterMatcher(selector)
    : createAllMatcher()
}


function createFilterMatcher(selector: FilterSelector): SelectorMatcher {
  const {
    entities,
    meta,
    events
  } = selector

  const matchers: SelectorMatcher[] = []

  if (entities) {
    matchers.push(createEntitiesMatcher(entities))
  }
  if (meta) {
    matchers.push(createMetaMatcher(meta))
  }
  if (events) {
    matchers.push(createDataMatcher(events))
  }

  return (e) => {
    for (const matcher of matchers) {
      if (matcher(e)) {
        return true
      }
    }
    return false
  }
}


function createAllMatcher(): SelectorMatcher {
  return () => true
}

function createEntitiesMatcher(searchEntities: EntitiesRecord): SelectorMatcher {
  // Looking for a single match
  return ({entities}) => {
    for (const [name, keys] of Object.entries(entities)) {
      const searchKeys = searchEntities[name]
      if (searchKeys) {
        for (const key of keys) {
          if(searchKeys.includes(key)) {
            return true
          }
        }
      }
    }
    return false
  }
}

type Exists = (input: any) => boolean

function createJsonpathMatcher({query, vars}: JsonpathFilter): Exists {
  // skip matcher for 'exists' query
  if (query === "$") {
    return createAllMatcher()
  }
  const matcher = sjp.compile(query)
  const variables = vars && {variables: vars}
  return (input) => matcher.exists(input, variables) as boolean
}

function createMetaMatcher(filter: JsonpathFilter): SelectorMatcher {
  const matcher = createJsonpathMatcher(filter)
  return ({meta}) => matcher(meta)
}


function createDataMatcher(dataFilter: Record<string, JsonpathFilter>): SelectorMatcher {
  const matchers = Object.entries(dataFilter)
    .reduce((acc, [event, filter]) => {
      acc[event] = createJsonpathMatcher(filter)
      return acc
    }, {} as Record<string, (input: any) => boolean>)

  return ({event, data}) => matchers[event]?.(data) ?? false
}

