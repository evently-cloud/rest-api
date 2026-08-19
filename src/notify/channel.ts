import {createId} from "@paralleldrive/cuid2"
import { EventMessage } from "fastify-sse-v2"
import { IterableWeakSet } from "weakref"

import { PersistedEvent } from "../api/type/persisted-event.ts"
import { asyncify, MessagePush } from "../asyncify.ts"
import { eventIdToString, maybeFromEventIdString } from "../eventId-utils.ts"
import { encodeFilterSelector } from "../selector-utils.ts"
import { EventID, Selector } from "../types.ts"
import { Channel, EventListener, EventListenerRegistrar, SelectorsNotification } from "./notify.ts"
import { createMatcher, SelectorMatcher } from "./selector-matcher.ts"


type SubscriptionFilter = {
  id:           string
  selector:     Selector
  matcher:      SelectorMatcher
  lastEventId?: EventID
}

// key is compressed encoded selector, so we can handle idempotent subscriptions
type FilterMap = Map<string, SubscriptionFilter>
type SSEStream = AsyncIterableIterator<EventMessage>
type PersistedToEventMessage = (event: PersistedEvent) => EventMessage | undefined


export function createChannel(registrar: EventListenerRegistrar, id: string): Channel {

  const filters = new Map<string, SubscriptionFilter>()
  // weak set because the client can close their side at any time. Weakness will clean those up.
  const sseStreams = new IterableWeakSet<SSEStream>()

  return {
    id,
    subscribe:        (s) => handleSubscribe(filters, s),
    unsubscribe:      (s) => handleUnsubscribe(filters, s),
    subscriptions:    () => getSubscriptions(filters),
    subscription:     (sid) => getSubscription(filters, sid),
    openEventStream:  (a?) => openEventStream(registrar, filters, sseStreams, a),
    close:            () => handleClose(sseStreams)
  }
}

function findFilter(filters: Map<string, SubscriptionFilter>, subscriptionId: string): { key: string, selector: Selector } | undefined {
  for (const [key, filter] of filters) {
    const {id, selector} = filter
    if (id === subscriptionId) {
      return {key, selector}
    }
  }
}

async function handleClose(sseConnections: Iterable<SSEStream>){
  const closers: Promise<unknown>[] = []
  for (const sse of sseConnections) {
    // sse listener unregisters itself onClose
    sse.return && closers.push(sse.return())
  }
  await Promise.allSettled(closers)
}


function handleSubscribe(filters: FilterMap, selectorIn: Selector) {
  // drop 'limit', retain 'after' for lastEventId
  const {
    limit,
    after: lastEventId,
    ...selector
  } = selectorIn
  const key = encodeFilterSelector(selector)
  // idempotency, don't add same filter more than once
  let id = filters.get(key)?.id
  if (id === undefined) {
    id = createId()
    const matcher = createMatcher(selector)
    const filter = {
      id,
      selector,
      matcher,
      lastEventId
    }
    filters.set(key, filter)
  }
  return id
}


function handleUnsubscribe(filters: FilterMap, subscriptionId: string) {
  const filter = findFilter(filters, subscriptionId)
  if (filter) {
    filters.delete(filter.key)
  }
}


function getSubscriptions(filters: FilterMap) {
  return Array.from(filters.values(), filter => filter.id)
}


function getSubscription(filters: FilterMap, subscriptionId: string) {
  const filter = findFilter(filters, subscriptionId)
  if (filter) {
    return filter.selector
  }
}


function openEventStream(registrar:     EventListenerRegistrar,
                         filters:       FilterMap,
                         sseStreams:    WeakSet<SSEStream>,
                         afterHeader?:  string): AsyncIterableIterator<EventMessage> {

  const notificationToSse = createNotifySelectorFilter(filters)
  const listenToNotify = async (push: MessagePush<EventMessage>) => {
    const listener: EventListener = (e) => {
      const eventMessage = notificationToSse(e)
      if (eventMessage) {
        push(eventMessage)
      }
    }
    registrar.addEventListener(listener)
    return listener
  }

  // consider switching to https://github.com/rolftimmermans/event-iterator
  const stream = asyncify(listenToNotify, {
    onClose: (listener, value) => {
      registrar.removeEventListener(listener, "disconnected" === value)
    }
  })

  sseStreams.add(stream)

  // Did the client send Last-Event-Id header? If so, maybe send a notification message so they can catch up.
  if (afterHeader) {
    const afterId = maybeFromEventIdString(afterHeader)
    let id = afterId as EventID
    const {timestamp: afterTs} = id
    const subscriptionIds = filters.values()
      .filter((f) => {
        const {lastEventId} = f
        if (lastEventId?.timestamp.greaterThan(afterTs)) {
          // capture the most recent event id
          if (lastEventId.timestamp.greaterThan(id.timestamp)) {
            id = lastEventId
          }
          return true
        }
      })
      .map((f) => f.id)
      .toArray()
    if (subscriptionIds.length) {
      const event = createEvent({
        subscriptionIds,
        position: eventIdToString(id)
      })
      stream.next(event)
    }
  }

  return stream
}


function createNotifySelectorFilter(filters: FilterMap): PersistedToEventMessage {
  return (event) => {
    const subscriptionIds = []
    for (const filter of filters.values()) {
      if (filter.matcher(event)) {
        filter.lastEventId = maybeFromEventIdString(event.eventId)
        subscriptionIds.push(filter.id)
      }
    }

    if (subscriptionIds.length) {
      return createEvent({
        position:        event.eventId,
        subscriptionIds: subscriptionIds
      })
    }
  }
}


function createEvent({position: id, subscriptionIds}: SelectorsNotification): EventMessage {
  /*
  Data is just a comma-separated list of subscription IDs that matched the new event.
  The client needs to take that ID and turn it into a selector query. Before, they would take the ID and
  post it to /selectors/ with their after and limit.

  If I don't add back the selectorId property of the form, they would need to do the bookkeeping themselves to match
  up the subscriptionId with the selector. Maybe that's OK.

  SubscriptionId should be a cuid not the encoded selector. Smaller that way. They do bookkeeping, but they need to do
  that anyhow to send a selector request with the right 'after' value.
   */
  const data = subscriptionIds.join(",")
  return {
    event: "Subscriptions Triggered",
    id,
    data
  }
}
