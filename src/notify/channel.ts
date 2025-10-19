import {createId} from "@paralleldrive/cuid2"
import { EventMessage } from "fastify-sse-v2"
import createHttpError from "http-errors"
import { IterableWeakSet } from "weakref"

import { PersistedEvent } from "../api/type/persisted-event.ts"
import { asyncify, MessagePush, ListenerProvider } from "../asyncify.ts"
import { encodeUnknownSelector } from "../selector-utils.ts"
import { Selector } from "../types.ts"
import { Channel, EventListener, EventListenerRegistrar, SelectorsNotification } from "./notify.ts"
import { createMatcher, SelectorMatcher } from "./selector-matcher.ts"


type SubscriptionFilter = {
  id:       string
  selector: Selector
  matcher:  SelectorMatcher
}

// key is compressed encoded selector, so we can handle idempotent subscriptions
type FilterMap = Map<string, SubscriptionFilter>
type SSEStream = AsyncIterableIterator<EventMessage>
type PersistedToEventMessage = (event: PersistedEvent) => EventMessage | undefined


const SSE_RETRY = 10_000 // 10 seconds


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

function findFilter(filters: Map<string, SubscriptionFilter>, subscriptionId: string): [string, SubscriptionFilter] | undefined {
  for (const [key, filter] of filters) {
    if (filter.id === subscriptionId) {
      return [key, filter]
    }
  }
}

async function handleClose(sseConnections: IterableWeakSet<SSEStream>){
  const closers: Promise<unknown>[] = []
  for (const sse of sseConnections) {
    // sse listener unregisters itself onClose
    sse.return && closers.push(sse.return())
  }
  await Promise.allSettled(closers)
}


function handleSubscribe(filters: FilterMap, selectorIn: Selector) {
  // drop limit
  const {
    limit,
    ...selector
  } = selectorIn
  const key = encodeUnknownSelector(selector)
  // idempotency, don't add same filter more than once
  let id = filters.get(key)?.id
  if (id === undefined) {
    id = createId()
    const matcher = createMatcher(selector)
    const filter = {
      id,
      selector,
      matcher
    }
    filters.set(key, filter)
  }
  return id
}


function handleUnsubscribe(filters: FilterMap, subscriptionId: string) {
  const filter = findFilter(filters, subscriptionId)
  if (filter) {
    filters.delete(filter[0])
  }
}


function getSubscriptions(filters: FilterMap) {
  return Array.from(filters.values(), filter => filter.id)
}


function getSubscription(filters: FilterMap, subscriptionId: string) {
  const filter = findFilter(filters, subscriptionId)
  if (filter) {
    return filter[1].selector
  }
}


function openEventStream(registrar:   EventListenerRegistrar,
                         filters:     FilterMap,
                         sseStreams:  WeakSet<SSEStream>,
                         after?:      string): AsyncIterableIterator<EventMessage> {
  if (after) {
    /*
        catch up the listener? This will be async, so it might miss events that happen while catchup is occurring.
        also, how will it know which selector to execute?
        when the listener is fired, the lastEventId updates internally.
        If there's a miss, It can replay again.

        This sounds pretty complicated. The listener should manage this, right?
        It would have an eventSource reference to look up the missed things.

        However, you're only looking for missed matches. If the new event matches the selector, who cares what was missed?
        You only need to look if the event does NOT match.
        In a fast-moving system, this will need a speed cache of recent events, so I can run the selector against that list
        instead of going to the db. Nathan Marz's speed layer approach.

        Actually, I can run the listener BACKWARDS. I only care about the most recent hit, not all the events.
        This is easier with in-memory queue, but will need an adjustment to the event selector function in PG.

        Need a timer on connection close to wait to unsubscribe all the selectors.
        Maybe that's the approach? Not an event cache, but a notification cache. It sticks around after the connection
        drops and keeps pumping matches into it; when reconnect occurs, we can just ignore the last-event-id
        and just send the current notification set.

        asyncify needs to be a bit different, more of a set?
        new push notification comes in; it looks through the pull list for matches. If matched, that means the notification
        has not been consumed, so it just drops the new notification.
      */
    throw createHttpError.BadRequest("can't use Last-Event-Id header yet")
  }

  const notificationToSse = createNotifySelectorFilter(filters)
  const listenToNotify: ListenerProvider<EventMessage, EventListener> = async (push: MessagePush<EventMessage>) => {
    const listener: EventListener = (e) => {
      const eventMessage = notificationToSse(e)
      if (eventMessage) {
        push(eventMessage)
      }
    }
    await registrar.addEventListener(listener)
    return listener
  }

  const stream = asyncify(listenToNotify, {
    onClose: (listener) => {
      registrar.removeEventListener(listener)
    }
  })
  sseStreams.add(stream)
  return stream
}


function createNotifySelectorFilter(filters: FilterMap): PersistedToEventMessage {
  return (event) => {
    const subscriptionIds = []
    for (const filter of filters.values()) {
      if (filter.matcher(event)) {
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
  Currently, data is just a comma-separated list of subscription IDs that matched the new event.
  The client needs to take that ID and turn it into a selector query. Before, they would take the ID and
  post it to /selectors/ with their after and limit.

  If I don't add back the selectorId property of the form, they would need to do the bookkeeping themselves to match
  up the subscriptionId with the selector. Maybe that's OK.

  SubscriptionId should be a cuid not the encoded selector. Smaller that way. They do bookkeeping, but they need to do
  that anyhow to send a selector request with the right 'after' value.
   */
  const data = subscriptionIds.join(",")
  return {
    retry: SSE_RETRY,
    event: "Subscriptions Triggered",
    id,
    data
  }
}
