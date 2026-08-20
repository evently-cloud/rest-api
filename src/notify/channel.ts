import { createId } from "@paralleldrive/cuid2"
import { EventMessage } from "fastify-sse-v2"

import { PersistedEvent } from "../api/type/persisted-event.ts"
import { asyncify, MessagePush } from "../asyncify.ts"
import { eventIdToString, maybeFromEventIdString } from "../eventId-utils.ts"
import { encodeFilterSelector } from "../selector-utils.ts"
import { EventID, Selector, SSE_RETRY } from "../types.ts"
import { Channel, NotifyListener, NotifyListenerRegistrar, SelectorsNotification } from "./notify.ts"
import { ResettableTimer } from "./resettable-timer.ts"
import { createMatcher, SelectorMatcher } from "./selector-matcher.ts"


type ChannelState = {
  id:         string
  registered: boolean
  listener:   NotifyListener
  timer:      ResettableTimer<[]>
}

type SubscriptionFilter = {
  id:           string
  selector:     Selector
  matcher:      SelectorMatcher
  lastEventId?: EventID
}

// key is compressed encoded selector, so we can handle idempotent subscriptions
type FilterMap = Map<string, SubscriptionFilter>
type SSEStream = AsyncIterableIterator<EventMessage>
type MessagePusher = MessagePush<EventMessage>
type PersistedEventToMessage = (event: PersistedEvent) => EventMessage | undefined

type Subscriber = {
  stream: SSEStream
  push:   MessagePusher
}


export function createChannel(registrar: NotifyListenerRegistrar, id: string): Channel {

  const filters = new Map<string, SubscriptionFilter>()
  const subscribers = new Set<Subscriber>()
  const channel = {
    id,
    registered: false,
    listener: createChannelListener(filters, subscribers),
    timer: new ResettableTimer(() => handleClose(registrar, channel, subscribers), SSE_RETRY * 4)
  }

  return {
    id,
    subscribe:        (s) => handleSubscribe(registrar, channel, filters, s),
    unsubscribe:      (s) => handleUnsubscribe(filters, s),
    subscriptions:    () => getSubscriptionIds(filters),
    subscription:     (sid) => getSubscription(filters, sid),
    openEventStream:  (a?) => openEventStream(channel, filters, subscribers, a),
    close:            () => handleClose(registrar, channel, subscribers)
  }
}


function createChannelListener(filters:     FilterMap,
                               subscribers: Set<Subscriber>): NotifyListener {
  const filtersProcessor = createNotifySelectorFilter(filters)

  return (event) => {
    const eventMessage = filtersProcessor(event)
    if (eventMessage) {
      subscribers.forEach(({push}) => push(eventMessage))
    }
  }
}

// called when subscribe() is called. Late binding to Postgres
function registerChannelListener(registrar: NotifyListenerRegistrar,
                                 channel:   ChannelState) {
  const {id, timer, registered, listener} = channel
  timer.cancel()
  if (!registered) {
    registrar.addListener(id, listener)
  }
  channel.registered = true
}


function findFilter(filters:        FilterMap,
                    subscriptionId: string): { key: string, selector: Selector } | undefined {
  for (const [key, filter] of filters) {
    const {id, selector} = filter
    if (id === subscriptionId) {
      return {key, selector}
    }
  }
}

async function handleClose(registrar:   NotifyListenerRegistrar,
                           channel:     ChannelState,
                           subscribers: Set<Subscriber>){
  channel.timer.cancel()
  if (channel.registered) {
    registrar.removeListener(channel.id)
    channel.registered = false
  }

  const closers: Promise<unknown>[] = []
  for (const sub of subscribers) {
    const {stream} = sub
    stream.return && closers.push(stream.return())
  }
  await Promise.allSettled(closers)
  subscribers.clear()
}


function handleSubscribe(registrar:   NotifyListenerRegistrar,
                         channel:     ChannelState,
                         filters:     FilterMap,
                         selectorIn:  Selector) {
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

  registerChannelListener(registrar, channel)
  return id
}


function handleUnsubscribe(filters:         FilterMap,
                           subscriptionId:  string) {
  const filter = findFilter(filters, subscriptionId)
  if (filter) {
    filters.delete(filter.key)
  }
}


function getSubscriptionIds(filters: FilterMap) {
  return filters.values()
    .map(filter => filter.id)
    .toArray()
}


function getSubscription(filters:         FilterMap,
                         subscriptionId:  string) {
  const filter = findFilter(filters, subscriptionId)
  if (filter) {
    return filter.selector
  }
}


function openEventStream({timer}:       ChannelState,
                         filters:       FilterMap,
                         subscribers:   Set<Subscriber>,
                         afterHeader?:  string): AsyncIterableIterator<EventMessage> {
  timer.cancel()

  const listenToChannel = async (push: MessagePusher) => {
    subscriber.push = push
    return push
  }

  // this gets fully initialized by the end of the method
  //@ts-ignore
  const subscriber: Subscriber = { }

  // consider switching asyncify to https://github.com/rolftimmermans/event-iterator
  const stream = asyncify(listenToChannel, {
    onClose: (p, value) => {
      const sub = subscribers
        .values()
        .find((s) => p === s.push)
      if (sub) {
        subscribers.delete(sub)
      }
      if ("disconnected" === value && subscribers.size === 0) {
        timer.reset()
      }
    }
  })

  subscriber.stream = stream
  subscribers.add(subscriber)

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
      subscriber.push(event)
    }
  }

  return stream
}


function createNotifySelectorFilter(filters: FilterMap): PersistedEventToMessage {
  return (event) => {
    const subscriptionIds = filters.values()
      .filter((filter) => {
        if (filter.matcher(event)) {
          filter.lastEventId = maybeFromEventIdString(event.eventId)
          return true
        }
      })
      .map((f) => f.id)
      .toArray()

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
