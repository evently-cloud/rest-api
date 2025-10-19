import { EventMessage } from "fastify-sse-v2"

import { PersistedEvent } from "../api/type/persisted-event.ts"
import { Ledger, Selector } from "../types.ts"


export interface EventListenerRegistrar {
  addEventListener(listener: EventListener): Promise<void>
  removeEventListener(listener: EventListener): void
}


export type EventListener = (event: PersistedEvent) => void


export type SelectorsNotification = {
  subscriptionIds:  string[]
  position:         string
}


/**
 * A channel represents a group of selectors that emit notifications. A channel can have multiple SSE connections.
 * Channels can be long-lived, based on the lifetime of the application that initiates and uses it. One can
 * imagine a web page being open for a long time, as a dashboard. The browser could refresh, the application could
 * restart, but the channel will persist their subscriptions.
 *
 * Similarly, a channel must survive a restart. The subscriptions must be persisted in the DB to support this.
 */
export interface Channel {
  id: string

  subscribe(selector: Selector): string

  unsubscribe(subscriptionId: string): void

  subscriptions(): string[]

  subscription(subscriptionId: string): Selector | undefined

  openEventStream(lastEventId?: string): AsyncIterable<EventMessage>

  close(): Promise<void>
}


export interface Channels {
  /**
   * Opens a channel and returns the channel ID.
   * @param ledger the ledger to open a channel for.
   */
  open(ledger: Ledger): string

  exists(ledger: Ledger, channelId: string): boolean

  subscribe(ledger: Ledger, channelId: string, selector: Selector): string

  unsubscribe(ledger: Ledger, channelId: string, subscriptionId: string): void

  subscriptions(ledger: Ledger, channelId: string): string[]

  subscription(ledger: Ledger, channelId: string, subscriptionId: string): Selector | undefined

  openEventStream(ledger: Ledger, channelId: string, lastEventId?: string): AsyncIterable<EventMessage>

  close(ledger: Ledger, channelId: string): Promise<void>
}
