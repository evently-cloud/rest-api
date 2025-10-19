import { createId } from "@paralleldrive/cuid2"
import { EventMessage } from "fastify-sse-v2"
import createHttpError from "http-errors"
import P from "pino"
import { Sql } from "postgres"

import { Ledger, Selector, ShutdownHookRegistrar } from "../types.ts"
import { createChannel } from "./channel.ts"
import { Channel, Channels, EventListenerRegistrar } from "./notify.ts"
import * as notifyRegistrar from "./postgres-listener.ts"



export async function init(shutdown:  ShutdownHookRegistrar,
                           logger:    P.Logger,
                           sql:       Sql): Promise<Channels> {

  const registrar = await notifyRegistrar.init(shutdown, logger, sql)

  const channels: Map<string, Channel> = new Map()

  return {
    open:             (l) => handleOpenChannel(registrar, channels, l),
    exists:           (l, cid) => handleChannelExists(channels, l, cid),
    openEventStream:  (l, cid, leid) => handleOpenEventStream(channels, l, cid, leid),
    close:            (l, cid) => handleCloseChannel(channels, l, cid),
    subscribe:        (l, cid, s) => handleSubscribe(channels, l, cid, s),
    unsubscribe:      (l, cid, sid) => handleUnsubscribe(channels, l, cid, sid),
    subscriptions:    (l, cid) => getSubscriptions(channels, l, cid),
    subscription:     (l, cid, sid) => getSubscription(channels, l, cid, sid)
  }
}


type ChannelsMap = Map<string, Channel>


function createKey({id}: Ledger, channelId: string): string {
  return `${id}|${channelId}`
}


function getChannel(channels:   ChannelsMap,
                    ledger:     Ledger,
                    channelId:  string): Channel {
  const channel = channels.get(createKey(ledger, channelId))
  if (channel) {
    return channel
  }
  throw createHttpError.NotFound(`channel ${channelId} does not exist.`)
}


function handleOpenChannel(registrar: EventListenerRegistrar,
                           channels:  ChannelsMap,
                           ledger:    Ledger): string {
  const channelId = createId()
  const channel = createChannel(registrar, channelId)
  channels.set(createKey(ledger, channelId), channel)
  return channelId
}


function handleChannelExists(channels:   ChannelsMap,
                             ledger:     Ledger,
                             channelId:  string): boolean {
  return channels.has(createKey(ledger, channelId))
}


async function handleCloseChannel(channels:   ChannelsMap,
                                  ledger:     Ledger,
                                  channelId:  string) {
  const channel = getChannel(channels, ledger, channelId)
  channels.delete(createKey(ledger, channelId))
  await channel.close()
}


function handleOpenEventStream(channels:      ChannelsMap,
                               ledger:        Ledger,
                               channelId:     string,
                               lastEventId?:  string): AsyncIterable<EventMessage> {
  const channel = getChannel(channels, ledger, channelId)
  return channel.openEventStream(lastEventId)
}


function handleSubscribe(channels:  ChannelsMap,
                         ledger:    Ledger,
                         channelId: string,
                         selector:  Selector): string {
  const channel = getChannel(channels, ledger, channelId)
  return channel.subscribe(selector)
}


function handleUnsubscribe(channels:        ChannelsMap,
                           ledger:          Ledger,
                           channelId:       string,
                           subscriptionId:  string): void {
  const channel = getChannel(channels, ledger, channelId)
  channel.unsubscribe(subscriptionId)
}


function getSubscriptions(channels:  ChannelsMap,
                          ledger:    Ledger,
                          channelId: string) {
  const channel = getChannel(channels, ledger, channelId)
  return channel.subscriptions()
}


function getSubscription(channels:       ChannelsMap,
                         ledger:         Ledger,
                         channelId:      string,
                         subscriptionId: string): Selector | undefined {
  const channel = getChannel(channels, ledger, channelId)
  return channel.subscription(subscriptionId)
}
