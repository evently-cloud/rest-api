import { FastifyInstance, FastifyReply, FastifyRequest } from "fastify"
import createHttpError from "http-errors"

import { maybeFromEventIdString } from "../eventId-utils.ts"
import { Channels } from "../notify/notify.ts"
import { isFilterSelector } from "../selector-utils.ts"
import { formattedJSON, getLedgerFromRequestCtx, linkProfile } from "./api-utils.ts"
import { authHeadersSchema, Authorization, Permission } from "./auth/index.ts"
import { CONTENT_TYPES, HEADERS, L3_PATTERNS, L3_PROFILES, L3_RELS } from "./constants.ts"
import ChannelParamSchema from "../../schema/channel-path-params.json" with { type: "json" }
import ChannelSSEHeadersSchema from "../../schema/channel-sse-headers.json" with { type: "json" }
import ChannelSubscriptionParamsSchema from "../../schema/channel-subscription-path-params.json" with { type: "json" }
import SelectorSubscriptionFormSchema from "../../schema/notify-subscribe-form.json" with { type: "json" }
import { ChannelPathParameters } from "./type/channel-path-params.ts"
import { ChannelSSEHeaders } from "./type/channel-sse-headers.ts"
import { ChannelSubscriptionPathParameters } from "./type/channel-subscription-path-params.ts"
import { HALObject, LinkObject } from "./type/hal.ts"
import { SelectorSubscriptionForm } from "./type/notify-subscribe-form.ts"


export enum PATHS {
  NOTIFY        = "/notify",
  // form to open a channel
  OPEN_CHANNEL  = "/notify/open-channel",
  CHANNEL       = "/notify/:channelId",
  SSE           = "/notify/:channelId/sse",
  SUBSCRIBE     = "/notify/:channelId/subscribe",
  SUBSCRIPTIONS = "/notify/:channelId/subscriptions",
  SUBSCRIPTION  = "/notify/:channelId/subscriptions/:subscriptionId",
}


const CHANNEL_ID = ":channelId"
const SUBSCRIPTION_ID = ":subscriptionId"
const SUBSCRIBE_URI = "SUBSCRIBE_URI"


export function initNotifyEndpoints(server:   FastifyInstance,
                                    authz:    Authorization,
                                    channels: Channels) {
  server.get(
    PATHS.NOTIFY,
    authz.readPublic,
    handleGetNotify
  )

  server.get(
    PATHS.OPEN_CHANNEL,
    authz.readPublic,
    handleGetOpenChannel
  )

  server.post(
    PATHS.OPEN_CHANNEL, {
      ...authHeadersSchema,
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandlePostOpenChannel(channels)
  )

  server.get<{Params: ChannelPathParameters}>(
    PATHS.CHANNEL, {
      schema: {
        ...authHeadersSchema.schema,
        params: ChannelParamSchema
      },
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandleGetChannel(channels)
  )

  server.delete<{Params: ChannelPathParameters}>(
    PATHS.CHANNEL, {
      schema: {
        ...authHeadersSchema.schema,
        params: ChannelParamSchema
      },
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandleDeleteChannel(channels)
  )

  server.get<{Headers:  ChannelSSEHeaders,
              Params:   ChannelPathParameters}>(
    PATHS.SSE, {
      schema: {
        headers:  ChannelSSEHeadersSchema,
        params:   ChannelParamSchema
      },
      /*
        TODO can Browser send Authentication header? If not, will need:
          1. access_token query param
          2. Set a cookie and instruct browser to set 'withCredentials: true' in EventSource constructor.
             This requires Evently to set a cookie with the access token.
      */
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandleGetChannelSSE(channels)
  )

  server.get<{Params: ChannelPathParameters}>(
    PATHS.SUBSCRIBE, {
      schema: {
        ...authHeadersSchema.schema,
        params: ChannelParamSchema
      },
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandleGetSubscribe(channels)
  )

  server.post<{Params: ChannelPathParameters,
               Body: SelectorSubscriptionForm}>(
    PATHS.SUBSCRIBE, {
      schema: {
        ...authHeadersSchema.schema,
        params: ChannelParamSchema,
        body:   SelectorSubscriptionFormSchema
      },
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandlePostSubscribe(channels)
  )

  server.get<{Params: ChannelPathParameters}>(
    PATHS.SUBSCRIPTIONS, {
      schema: {
        ...authHeadersSchema.schema,
        params: ChannelParamSchema
      },
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandleGetSubscriptions(channels)
  )

  server.get<{Params: ChannelSubscriptionPathParameters}>(
    PATHS.SUBSCRIPTION, {
      schema: {
        ...authHeadersSchema.schema,
        params: ChannelSubscriptionParamsSchema
      },
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandleGetSubscription(channels)
  )

  server.delete<{Params: ChannelSubscriptionPathParameters}>(
    PATHS.SUBSCRIPTION, {
      schema: {
        ...authHeadersSchema.schema,
        params: ChannelSubscriptionParamsSchema
      },
      preHandler: authz.allowed(Permission.SubscribeNotifications)
    },
    initHandleDeleteSubscription(channels)
  )
}


const getNotifyResponse: HALObject = {
  title: "Notification API",
  description: "Open notification channels and subscribe to notifications for selectors that match new events. Utilizes Server-Sent Events.",
  _links: {
    "open-channel": {
      title: "Open a new channel",
      href: PATHS.OPEN_CHANNEL,
      profile: linkProfile(L3_PROFILES.ACTION)
    }
  }
}


const formattedNotifyResponse = formattedJSON(getNotifyResponse)

function handleGetNotify(request: FastifyRequest,
                         reply:   FastifyReply) {
  // CONSIDER: Admin clients see the list of channels
  reply
    .type(CONTENT_TYPES.HAL)
    .header(HEADERS.PROFILE, L3_PROFILES.HOME)
    .send(formattedNotifyResponse)
}


const openChannelResponse = {
  description: "POST an empty body to this resource to open a notification channel.",
  _links: {
    "open-action": {
      title:    "Open a new channel",
      href:     PATHS.OPEN_CHANNEL,
      profile:  linkProfile(L3_PROFILES.ACTION)
    }
  }
}

const formattedOpenChannelResponse = formattedJSON(openChannelResponse)

function handleGetOpenChannel(request: FastifyRequest,
                              reply:   FastifyReply) {
  reply
    .type(CONTENT_TYPES.HAL)
    .header(HEADERS.PROFILE, L3_PROFILES.ACTION)
    .send(formattedOpenChannelResponse)
}


function initHandlePostOpenChannel(channels: Channels) {
  return (request:  FastifyRequest,
          reply:    FastifyReply) => {
    const ledger = getLedgerFromRequestCtx(request)
    const channelId = channels.open(ledger)
    const newChannelUri = PATHS.CHANNEL.replace(CHANNEL_ID, channelId)

    return reply
      .status(201)
      .header(HEADERS.LOCATION, newChannelUri)
      .send(`New Channel created: ${newChannelUri}`)
  }
}


function initHandleGetChannel(channels: Channels) {
  return (request:  FastifyRequest<{Params: ChannelPathParameters}>,
          reply:    FastifyReply) => {
    const channelId = verifyChannelId(channels, request)
    const getChannelResponse: HALObject = {
      _links: {
        subscribe: {
          title:    "Subscribe to selector notifications in this channel",
          href:     PATHS.SUBSCRIBE.replace(CHANNEL_ID, channelId),
          profile:  linkProfile(L3_PROFILES.FORM)
        },
        subscriptions: {
          title:    "Selectors currently subscribed to on this channel",
          href:     PATHS.SUBSCRIPTIONS.replace(CHANNEL_ID, channelId),
          profile:  linkProfile(L3_PROFILES.INFO)
        },
        stream: {
          title:    "Notification stream, provided with Server-Sent Events. See https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events",
          href:     PATHS.SSE.replace(CHANNEL_ID, channelId)
        }
      }
    }
    reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, L3_PROFILES.NEXUS)
      .send(formattedJSON(getChannelResponse))
  }
}


function initHandleDeleteChannel(channels: Channels) {
  return async (request:  FastifyRequest<{Params: ChannelPathParameters}>,
                reply:    FastifyReply) => {
    const channelId = verifyChannelId(channels, request)
    const ledger = getLedgerFromRequestCtx(request)

    await channels.close(ledger, channelId)
    return reply
      .status(204)
      .send()
  }
}


function initHandleGetChannelSSE(channels: Channels) {
  return async (request:  FastifyRequest<{Headers:  ChannelSSEHeaders,
                                          Params:   ChannelPathParameters}>,
                reply:    FastifyReply) => {
    const {
      headers,
      params: {
        channelId
      }
    } = request
    const lastEventId = headers[HEADERS.LAST_EVENT_ID]
    const ledger = getLedgerFromRequestCtx(request)
    const eventStream = channels.openEventStream(ledger, channelId, lastEventId)
    reply.sse(eventStream)
  }
}


function initHandleGetSubscribe(channels: Channels) {
  return (request:  FastifyRequest<{Params: ChannelPathParameters}>,
          reply:    FastifyReply) => {
    const channelId = verifyChannelId(channels, request)
    const subscribeUri = PATHS.SUBSCRIBE.replace(CHANNEL_ID, channelId)
    const getSubscribeResponse = {
      ...SelectorSubscriptionFormSchema,
      description: SelectorSubscriptionFormSchema.description.replace(SUBSCRIBE_URI, subscribeUri)
    }
    const formattedGetSubscribeResponse = formattedJSON(getSubscribeResponse)
    const subscriptionsUri = PATHS.SUBSCRIPTIONS.replace(CHANNEL_ID, channelId)
    reply
      .type(CONTENT_TYPES.JSON_SCHEMA)
      .headers({
        [HEADERS.PROFILE]: [
          L3_PROFILES.FORM,
          L3_PATTERNS.ADD_ENTRY_RESOURCE
        ],
        [HEADERS.LINK]: `<${subscriptionsUri}>; rel="${L3_RELS.ADDS_TO_LIST}"; title="List of selector subscriptions for this channel"`
      })
      .send(formattedGetSubscribeResponse)
  }
}


function initHandlePostSubscribe(channels: Channels) {
  return async (request:  FastifyRequest<{Params: ChannelPathParameters,
                                          Body:   SelectorSubscriptionForm}>,
                reply:    FastifyReply) => {
    const {
      params: {
        channelId
      },
      body: subForm
    } = request
    const ledger = getLedgerFromRequestCtx(request)
    const selector = {
      ...subForm,
      after: maybeFromEventIdString(subForm.after)
    }
    const subscriptionId = channels.subscribe(ledger, channelId, selector)
    const subscriptionUri = PATHS.SUBSCRIPTION
      .replace(CHANNEL_ID, channelId)
      .replace(SUBSCRIPTION_ID, subscriptionId)
    return reply
      .status(201)
      .header(HEADERS.LOCATION, subscriptionUri)
      .send()
  }
}

function initHandleGetSubscriptions(channels: Channels) {
  return async (request:  FastifyRequest<{Params: ChannelPathParameters}>,
                reply:    FastifyReply) => {
    const {
      params: {
        channelId
      }
    } = request

    const ledger = getLedgerFromRequestCtx(request)
    const subIds = channels.subscriptions(ledger, channelId)
    const subscriptionUriTemplate = PATHS.SUBSCRIPTION.replace(CHANNEL_ID, channelId)
    const subLinks: LinkObject[] = subIds
      .map((subId) => ({
        href:     subscriptionUriTemplate.replace(SUBSCRIPTION_ID, subId),
        profile:  linkProfile(L3_PROFILES.DATA)
    }))

    const subscriptionsResponse: HALObject = {
      _links: {
        [L3_RELS.LIST_ENTRY]: subLinks,
        [L3_RELS.ADD_ENTRY]:  {
          title:    "Selector subscription form",
          href:     PATHS.SUBSCRIBE.replace(CHANNEL_ID, channelId),
          profile:  linkProfile(L3_PROFILES.FORM)
        },
        stream: {
          title:  "Notification stream, provided with Server-Sent Events. See https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events",
          href:   PATHS.SSE.replace(CHANNEL_ID, channelId)
        }
      }
    }
    reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, [
        L3_PROFILES.INFO,
        L3_PATTERNS.LIST_RESOURCE
      ])
      .send(formattedJSON(subscriptionsResponse))
  }
}


function initHandleGetSubscription(channels: Channels) {
  return async (request:  FastifyRequest<{Params: ChannelSubscriptionPathParameters}>,
                reply:    FastifyReply) => {
    const {
      params: {
        channelId,
        subscriptionId
      }
    } = request
    const ledger = getLedgerFromRequestCtx(request)
    const selector = channels.subscription(ledger, channelId, subscriptionId)
    if (!selector) {
      throw createHttpError.NotFound(`No subscription for channel ${channelId}, subscription ${subscriptionId}`)
    }
    const selectorType = isFilterSelector(selector)
      ? "Event Filter"
      : "All Events"
    const subscriptionResponse: HALObject = {
      description: "The subscription's selector that triggers new notifications. Use http DELETE request to this URI to cancel.",
      id: subscriptionId,
      selectorType,
      selector,
      _links: {
        channel: {
          title:    "Channel this subscription belongs to",
          href:     PATHS.CHANNEL.replace(CHANNEL_ID, channelId),
          profile:  linkProfile(L3_PROFILES.NEXUS)
        }
      }
    }
    reply
      .type(CONTENT_TYPES.HAL)
      .header(HEADERS.PROFILE, [
        L3_PROFILES.DATA,
        L3_PATTERNS.ENTRY_RESOURCE
      ])
      .send(formattedJSON(subscriptionResponse))
  }
}

function initHandleDeleteSubscription(channels: Channels) {
  return async (request: FastifyRequest<{Params: ChannelSubscriptionPathParameters}>,
                reply: FastifyReply) => {
    const {
      params: {
        channelId,
        subscriptionId
      }
    } = request
    const ledger = getLedgerFromRequestCtx(request)
    channels.unsubscribe(ledger, channelId, subscriptionId)

    return reply
      .status(204)
      .send()
  }
}


function verifyChannelId(channels: Channels, request: FastifyRequest<{Params: ChannelPathParameters}>) {
  const {
    params: {
      channelId
    }
  } = request
  const ledger = getLedgerFromRequestCtx(request)

  if (!channels.exists(ledger, channelId)) {
    throw createHttpError.NotFound(`channel '${channelId}' not found.`)
  }
  return channelId
}
