export enum REQUEST_CTX {
  LEDGER = "ledger",
  ROLES = "roles"
}

export enum HEADERS {
  AUTHORIZATION       = "authorization",
  CONTENT_TYPE        = "content-type",
  CACHE_CONTROL       = "cache-control",
  CSP                 = "content-security-policy",
  CONTENT_LOCATION    = "content-location",
  HSTS                = "strict-transport-security",
  IF_NONE_MATCH       = "if-none-match",
  LAST_EVENT_ID       = "last-event-id",
  LINK                = "link",
  LOCATION            = "location",
  PREFER              = "prefer",
  PREFERENCE_APPLIED  = "preference-applied",
  PROFILE             = "profile",
  WWW_AUTHENTICATE    = "www-authenticate",
  XCTO                = "x-content-type-options"
}

export enum CONTENT_TYPES {
  JSON        = "application/json; charset=utf-8",
  JSON_SCHEMA = "application/schema+json; charset=utf-8",
  HAL         = "application/hal+json; charset=utf-8",
  ND_JSON     = "application/x-ndjson; charset=utf-8"
}

export enum L3_PATTERNS {
  LIST_RESOURCE      = "<https://level3.rest/patterns/list#list-resource>",
  ADD_ENTRY_RESOURCE = "<https://level3.rest/patterns/list#add-entry-resource>",
  ENTRY_RESOURCE     = "<https://level3.rest/patterns/list#entry-resource>"
}

export enum L3_PROFILES {
  ACTION          = "<https://level3.rest/profiles/action>",
  DATA            = "<https://level3.rest/profiles/data>",
  ENTITY          = "<https://level3.rest/profiles/mixins/entity>",
  FORM            = "<https://level3.rest/profiles/form>",
  INFO            = "<https://level3.rest/profiles/info>",
  HOME            = "<https://level3.rest/profiles/home>",
  LOOKUP          = "<https://level3.rest/profiles/lookup>",
  NEXUS           = "<https://level3.rest/profiles/nexus>",
  REPRESENTATION  = "<https://level3.rest/profiles/mixins/representation>"
}

export enum L3_RELS {
  ADD_ENTRY     = "https://level3.rest/patterns/list/editable#add-entry",
  ADDS_TO_LIST  = "https://level3.rest/patterns/list/editable#adds-to-list",
  FORM          = "https://level3.rest/profiles/form",
  LIST          = "https://level3.rest/patterns/list#list",
  LIST_ENTRY    = "https://level3.rest/patterns/list#list-entry",
  LOOKUP        = "https://level3.rest/profiles/lookup"
}
