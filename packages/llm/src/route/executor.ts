import { Cause, Clock, Context, Effect, Exit, Layer, Random } from "effect"
import {
  FetchHttpClient,
  Headers,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http"
import {
  AuthenticationReason,
  ContentPolicyReason,
  HttpContext,
  HttpRateLimitDetails,
  HttpRequestDetails,
  HttpResponseDetails,
  InvalidRequestReason,
  LLMError,
  ProviderInternalReason,
  QuotaExceededReason,
  RateLimitReason,
  TransportReason,
  UnknownProviderReason,
} from "../schema"
import { isContextOverflow } from "../provider-error"

export interface Interface {
  readonly execute: (
    request: HttpClientRequest.HttpClientRequest,
  ) => Effect.Effect<HttpClientResponse.HttpClientResponse, LLMError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/LLM/RequestExecutor") {}

export type AttemptOrdinal = "first" | "second" | "third_or_later"
export type AttemptStatusFamily = "none" | "1xx" | "2xx" | "3xx" | "4xx" | "5xx" | "other"
export type AttemptErrorKind =
  | "authentication"
  | "content_policy"
  | "invalid_provider_output"
  | "invalid_request"
  | "no_route"
  | "provider_internal"
  | "quota_exceeded"
  | "rate_limit"
  | "transport"
  | "unknown_provider"
  | "unknown"

export type AttemptEvent =
  | {
      readonly type: "started"
      readonly attempt: AttemptOrdinal
    }
  | {
      readonly type: "completed"
      readonly attempt: AttemptOrdinal
      readonly outcome: "success"
      readonly durationMs: number
      readonly statusFamily: AttemptStatusFamily
      readonly willRetry: false
    }
  | {
      readonly type: "completed"
      readonly attempt: AttemptOrdinal
      readonly outcome: "failed"
      readonly durationMs: number
      readonly statusFamily: AttemptStatusFamily
      readonly errorKind: AttemptErrorKind
      readonly retryable: boolean
      readonly willRetry: boolean
    }
  | {
      readonly type: "completed"
      readonly attempt: AttemptOrdinal
      readonly outcome: "cancelled"
      readonly durationMs: number
      readonly statusFamily: "none"
      readonly willRetry: false
    }
  | {
      readonly type: "retry-scheduled"
      readonly attempt: AttemptOrdinal
      readonly nextAttempt: AttemptOrdinal
      readonly delayMs: number
      readonly delaySource: "retry_after" | "backoff"
    }

export type AttemptObserver = (event: AttemptEvent) => Effect.Effect<void>

/**
 * Optional, fiber-local observation of physical HTTP attempts owned by this
 * executor. One `started`/`completed` pair is emitted per transport request;
 * `retry-scheduled` is emitted after a retryable failure and before its delay.
 * A successful attempt completes when response headers and status are
 * available; streaming body consumption remains owned by the route parser.
 * Events intentionally expose only bounded diagnostic dimensions, never
 * request URLs, headers, bodies, request IDs, or provider error messages.
 */
export const CurrentAttemptObserver = Context.Reference<AttemptObserver | undefined>(
  "@opencode/LLM/RequestExecutor/CurrentAttemptObserver",
  { defaultValue: () => undefined },
)

const BODY_LIMIT = 16_384
const MAX_RETRIES = 2
const BASE_DELAY_MS = 500
const MAX_DELAY_MS = 10_000
const REDACTED = "<redacted>"

// One source of truth for what counts as a sensitive name across headers,
// URL query keys, and field names embedded inside request/response bodies.
//
// `SENSITIVE_NAME` is used as both a substring matcher (for free-form header
// names like `Authorization` / `X-API-Key`) and as the body-field alternation
// list. `SHORT_QUERY_NAME` covers anchored short keys like `?key=…` / `?sig=…`
// that are too generic to redact substring-style without false positives.
const SENSITIVE_NAME_SOURCE =
  "authorization|api[-_]?key|access[-_]?token|refresh[-_]?token|id[-_]?token|token|secret|credential|signature|x-amz-signature"
const SENSITIVE_NAME = new RegExp(SENSITIVE_NAME_SOURCE, "i")
const SHORT_QUERY_NAME = /^(key|sig)$/i
const SENSITIVE_BODY_FIELD = new RegExp(`(?:${SENSITIVE_NAME_SOURCE}|key)`, "i")
const REDACT_JSON_FIELD = new RegExp(`("(?:${SENSITIVE_BODY_FIELD.source})"\\s*:\\s*)"[^"]*"`, "gi")
const REDACT_QUERY_FIELD = new RegExp(`((?:${SENSITIVE_BODY_FIELD.source})=)[^&\\s"]+`, "gi")

const isSensitiveHeaderName = (name: string) => SENSITIVE_NAME.test(name)

const isSensitiveQueryName = (name: string) => isSensitiveHeaderName(name) || SHORT_QUERY_NAME.test(name)

const redactHeaders = (headers: Headers.Headers, redactedNames: ReadonlyArray<string | RegExp>) =>
  Object.fromEntries(
    Object.entries(Headers.redact(headers, [...redactedNames, SENSITIVE_NAME])).map(([name, value]) => [
      name,
      String(value),
    ]),
  )

const redactUrl = (value: string) => {
  if (!URL.canParse(value)) return REDACTED
  const url = new URL(value)
  url.searchParams.forEach((_, key) => {
    if (isSensitiveQueryName(key)) url.searchParams.set(key, REDACTED)
  })
  return url.toString()
}

const normalizedHeaders = (headers: Headers.Headers) =>
  Object.fromEntries(Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value]))

const requestId = (headers: Record<string, string>) => {
  return (
    headers["x-request-id"] ??
    headers["request-id"] ??
    headers["x-amzn-requestid"] ??
    headers["x-amz-request-id"] ??
    headers["x-goog-request-id"] ??
    headers["cf-ray"]
  )
}

const retryableStatus = (status: number) => status === 429 || status === 503 || status === 504 || status === 529

const retryAfterMs = (headers: Record<string, string>) => {
  const millis = Number(headers["retry-after-ms"])
  if (Number.isFinite(millis)) return Math.max(0, millis)

  const value = headers["retry-after"]
  if (!value) return undefined

  const seconds = Number(value)
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000)

  const date = Date.parse(value)
  if (!Number.isNaN(date)) return Math.max(0, date - Date.now())
  return undefined
}

const addRateLimitValue = (target: Record<string, string>, key: string, value: string) => {
  if (key.length > 0) target[key] = value
}

const rateLimitDetails = (headers: Record<string, string>, retryAfter: number | undefined) => {
  const limit: Record<string, string> = {}
  const remaining: Record<string, string> = {}
  const reset: Record<string, string> = {}

  Object.entries(headers).forEach(([name, value]) => {
    const openaiLimit = /^x-ratelimit-limit-(.+)$/.exec(name)?.[1]
    if (openaiLimit) return addRateLimitValue(limit, openaiLimit, value)

    const openaiRemaining = /^x-ratelimit-remaining-(.+)$/.exec(name)?.[1]
    if (openaiRemaining) return addRateLimitValue(remaining, openaiRemaining, value)

    const openaiReset = /^x-ratelimit-reset-(.+)$/.exec(name)?.[1]
    if (openaiReset) return addRateLimitValue(reset, openaiReset, value)

    const anthropic = /^anthropic-ratelimit-(.+)-(limit|remaining|reset)$/.exec(name)
    if (!anthropic) return
    if (anthropic[2] === "limit") return addRateLimitValue(limit, anthropic[1], value)
    if (anthropic[2] === "remaining") return addRateLimitValue(remaining, anthropic[1], value)
    return addRateLimitValue(reset, anthropic[1], value)
  })

  if (
    retryAfter === undefined &&
    Object.keys(limit).length === 0 &&
    Object.keys(remaining).length === 0 &&
    Object.keys(reset).length === 0
  )
    return undefined

  return new HttpRateLimitDetails({
    retryAfterMs: retryAfter,
    limit: Object.keys(limit).length === 0 ? undefined : limit,
    remaining: Object.keys(remaining).length === 0 ? undefined : remaining,
    reset: Object.keys(reset).length === 0 ? undefined : reset,
  })
}

const requestDetails = (request: HttpClientRequest.HttpClientRequest, redactedNames: ReadonlyArray<string | RegExp>) =>
  new HttpRequestDetails({
    method: request.method,
    url: redactUrl(request.url),
    headers: redactHeaders(request.headers, redactedNames),
  })

const responseDetails = (
  response: HttpClientResponse.HttpClientResponse,
  redactedNames: ReadonlyArray<string | RegExp>,
) =>
  new HttpResponseDetails({
    status: response.status,
    headers: redactHeaders(response.headers, redactedNames),
  })

const secretValues = (request: HttpClientRequest.HttpClientRequest) => {
  const values = new Set<string>()
  const add = (value: string) => {
    if (value.length < 4) return
    values.add(value)
    values.add(encodeURIComponent(value))
  }

  Object.entries(request.headers).forEach(([name, value]) => {
    if (!isSensitiveHeaderName(name)) return
    add(value)
    const bearer = /^Bearer\s+(.+)$/i.exec(value)?.[1]
    if (bearer) add(bearer)
  })

  if (!URL.canParse(request.url)) return values
  new URL(request.url).searchParams.forEach((value, key) => {
    if (isSensitiveQueryName(key)) add(value)
  })
  return values
}

// Two passes: structural (redact `"name": "value"` and `name=value` patterns
// for any field name that looks sensitive) plus literal (replace any actual
// secret values we sent in the request, in case the response echoes one back).
const redactBody = (body: string, request: HttpClientRequest.HttpClientRequest) =>
  Array.from(secretValues(request)).reduce(
    (text, secret) => text.split(secret).join(REDACTED),
    body.replace(REDACT_JSON_FIELD, `$1"${REDACTED}"`).replace(REDACT_QUERY_FIELD, `$1${REDACTED}`),
  )

const responseBody = (body: string | void, request: HttpClientRequest.HttpClientRequest) => {
  if (body === undefined) return {}
  const redacted = redactBody(body, request)
  if (redacted.length <= BODY_LIMIT) return { body: redacted }
  return { body: redacted.slice(0, BODY_LIMIT), bodyTruncated: true }
}

const providerMessage = (status: number, body: { readonly body?: string }) => {
  if (body.body && body.body.length <= 500) return `Provider request failed with HTTP ${status}: ${body.body}`
  return `Provider request failed with HTTP ${status}`
}

const responseHttp = (input: {
  readonly request: HttpClientRequest.HttpClientRequest
  readonly response: HttpClientResponse.HttpClientResponse
  readonly redactedNames: ReadonlyArray<string | RegExp>
  readonly body: ReturnType<typeof responseBody>
  readonly requestId?: string | undefined
  readonly rateLimit?: HttpRateLimitDetails | undefined
}) =>
  new HttpContext({
    request: requestDetails(input.request, input.redactedNames),
    response: responseDetails(input.response, input.redactedNames),
    ...input.body,
    requestId: input.requestId,
    rateLimit: input.rateLimit,
  })

const statusReason = (input: {
  readonly status: number
  readonly message: string
  readonly retryAfterMs?: number | undefined
  readonly rateLimit?: HttpRateLimitDetails | undefined
  readonly http: HttpContext
}) => {
  const body = input.http.body ?? ""
  if (/content[-_\s]?policy|content_filter|safety/i.test(body)) {
    return new ContentPolicyReason({ message: input.message, http: input.http })
  }
  if (input.status === 401) {
    return new AuthenticationReason({ message: input.message, kind: "invalid", http: input.http })
  }
  if (input.status === 403) {
    return new AuthenticationReason({ message: input.message, kind: "insufficient-permissions", http: input.http })
  }
  if (input.status === 429) {
    if (/insufficient[-_\s]?quota|quota[-_\s]?exceeded/i.test(body)) {
      return new QuotaExceededReason({ message: input.message, http: input.http })
    }
    return new RateLimitReason({
      message: input.message,
      retryAfterMs: input.retryAfterMs,
      rateLimit: input.rateLimit,
      http: input.http,
    })
  }
  if (
    input.status === 400 ||
    input.status === 404 ||
    input.status === 409 ||
    input.status === 413 ||
    input.status === 422
  ) {
    return new InvalidRequestReason({
      message: input.message,
      classification: isContextOverflow(body) ? "context-overflow" : undefined,
      http: input.http,
    })
  }
  if (input.status >= 500 || retryableStatus(input.status)) {
    return new ProviderInternalReason({
      message: input.message,
      status: input.status,
      retryAfterMs: input.retryAfterMs,
      http: input.http,
    })
  }
  return new UnknownProviderReason({ message: input.message, status: input.status, http: input.http })
}

const statusError =
  (request: HttpClientRequest.HttpClientRequest, redactedNames: ReadonlyArray<string | RegExp>) =>
  (response: HttpClientResponse.HttpClientResponse) =>
    Effect.gen(function* () {
      if (response.status < 400) return response
      const body = yield* response.text.pipe(Effect.catch(() => Effect.void))
      const headers = normalizedHeaders(response.headers)
      const retryAfter = retryAfterMs(headers)
      const rateLimit = rateLimitDetails(headers, retryAfter)
      const details = responseBody(body, request)
      return yield* new LLMError({
        module: "RequestExecutor",
        method: "execute",
        reason: statusReason({
          status: response.status,
          message: providerMessage(response.status, details),
          retryAfterMs: retryAfter,
          rateLimit,
          http: responseHttp({
            request,
            response,
            redactedNames,
            body: details,
            requestId: requestId(headers),
            rateLimit,
          }),
        }),
      })
    })

const toHttpError = (redactedNames: ReadonlyArray<string | RegExp>) => (error: unknown) => {
  const transportError = (input: {
    readonly message: string
    readonly kind?: string | undefined
    readonly request?: HttpClientRequest.HttpClientRequest | undefined
  }) =>
    new LLMError({
      module: "RequestExecutor",
      method: "execute",
      reason: new TransportReason({
        message: input.message,
        kind: input.kind,
        url: input.request ? redactUrl(input.request.url) : undefined,
        http: input.request ? new HttpContext({ request: requestDetails(input.request, redactedNames) }) : undefined,
      }),
    })

  if (Cause.isTimeoutError(error)) {
    return transportError({ message: error.message, kind: "Timeout" })
  }
  if (!HttpClientError.isHttpClientError(error)) {
    return transportError({ message: "HTTP transport failed" })
  }
  const request = "request" in error ? error.request : undefined
  if (error.reason._tag === "TransportError") {
    return transportError({
      message: error.reason.description ?? "HTTP transport failed",
      kind: error.reason._tag,
      request,
    })
  }
  return transportError({
    message: `HTTP transport failed: ${error.reason._tag}`,
    kind: error.reason._tag,
    request,
  })
}

const retryDelay = (error: LLMError, attempt: number) => {
  if (error.retryAfterMs !== undefined) return Effect.succeed(Math.min(error.retryAfterMs, MAX_DELAY_MS))
  return Random.nextBetween(
    Math.min(BASE_DELAY_MS * 2 ** attempt * 0.8, MAX_DELAY_MS),
    Math.min(BASE_DELAY_MS * 2 ** attempt * 1.2, MAX_DELAY_MS),
  ).pipe(Effect.map((delay) => Math.round(delay)))
}

const attemptOrdinal = (attempt: number): AttemptOrdinal => {
  if (attempt <= 0) return "first"
  if (attempt === 1) return "second"
  return "third_or_later"
}

const statusFamily = (status: number | undefined): AttemptStatusFamily => {
  if (status === undefined) return "none"
  if (status >= 100 && status < 200) return "1xx"
  if (status >= 200 && status < 300) return "2xx"
  if (status >= 300 && status < 400) return "3xx"
  if (status >= 400 && status < 500) return "4xx"
  if (status >= 500 && status < 600) return "5xx"
  return "other"
}

const errorStatus = (error: LLMError) => {
  if ("http" in error.reason) return error.reason.http?.response?.status
  if ("status" in error.reason) return error.reason.status
  return undefined
}

const errorKind = (error: LLMError): AttemptErrorKind => {
  switch (error.reason._tag) {
    case "Authentication":
      return "authentication"
    case "ContentPolicy":
      return "content_policy"
    case "InvalidProviderOutput":
      return "invalid_provider_output"
    case "InvalidRequest":
      return "invalid_request"
    case "NoRoute":
      return "no_route"
    case "ProviderInternal":
      return "provider_internal"
    case "QuotaExceeded":
      return "quota_exceeded"
    case "RateLimit":
      return "rate_limit"
    case "Transport":
      return "transport"
    case "UnknownProvider":
      return "unknown_provider"
    default:
      return "unknown"
  }
}

const notify = (observer: AttemptObserver | undefined, event: AttemptEvent) => {
  if (!observer) return Effect.void
  return observer(event).pipe(
    Effect.catchCause((cause) => (Cause.hasInterrupts(cause) ? Effect.failCause(cause) : Effect.void)),
  )
}

const retryStatusFailures = <R>(
  effect: Effect.Effect<HttpClientResponse.HttpClientResponse, LLMError, R>,
  retries = MAX_RETRIES,
  attempt = 0,
): Effect.Effect<HttpClientResponse.HttpClientResponse, LLMError, R> =>
  Effect.gen(function* () {
    const observer = yield* CurrentAttemptObserver
    const ordinal = attemptOrdinal(attempt)
    yield* notify(observer, { type: "started", attempt: ordinal })
    const started = yield* Clock.currentTimeMillis
    const exit = yield* Effect.exit(effect).pipe(
      Effect.onExit((result) => {
        if (Exit.isSuccess(result) || !Cause.hasInterrupts(result.cause)) return Effect.void
        return Clock.currentTimeMillis.pipe(
          Effect.flatMap((ended) =>
            notify(observer, {
              type: "completed",
              attempt: ordinal,
              outcome: "cancelled",
              durationMs: Math.max(0, ended - started),
              statusFamily: "none",
              willRetry: false,
            }),
          ),
        )
      }),
    )
    const durationMs = Math.max(0, (yield* Clock.currentTimeMillis) - started)

    if (Exit.isSuccess(exit)) {
      yield* notify(observer, {
        type: "completed",
        attempt: ordinal,
        outcome: "success",
        durationMs,
        statusFamily: statusFamily(exit.value.status),
        willRetry: false,
      })
      return exit.value
    }

    if (Cause.hasInterrupts(exit.cause)) {
      yield* notify(observer, {
        type: "completed",
        attempt: ordinal,
        outcome: "cancelled",
        durationMs,
        statusFamily: "none",
        willRetry: false,
      })
      return yield* Effect.failCause(exit.cause)
    }

    const failure = exit.cause.reasons.find(Cause.isFailReason)?.error
    if (!(failure instanceof LLMError)) {
      yield* notify(observer, {
        type: "completed",
        attempt: ordinal,
        outcome: "failed",
        durationMs,
        statusFamily: "none",
        errorKind: "unknown",
        retryable: false,
        willRetry: false,
      })
      return yield* Effect.failCause(exit.cause)
    }

    const willRetry = failure.retryable && retries > 0
    yield* notify(observer, {
      type: "completed",
      attempt: ordinal,
      outcome: "failed",
      durationMs,
      statusFamily: statusFamily(errorStatus(failure)),
      errorKind: errorKind(failure),
      retryable: failure.retryable,
      willRetry,
    })
    if (!willRetry) return yield* Effect.failCause(exit.cause)

    const delay = yield* retryDelay(failure, attempt)
    yield* notify(observer, {
      type: "retry-scheduled",
      attempt: ordinal,
      nextAttempt: attemptOrdinal(attempt + 1),
      delayMs: delay,
      delaySource: failure.retryAfterMs === undefined ? "backoff" : "retry_after",
    })
    yield* Effect.sleep(delay)
    return yield* retryStatusFailures(effect, retries - 1, attempt + 1)
  })

export const layer: Layer.Layer<Service, never, HttpClient.HttpClient> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient
    const executeOnce = (request: HttpClientRequest.HttpClientRequest) =>
      Effect.gen(function* () {
        const redactedNames = yield* Headers.CurrentRedactedNames
        return yield* http
          .execute(request)
          .pipe(Effect.mapError(toHttpError(redactedNames)), Effect.flatMap(statusError(request, redactedNames)))
      })
    return Service.of({
      execute: (request) => retryStatusFailures(executeOnce(request)),
    })
  }),
)

export const fetchLayer = layer.pipe(Layer.provide(FetchHttpClient.layer))

export * as RequestExecutor from "./executor"
