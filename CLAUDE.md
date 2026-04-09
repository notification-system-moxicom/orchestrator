# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build ./...

# Run
go run ./cmd/api-gateway/main.go -c configs/config.yaml

# Lint
golangci-lint run
```

There are no tests yet.

## Architecture

Orchestrator is the **business logic layer** of the notification system. It consumes enriched notification payloads from Kafka (produced by api-gateway), applies templates, filters users by preferences, and routes delivery messages to channel-specific Kafka topics.

### Message flow

1. Consumes `SendNotificationPayload` from topic `send-notification-orchestrator`
2. Applies Go template if `scenario.template` is provided (simple `{{key}}` replacement)
3. Resolves delivery methods from `methods` field (fallback: email)
4. **Filters users per method**: removes users who disabled the channel via subscriptions
5. **Filters by quiet hours**: skips users whose current local time falls within their quiet hours window
6. Publishes `DeliveryPayload` to channel-specific topics (`delivery_email`, `delivery_telegram`, `delivery_sms`)

### Internal layers

```
cmd/api-gateway/main.go        entry point: config → kafka → handler → consumer
internal/
  config/                       YAML config struct
  types/
    enums.go                    DeliveryMethod, Operation constants
    messages.go                 SendNotificationPayload, DeliveryPayload, UserContact types
  kafka/
    service.go                  Sarama producer/consumer with SCRAM auth
    consumerhandler.go          Consumer group handler
    handler/gateway/
      init.go                   Consumer setup
      handler.go                Core processing: template rendering, user filtering, delivery routing
  validation/                   JSON schema message validator
  server/                       HTTP server (chi router, health endpoints)
  http/handlers/                HTTP handlers (stubs)
  errors/                       Typed errors
```

### User preference filtering

The orchestrator filters users based on preferences enriched into the Kafka payload by api-gateway:

- **Subscriptions**: each user may have `subscriptions[]` with `{method, enabled, priority}`. If a user has `enabled: false` for a method, they are excluded from that delivery. If no subscriptions are set, all methods are enabled (backwards compatible)
- **Quiet Hours**: each user may have `quiet_hours` with `{start, end, timezone}`. If current time in user's timezone falls within the window (supports overnight, e.g., 22:00–08:00), the user is skipped
- Filtering happens in `filterUsersByMethod()` — called per delivery method, returns only eligible users

### Configuration

Config is loaded from YAML (`configs/config.yaml`, override with `-c`). Key fields:
- `settings.operations_topics` — maps operation names to Kafka topics (e.g., `send_notification_orchestration`, `delivery_email`, `delivery_telegram`, `delivery_sms`)
- `connections.kafka.gateway` — Kafka broker config with SCRAM auth

### Key design decisions

- **No persistence calls** — orchestrator receives everything it needs in the Kafka payload (contacts, subscriptions, quiet hours, scenario/template)
- **Simple template engine** — `{{key}}` replacement from `template_params`, not full Go `text/template`
- **Per-method user filtering** — different users may receive different channels based on their subscriptions
- **Backwards compatible** — users without subscriptions receive all system-enabled methods