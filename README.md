# Probability Amplifier (v2, Rust) — Realtime Brainstorm Helper

Multi-room brainstorming chat with LLM analysis, built with:

- **Rust** + Axum + WebSockets
- **PubNub Rust Core SDK 0.8** (vendored + patched in `vendor/pubnub`) for publish / subscribe
- **PubNub REST** for signals (typing) and message actions (reactions + read receipts)

The upstream 0.8.0 subscribe deserializer breaks on signals/message-actions (`"e": 1|3`). See `vendor/pubnub/PATCHES.md`.

Guided by PubNub MCP `write_pubnub_app` (channel naming, message envelopes, security hygiene).

## Features

- Multiple rooms (`probability-amplifier.room.<slug>`)
- Live messages only (no Presence, no Message Persistence)
- Typing indicators (signals)
- Read receipts (message actions `receipt/read`)
- Reactions (message actions `reaction/<emoji>`)
- LLM brainstorm analysis (OpenAI or local fallback)

## Run

```bash
cd v2
cp .env.example .env   # keys already filled if you kept .env
cargo run
```

Open **http://127.0.0.1:8080**

If **Access Manager** is enabled on the keyset (403 Forbidden on subscribe), set:

```bash
PUBNUB_SECRET_KEY=sec-c-...
```

The hub grants itself a 24h token for `^brain\.room\..*` on startup.

Optional LLM:

```bash
OPENAI_API_KEY=sk-...
```

## Architecture

| Piece | Tech |
| --- | --- |
| UI | Static HTML/CSS/JS served by Axum |
| Realtime bridge | WebSocket ↔ PubNub hub |
| Pub/Sub | `pubnub` crate |
| Signals / actions / App Context | REST helpers in `src/rest.rs` |
| Room catalog | App Context channel metadata (`probability-amplifier.room.*`), synced from BizOps |
| Analysis | `src/llm.rs` |

## Notes

Rust SDK currently documents publish/subscribe/access — not message-action writers — so those use REST while still receiving action/signal events on the subscribe stream. Publishes set `store: false`. Analyze reads an in-memory transcript of messages this process has seen live, capped at 50.
