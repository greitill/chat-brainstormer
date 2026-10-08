# Local patches on pubnub 0.8.0

## SubscribeMessageType deserialize

Upstream derives `serde(untagged)` on the C-like `SubscribeMessageType` enum.
That cannot parse numeric wire values for `"e"` (`1` = signal, `3` = message
action), so a single action/signal aborts the subscribe receive loop with:

`Deserialization { details: "data did not match any variant of untagged enum SubscribeResponseBody" }`

Fix: deserialize from `u8` via an explicit `Deserialize` impl.

## MessageActionEvent wire values

`TryFrom<String> for MessageActionEvent` only accepted `"update"` / `"delete"`,
but PubNub sends `"added"` / `"removed"` — as upstream's own doc comment on
`SubscribeResponseBody::MessageActionUpdate::event` states. Every reaction and
read receipt therefore killed the subscribe receive loop with:

`Deserialization { details: "Unable deserialize: unexpected message action event type" }`

Fix: accept `added`/`removed` alongside the original spellings.
