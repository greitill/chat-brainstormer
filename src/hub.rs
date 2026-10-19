use crate::config::Config;
use crate::llm;
use crate::models::{
    avatar_initials, default_avatar_color, is_room_channel, room_channel, room_slug_from_channel,
    sanitize_avatar_color, slugify, ChatEnvelope, ChatMessage, RoomInfo, ServerEvent, UserProfile,
};
use crate::rest::PubNubRest;
use dashmap::DashMap;
use futures::StreamExt;
use pubnub::access::permissions;
use pubnub::dx::subscribe::Update;
use pubnub::subscribe::{EventEmitter, EventSubscriber, MessageActionEvent, SubscriptionParams};
use pubnub::{Keyset, PubNubClient, PubNubClientBuilder};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use uuid::Uuid;

const HUB_USER_ID: &str = "sparkline-hub";
/// How often to pull BizOps / App Context channel metadata into the room list.
const APP_CONTEXT_REFRESH_SECS: u64 = 20;
/// Access Manager token lifetime. Short on purpose: the hub re-grants on a timer,
/// so a leaked token stops working quickly.
const TOKEN_TTL_MINUTES: usize = 60;
/// Re-grant this far ahead of expiry, leaving room to retry before the old token dies.
const TOKEN_REFRESH_MARGIN_MINUTES: usize = 15;
/// Backoff between failed re-grants, while the current token is still valid.
const TOKEN_RETRY_SECS: u64 = 60;
/// How many live messages Analyze can see. This process keeps them in memory;
/// nothing is read back from Message Persistence.
const LIVE_TRANSCRIPT_CAP: usize = 50;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub rest: PubNubRest,
    pub client: Arc<PubNubClient>,
    pub rooms: Arc<DashMap<String, RoomState>>,
    pub subscribed: Arc<DashMap<String, ()>>,
    pub tx: broadcast::Sender<ServerEvent>,
    pub names: Arc<DashMap<String, String>>,
    /// In-memory cache of App Context profiles (initials + color).
    pub profiles: Arc<DashMap<String, UserProfile>>,
    /// Messages observed on the live subscribe stream since this process started.
    transcripts: Arc<DashMap<String, Vec<ChatMessage>>>,
}

#[derive(Debug, Clone)]
pub struct RoomState {
    pub slug: String,
    pub channel: String,
    pub name: String,
    pub topic: String,
}

impl AppState {
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let auth_token = if let Some(secret_key) = &config.secret_key {
            Some(grant_hub_token(&config, secret_key).await?)
        } else {
            tracing::warn!(
                "PUBNUB_SECRET_KEY not set — if Access Manager is enabled on this keyset, subscribe will return 403"
            );
            None
        };

        let client = PubNubClientBuilder::with_reqwest_transport()
            .with_keyset(Keyset {
                subscribe_key: config.subscribe_key.clone(),
                publish_key: Some(config.publish_key.clone()),
                secret_key: None,
            })
            .with_user_id(HUB_USER_ID)
            .build()?;

        if let Some(token) = &auth_token {
            client.set_token(token.clone());
            tracing::info!("Access Manager token applied to hub client");
        }

        // Headroom for typing bursts so slow clients rarely lag at all.
        let (tx, _) = broadcast::channel(4096);
        let rest = PubNubRest::new(
            config.publish_key.clone(),
            config.subscribe_key.clone(),
            auth_token.clone(),
            config.secret_key.clone(),
        );

        let state = Self {
            config,
            rest,
            client: Arc::new(client),
            rooms: Arc::new(DashMap::new()),
            subscribed: Arc::new(DashMap::new()),
            tx,
            names: Arc::new(DashMap::new()),
            profiles: Arc::new(DashMap::new()),
            transcripts: Arc::new(DashMap::new()),
        };

        // Defaults always present in-memory; App Context may override name/topic.
        state.ensure_room("general", "General", "Open brainstorming");
        state.ensure_room("product", "Product", "Product ideation");

        // Seed the defaults into App Context *before* loading from it: the load
        // prunes rooms App Context doesn't know about, which on a fresh keyset
        // would otherwise delete them before they were ever published.
        for slug in ["general", "product"] {
            if let Some(info) = state.room_info(slug) {
                if let Err(err) = state.persist_room_metadata(&info).await {
                    tracing::warn!("seed channel metadata for {slug} failed: {err}");
                }
            }
        }

        match state.refresh_rooms_from_app_context().await {
            Ok(n) => tracing::info!("Loaded {n} room(s) from App Context"),
            Err(err) => tracing::warn!("App Context room load failed: {err}"),
        }

        state.spawn_app_context_refresh_loop();
        state.spawn_token_refresh_loop();

        Ok(state)
    }

    /// Apply a token to both PubNub surfaces: the SDK client (subscribe, publish)
    /// and the REST helpers (signals, actions, App Context).
    fn apply_token(&self, token: String) {
        self.client.set_token(token.clone());
        self.rest.set_auth_token(Some(token));
    }

    /// Re-grant the hub's Access Manager token before it expires.
    ///
    /// Without this the token dies mid-session and every REST call starts failing
    /// with a 403 the UI can't explain, until someone restarts the process.
    fn spawn_token_refresh_loop(&self) {
        let Some(secret_key) = self.config.secret_key.clone() else {
            return;
        };
        let state = self.clone();
        let lead_time = Duration::from_secs(
            (TOKEN_TTL_MINUTES.saturating_sub(TOKEN_REFRESH_MARGIN_MINUTES) as u64) * 60,
        );

        // `grant_token` holds a `Vec<Box<dyn Permission>>` across its await, which is
        // neither `Send` nor `Sync`, so the grant future can't go to `tokio::spawn`.
        // Give the loop its own thread and current-thread runtime instead.
        std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    tracing::error!("token refresh runtime failed to start: {err}");
                    return;
                }
            };

            runtime.block_on(async move {
                loop {
                    tokio::time::sleep(lead_time).await;

                    loop {
                        match grant_hub_token(&state.config, &secret_key).await {
                            Ok(token) => {
                                state.apply_token(token);
                                tracing::info!("Access Manager token refreshed");
                                break;
                            }
                            Err(err) => {
                                tracing::warn!(
                                    "Access Manager token refresh failed, retrying in {TOKEN_RETRY_SECS}s: {err}"
                                );
                                tokio::time::sleep(Duration::from_secs(TOKEN_RETRY_SECS)).await;
                            }
                        }
                    }
                }
            });
        });
    }

    /// Upsert an in-memory room and subscribe to its pub/sub channel.
    pub fn ensure_room(&self, slug: &str, name: &str, topic: &str) -> RoomInfo {
        let slug = slugify(slug);
        let channel = room_channel(&slug);

        // Only take the write lock when something actually changes. The periodic
        // App Context refresh calls this for every known room, and an idle writer
        // would block every concurrent reader on the same shard.
        let needs_write = self
            .rooms
            .get(&slug)
            .map_or(true, |room| room.name != name || room.topic != topic);

        if needs_write {
            self.rooms
                .entry(slug.clone())
                .and_modify(|r| {
                    r.name = name.to_string();
                    r.topic = topic.to_string();
                })
                .or_insert_with(|| RoomState {
                    slug: slug.clone(),
                    channel: channel.clone(),
                    name: name.to_string(),
                    topic: topic.to_string(),
                });
        }

        self.subscribe_room(&channel);
        self.room_info(&slug).expect("room exists")
    }

    /// Resolve a room's channel, releasing the map guard before the caller awaits.
    ///
    /// Holding a `DashMap` guard across an `.await` deadlocks against any writer
    /// (see `ensure_room`), so every async path must copy what it needs first.
    fn channel_for(&self, room_slug: &str) -> Result<String, String> {
        self.rooms
            .get(room_slug)
            .map(|room| room.channel.clone())
            .ok_or_else(|| format!("Unknown room '{room_slug}'"))
    }

    /// Create/update room locally and write App Context metadata (BizOps).
    pub async fn create_room(&self, name: &str, topic: &str) -> Result<RoomInfo, String> {
        let slug = slugify(name);
        let info = self.ensure_room(&slug, name, topic);
        if let Err(err) = self.persist_room_metadata(&info).await {
            tracing::warn!(
                "App Context setChannelMetadata failed for {}: {err}",
                info.channel
            );
            // App Context is the room catalog: without the record the next refresh
            // prunes this room anyway, so don't hand the client a phantom entry.
            self.rooms.remove(&slug);
            return Err(format!(
                "Could not create '{}' — {err}",
                truncate_for_display(name, 40)
            ));
        }
        Ok(info)
    }

    pub async fn persist_room_metadata(&self, info: &RoomInfo) -> Result<(), String> {
        self.rest
            .set_channel_metadata(&info.channel, &info.name, &info.topic)
            .await
    }

    /// Pull `probability-amplifier.room.*` channel metadata from App Context into the room list.
    /// Returns how many matching channels were applied.
    pub async fn refresh_rooms_from_app_context(&self) -> Result<usize, String> {
        let listing = self.rest.get_all_channel_metadata().await?;
        let mut applied = 0usize;
        let mut seen: HashSet<String> = HashSet::new();
        for ch in listing.channels {
            if !is_room_channel(&ch.id) {
                continue;
            }
            let Some(slug) = room_slug_from_channel(&ch.id) else {
                continue;
            };
            seen.insert(slug.clone());
            let name = ch
                .name
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| slug.clone());
            let topic = ch
                .description
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| name.clone());
            self.ensure_room(&slug, &name, &topic);
            applied += 1;
        }

        // A channel deleted in BizOps disappears from App Context, so anything we
        // know about but no longer see is gone. Only safe on a complete listing —
        // a truncated page would otherwise look like a mass deletion.
        if listing.complete {
            let stale: Vec<String> = self
                .rooms
                .iter()
                .map(|r| r.slug.clone())
                .filter(|slug| !seen.contains(slug))
                .collect();
            for slug in stale {
                tracing::info!("room '{slug}' removed: no longer in App Context");
                self.rooms.remove(&slug);
                // Deliberately keep the `subscribed` entry: there is no handle to
                // cancel the subscribe task, and keeping it stops a re-created
                // channel from spawning a second loop on the same channel.
            }
        }

        Ok(applied)
    }

    /// Refresh App Context rooms and broadcast an updated room list when anything changes.
    pub async fn sync_rooms_from_app_context(&self) {
        let before = room_fingerprint(&self.rooms);
        match self.refresh_rooms_from_app_context().await {
            Ok(_) => {
                let after = room_fingerprint(&self.rooms);
                if after != before {
                    let _ = self.tx.send(ServerEvent::Rooms {
                        rooms: self.list_rooms(),
                    });
                }
            }
            Err(err) => tracing::warn!("App Context room refresh failed: {err}"),
        }
    }

    fn spawn_app_context_refresh_loop(&self) {
        let state = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(APP_CONTEXT_REFRESH_SECS));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // First tick completes immediately; skip — we already loaded at startup.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                state.sync_rooms_from_app_context().await;
            }
        });
    }

    pub fn room_info(&self, slug: &str) -> Option<RoomInfo> {
        self.rooms.get(slug).map(|r| RoomInfo {
            slug: r.slug.clone(),
            channel: r.channel.clone(),
            name: r.name.clone(),
            topic: r.topic.clone(),
        })
    }

    pub fn list_rooms(&self) -> Vec<RoomInfo> {
        let mut rooms: Vec<_> = self
            .rooms
            .iter()
            .map(|r| RoomInfo {
                slug: r.slug.clone(),
                channel: r.channel.clone(),
                name: r.name.clone(),
                topic: r.topic.clone(),
            })
            .collect();
        rooms.sort_by(|a, b| a.name.cmp(&b.name));
        rooms
    }

    fn subscribe_room(&self, channel: &str) {
        if self.subscribed.contains_key(channel) {
            return;
        }
        self.subscribed.insert(channel.to_string(), ());

        let subscription = self.client.subscription(SubscriptionParams {
            channels: Some(&[channel]),
            channel_groups: None,
            options: None,
        });
        subscription.subscribe();

        let tx = self.tx.clone();
        let names = self.names.clone();
        let rooms = self.rooms.clone();
        let transcripts = self.transcripts.clone();

        tokio::spawn(async move {
            subscription
                .stream()
                .for_each(|event| {
                    let tx = tx.clone();
                    let names = names.clone();
                    let rooms = rooms.clone();
                    let transcripts = transcripts.clone();
                    async move {
                        match event {
                            Update::Message(message) => {
                                match serde_json::from_slice::<ChatEnvelope>(&message.data) {
                                    Ok(envelope) => {
                                        let room = rooms
                                            .iter()
                                            .find(|r| r.channel == message.channel)
                                            .map(|r| r.slug.clone())
                                            .unwrap_or_else(|| envelope.room.clone());
                                        let chat_message = ChatMessage {
                                            timetoken: message.timestamp.to_string(),
                                            envelope,
                                            reactions: HashMap::new(),
                                            readers: Vec::new(),
                                            edited: false,
                                        };
                                        remember_live_message(&transcripts, &room, &chat_message);
                                        let _ = tx.send(ServerEvent::Message {
                                            room,
                                            message: chat_message,
                                        });
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            "skip subscribe message on {}: {err}",
                                            message.channel
                                        );
                                    }
                                }
                            }
                            Update::Signal(message) => {
                                if let Ok(payload) =
                                    serde_json::from_slice::<serde_json::Value>(&message.data)
                                {
                                    let room = rooms
                                        .iter()
                                        .find(|r| r.channel == message.channel)
                                        .map(|r| r.slug.clone())
                                        .unwrap_or_default();
                                    let user_id = payload
                                        .get("user_id")
                                        .and_then(|v| v.as_str())
                                        .map(str::to_string)
                                        .or_else(|| message.sender.clone())
                                        .unwrap_or_else(|| "unknown".into());
                                    let display_name = names
                                        .get(&user_id)
                                        .map(|n| n.clone())
                                        .unwrap_or_else(|| user_id.clone());
                                    if payload.get("t").and_then(|v| v.as_str()) == Some("typing")
                                        || payload.get("typing").is_some()
                                    {
                                        let typing = payload
                                            .get("typing")
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false);
                                        let _ = tx.send(ServerEvent::Typing {
                                            room,
                                            user_id,
                                            display_name,
                                            typing,
                                        });
                                    }
                                }
                            }
                            Update::MessageAction(action) => {
                                let room = rooms
                                    .iter()
                                    .find(|r| r.channel == action.channel)
                                    .map(|r| r.slug.clone())
                                    .unwrap_or_default();
                                match action.r#type.as_str() {
                                    "reaction" => {
                                        let added =
                                            !matches!(action.event, MessageActionEvent::Delete);
                                        let _ = tx.send(ServerEvent::Reaction {
                                            room,
                                            message_timetoken: action.message_timetoken,
                                            reaction: action.value,
                                            user_id: action.sender,
                                            added,
                                        });
                                    }
                                    "receipt" if action.value == "read" => {
                                        let display_name = names
                                            .get(&action.sender)
                                            .map(|n| n.clone())
                                            .unwrap_or_else(|| action.sender.clone());
                                        let _ = tx.send(ServerEvent::ReadReceipt {
                                            room,
                                            message_timetoken: action.message_timetoken,
                                            user_id: action.sender,
                                            display_name,
                                        });
                                    }
                                    // An edit carries the replacement text as its
                                    // value; the published payload never changes.
                                    "edited"
                                        if !matches!(action.event, MessageActionEvent::Delete) =>
                                    {
                                        revise_live_message(
                                            &transcripts,
                                            &room,
                                            &action.message_timetoken,
                                            &action.value,
                                        );
                                        let _ = tx.send(ServerEvent::MessageEdited {
                                            room,
                                            message_timetoken: action.message_timetoken,
                                            text: action.value,
                                        });
                                    }
                                    // Moderation tools (BizOps) soft-delete via a message action.
                                    "deleted"
                                        if !matches!(action.event, MessageActionEvent::Delete) =>
                                    {
                                        forget_live_message(
                                            &transcripts,
                                            &room,
                                            &action.message_timetoken,
                                        );
                                        let _ = tx.send(ServerEvent::MessageDeleted {
                                            room,
                                            message_timetoken: action.message_timetoken,
                                        });
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                })
                .await;
        });
    }

    /// Register / refresh profile: memory + PubNub App Context (Objects REST).
    pub async fn register_profile(
        &self,
        user_id: &str,
        display_name: &str,
        avatar_color: Option<&str>,
    ) -> UserProfile {
        let cached_color = self.profiles.get(user_id).map(|p| p.avatar_color.clone());
        // Prefer client color, then memory, then App Context, then default.
        let color = if let Some(c) = avatar_color {
            sanitize_avatar_color(Some(c))
        } else if let Some(c) = cached_color {
            sanitize_avatar_color(Some(&c))
        } else if let Ok(Some(remote)) = self.rest.get_uuid_metadata(user_id).await {
            sanitize_avatar_color(Some(&remote.avatar_color))
        } else {
            default_avatar_color()
        };

        let profile = UserProfile {
            user_id: user_id.to_string(),
            display_name: display_name.to_string(),
            avatar_initials: avatar_initials(display_name),
            avatar_color: color,
        };
        self.names
            .insert(user_id.to_string(), display_name.to_string());
        self.profiles.insert(user_id.to_string(), profile.clone());

        if let Err(err) = self.rest.set_uuid_metadata(&profile).await {
            tracing::warn!("App Context setUUIDMetadata failed for {user_id}: {err}");
        }
        profile
    }

    pub fn profile_for(&self, user_id: &str, display_name: &str) -> UserProfile {
        self.profiles
            .get(user_id)
            .map(|p| p.clone())
            .unwrap_or_else(|| UserProfile {
                user_id: user_id.to_string(),
                display_name: display_name.to_string(),
                avatar_initials: avatar_initials(display_name),
                avatar_color: default_avatar_color(),
            })
    }

    pub fn join_room(
        &self,
        slug: &str,
        user_id: &str,
        display_name: &str,
    ) -> Result<RoomInfo, String> {
        self.names
            .insert(user_id.to_string(), display_name.to_string());
        self.room_info(slug)
            .ok_or_else(|| format!("Unknown room '{slug}'"))
    }

    pub async fn publish_chat(
        &self,
        room_slug: &str,
        user_id: &str,
        display_name: &str,
        text: &str,
        kind: &str,
    ) -> Result<(), String> {
        let channel = self.channel_for(room_slug)?;
        let profile = self.profile_for(user_id, display_name);
        let envelope = ChatEnvelope {
            schema: 1,
            id: Uuid::new_v4().to_string(),
            kind: kind.to_string(),
            text: text.to_string(),
            author_id: user_id.to_string(),
            author_name: display_name.to_string(),
            room: room_slug.to_string(),
            avatar_initials: profile.avatar_initials,
            avatar_color: profile.avatar_color,
        };

        self.client
            .publish_message(envelope)
            .channel(channel)
            // Live delivery only. This build does not write Message Persistence.
            .store(false)
            // GET carries the payload in the URL, so anything but a short message
            // comes back as 414 Request URI Too Long.
            .use_post(true)
            .execute()
            .await
            .map_err(|e| e.to_string())?;

        // No local WS fan-out: Auto Moderation may abort (422) or rewrite the
        // payload. Clients must observe the post-Function message via subscribe
        // (or nothing, if blocked).
        Ok(())
    }

    pub async fn typing(&self, room_slug: &str, user_id: &str, typing: bool) -> Result<(), String> {
        let channel = self.channel_for(room_slug)?;
        self.rest
            .send_signal(
                &channel,
                user_id,
                &json!({
                    "t": "typing",
                    "typing": typing,
                    "user_id": user_id,
                }),
            )
            .await
    }

    pub async fn react(
        &self,
        room_slug: &str,
        user_id: &str,
        message_timetoken: &str,
        reaction: &str,
    ) -> Result<(), String> {
        let channel = self.channel_for(room_slug)?;
        self.rest
            .add_message_action(&channel, message_timetoken, user_id, "reaction", reaction)
            .await
    }

    pub async fn mark_read(
        &self,
        room_slug: &str,
        user_id: &str,
        message_timetoken: &str,
    ) -> Result<(), String> {
        let channel = self.channel_for(room_slug)?;
        self.rest
            .add_message_action(&channel, message_timetoken, user_id, "receipt", "read")
            .await
    }

    pub async fn analyze(
        &self,
        room_slug: &str,
        user_id: &str,
        display_name: &str,
    ) -> Result<(), String> {
        self.channel_for(room_slug)?;
        let seen: Vec<ChatMessage> = self
            .transcripts
            .get(room_slug)
            .map(|messages| messages.clone())
            .unwrap_or_default();
        let analysis = llm::analyze_room(&self.config, room_slug, &seen).await;
        let _ = self.tx.send(ServerEvent::Analysis {
            room: room_slug.to_string(),
            analysis: analysis.clone(),
        });

        let text = format!(
            "🧠 Analysis\n{}\n\nThemes:\n{}\n\nPromising ideas:\n{}\n\nOpen questions:\n{}\n\nNext steps:\n{}",
            analysis.summary,
            analysis
                .themes
                .iter()
                .map(|t| format!("• {t}"))
                .collect::<Vec<_>>()
                .join("\n"),
            analysis
                .promising_ideas
                .iter()
                .map(|t| format!("• {t}"))
                .collect::<Vec<_>>()
                .join("\n"),
            analysis
                .open_questions
                .iter()
                .map(|t| format!("• {t}"))
                .collect::<Vec<_>>()
                .join("\n"),
            analysis
                .next_steps
                .iter()
                .map(|t| format!("• {t}"))
                .collect::<Vec<_>>()
                .join("\n"),
        );
        self.publish_chat(room_slug, user_id, display_name, &text, "analysis")
            .await
    }
}

async fn grant_hub_token(
    config: &Config,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let admin = PubNubClientBuilder::with_reqwest_transport()
        .with_keyset(Keyset {
            subscribe_key: config.subscribe_key.clone(),
            publish_key: Some(config.publish_key.clone()),
            secret_key: Some(secret_key.to_string()),
        })
        .with_user_id(HUB_USER_ID)
        .build()?;

    // Chat channels and App Context UUID + channel metadata.
    // No presence `join` grant: this build does not subscribe to `-pnpres`.
    // Channel pattern get/update covers Objects channel metadata for probability-amplifier.room.*.
    // No authorized_user_id: hub REST calls attach end-user uuids for
    // reactions/typing/receipts while still using this server token.
    let perms: Vec<Box<dyn permissions::Permission>> = vec![
        permissions::channel(r"^probability-amplifier\.room\..*")
            .read()
            .write()
            .get()
            .manage()
            .update()
            .delete(),
        permissions::user_id(".*").get().update().delete(),
    ];
    let grant = admin
        .grant_token(TOKEN_TTL_MINUTES)
        .patterns(&perms)
        .execute()
        .await?;

    tracing::info!("Granted Access Manager token for hub (ttl={TOKEN_TTL_MINUTES}m)");
    Ok(grant.token)
}

/// Keep user-supplied text short enough that it can't bury the error beside it.
fn truncate_for_display(value: &str, max_chars: usize) -> String {
    let mut out: String = value.chars().take(max_chars).collect();
    if value.chars().nth(max_chars).is_some() {
        out.push('…');
    }
    out
}

fn room_fingerprint(rooms: &DashMap<String, RoomState>) -> Vec<(String, String, String)> {
    let mut rows: Vec<_> = rooms
        .iter()
        .map(|r| (r.slug.clone(), r.name.clone(), r.topic.clone()))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn remember_live_message(
    transcripts: &DashMap<String, Vec<ChatMessage>>,
    room: &str,
    message: &ChatMessage,
) {
    let mut messages = transcripts.entry(room.to_string()).or_default();
    if messages
        .iter()
        .any(|existing| existing.timetoken == message.timetoken)
    {
        return;
    }
    messages.push(message.clone());
    let overflow = messages.len().saturating_sub(LIVE_TRANSCRIPT_CAP);
    if overflow > 0 {
        messages.drain(0..overflow);
    }
}

fn revise_live_message(
    transcripts: &DashMap<String, Vec<ChatMessage>>,
    room: &str,
    timetoken: &str,
    text: &str,
) {
    let Some(mut messages) = transcripts.get_mut(room) else {
        return;
    };
    let Some(message) = messages.iter_mut().find(|m| m.timetoken == timetoken) else {
        return;
    };
    message.envelope.set_text(text);
    message.edited = true;
}

fn forget_live_message(
    transcripts: &DashMap<String, Vec<ChatMessage>>,
    room: &str,
    timetoken: &str,
) {
    let Some(mut messages) = transcripts.get_mut(room) else {
        return;
    };
    messages.retain(|message| message.timetoken != timetoken);
}
