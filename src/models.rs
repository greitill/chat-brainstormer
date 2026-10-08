use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub fn slugify(value: &str) -> String {
    let s = value
        .trim()
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>();
    let s = s.trim_matches('-').chars().take(32).collect::<String>();
    if s.is_empty() {
        "ideas".into()
    } else {
        s
    }
}

/// write_pubnub_app: max 3 dot levels, IDs last — `<project>.room.<slug>`
pub fn room_channel(slug: &str) -> String {
    format!("probability-amplifier.room.{}", slugify(slug))
}

const ROOM_CHANNEL_PREFIX: &str = "probability-amplifier.room.";

/// Map an App Context / pub/sub channel id back to a room slug.
pub fn room_slug_from_channel(channel: &str) -> Option<String> {
    channel
        .strip_prefix(ROOM_CHANNEL_PREFIX)
        .map(slugify)
        .filter(|s| !s.is_empty())
}

/// True when this Objects channel id belongs to Probability Amplifier rooms.
pub fn is_room_channel(channel: &str) -> bool {
    room_slug_from_channel(channel).is_some()
}

/// Initials from each word: "Alex" → "A", "Anna Kowalska" → "AK"
pub fn avatar_initials(name: &str) -> String {
    let initials: String = name
        .split_whitespace()
        .filter_map(|word| {
            word.chars()
                .find(|c| c.is_alphanumeric())
                .map(|c| c.to_uppercase().next().unwrap_or(c))
        })
        .take(3)
        .collect();
    if initials.is_empty() {
        "?".into()
    } else {
        initials
    }
}

pub fn default_avatar_color() -> String {
    "#0f7a5f".into()
}

/// Accept `#RGB` / `#RRGGBB`; normalize to `#RRGGBB`.
pub fn sanitize_avatar_color(value: Option<&str>) -> String {
    let Some(raw) = value.map(str::trim).filter(|s| !s.is_empty()) else {
        return default_avatar_color();
    };
    let hex = raw.strip_prefix('#').unwrap_or(raw);
    if hex.len() == 3 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        let expanded: String = hex.chars().flat_map(|c| [c, c]).collect();
        return format!("#{expanded}");
    }
    if hex.len() == 6 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return format!("#{}", hex.to_ascii_lowercase());
    }
    default_avatar_color()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub user_id: String,
    pub display_name: String,
    pub avatar_initials: String,
    pub avatar_color: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatEnvelope {
    pub schema: u8,
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    /// Flat on purpose: Auto Moderation evaluates `$.message.text` against
    /// `{ message: <published payload>, ... }`, so text must be at payload root.
    pub text: String,
    pub author_id: String,
    pub author_name: String,
    #[serde(default)]
    pub room: String,
    #[serde(default)]
    pub avatar_initials: String,
    #[serde(default = "default_avatar_color")]
    pub avatar_color: String,
}

#[derive(Deserialize)]
struct ChatEnvelopeBody {
    text: String,
}

#[derive(Deserialize)]
struct ChatEnvelopeWire {
    #[serde(default = "default_schema")]
    schema: u8,
    id: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
    /// Brief nested shape from an earlier experiment; still accepted on read.
    #[serde(default)]
    message: Option<ChatEnvelopeBody>,
    author_id: String,
    author_name: String,
    #[serde(default)]
    room: String,
    #[serde(default)]
    avatar_initials: String,
    #[serde(default = "default_avatar_color")]
    avatar_color: String,
}

fn default_schema() -> u8 {
    1
}

impl<'de> Deserialize<'de> for ChatEnvelope {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ChatEnvelopeWire::deserialize(deserializer)?;
        let text = wire
            .text
            .or_else(|| wire.message.map(|m| m.text))
            .ok_or_else(|| serde::de::Error::custom("missing text"))?;
        Ok(ChatEnvelope {
            schema: wire.schema,
            id: wire.id,
            kind: wire.kind,
            text,
            author_id: wire.author_id,
            author_name: wire.author_name,
            room: wire.room,
            avatar_initials: if wire.avatar_initials.is_empty() {
                "?".into()
            } else {
                wire.avatar_initials
            },
            avatar_color: wire.avatar_color,
        })
    }
}

impl ChatEnvelope {
    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn set_text(&mut self, text: impl Into<String>) {
        self.text = text.into();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub timetoken: String,
    pub envelope: ChatEnvelope,
    #[serde(default)]
    pub reactions: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub readers: Vec<String>,
    /// True when an `edited` action replaced the originally published text.
    #[serde(default)]
    pub edited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ClientEvent {
    Hello {
        display_name: String,
        /// Stable client identity so history keeps aligning to "mine" on re-entry.
        #[serde(default)]
        user_id: Option<String>,
        /// Chosen at gate; stored in PubNub App Context custom fields.
        #[serde(default)]
        avatar_color: Option<String>,
    },
    ListRooms,
    CreateRoom {
        name: String,
        topic: Option<String>,
    },
    JoinRoom {
        room: String,
    },
    LeaveRoom {
        room: String,
    },
    SendMessage {
        room: String,
        text: String,
    },
    Typing {
        room: String,
        typing: bool,
    },
    React {
        room: String,
        message_timetoken: String,
        reaction: String,
    },
    Read {
        room: String,
        message_timetoken: String,
    },
    Analyze {
        room: String,
    },
    LoadHistory {
        room: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ServerEvent {
    Welcome {
        user_id: String,
        display_name: String,
        avatar_initials: String,
        avatar_color: String,
    },
    Rooms {
        rooms: Vec<RoomInfo>,
    },
    RoomJoined {
        room: RoomInfo,
        history: Vec<ChatMessage>,
        online: Vec<OnlineUser>,
    },
    Message {
        room: String,
        message: ChatMessage,
    },
    Typing {
        room: String,
        user_id: String,
        display_name: String,
        typing: bool,
    },
    Presence {
        room: String,
        online: Vec<OnlineUser>,
    },
    Reaction {
        room: String,
        message_timetoken: String,
        reaction: String,
        user_id: String,
        added: bool,
    },
    ReadReceipt {
        room: String,
        message_timetoken: String,
        user_id: String,
        display_name: String,
    },
    /// A message was soft-deleted (moderation / BizOps), which PubNub reports as a
    /// `deleted` message action rather than removing it from Message Persistence.
    MessageDeleted {
        room: String,
        message_timetoken: String,
    },
    /// A message's text was replaced via an `edited` action; the published payload
    /// in history is unchanged.
    MessageEdited {
        room: String,
        message_timetoken: String,
        text: String,
    },
    Analysis {
        room: String,
        analysis: AnalysisResult,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomInfo {
    pub slug: String,
    pub channel: String,
    pub name: String,
    pub topic: String,
    pub members: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineUser {
    pub user_id: String,
    pub display_name: String,
    #[serde(default)]
    pub avatar_initials: String,
    #[serde(default = "default_avatar_color")]
    pub avatar_color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub summary: String,
    pub themes: Vec<String>,
    #[serde(alias = "promisingIdeas")]
    pub promising_ideas: Vec<String>,
    #[serde(alias = "openQuestions")]
    pub open_questions: Vec<String>,
    #[serde(alias = "nextSteps")]
    pub next_steps: Vec<String>,
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

fn default_source() -> String {
    "openai".into()
}
