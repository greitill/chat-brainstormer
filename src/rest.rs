use crate::models::{avatar_initials, default_avatar_color, UserProfile};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use sha2::Sha256;
use std::sync::{Arc, RwLock};

/// Requesting UUID for Objects REST (PAM token is unbound to a user).
const HUB_OBJECTS_UUID: &str = "sparkline-hub";

fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 3);
    for b in value.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Channel metadata from App Context (Objects API).
/// Result of listing App Context channels.
#[derive(Debug, Clone, Default)]
pub struct ChannelListing {
    pub channels: Vec<ChannelMetadata>,
    /// False when a page failed mid-walk. Callers must not treat a missing channel
    /// as deleted unless the listing is complete.
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub struct ChannelMetadata {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
}

/// Thin REST helpers for APIs not exposed by the Rust Core SDK yet
/// (signals, message actions, App Context user/channel metadata).
#[derive(Clone)]
pub struct PubNubRest {
    http: Client,
    publish_key: String,
    subscribe_key: String,
    /// Shared with every clone so a token refresh reaches in-flight callers.
    auth_token: Arc<RwLock<Option<String>>>,
    secret_key: Option<String>,
}

impl PubNubRest {
    pub fn new(
        publish_key: String,
        subscribe_key: String,
        auth_token: Option<String>,
        secret_key: Option<String>,
    ) -> Self {
        Self {
            http: Client::new(),
            publish_key,
            subscribe_key,
            auth_token: Arc::new(RwLock::new(auth_token)),
            secret_key,
        }
    }

    /// Swap in a freshly granted Access Manager token.
    pub fn set_auth_token(&self, token: Option<String>) {
        *self
            .auth_token
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = token;
    }

    fn with_auth(&self, url: String) -> String {
        let token = self
            .auth_token
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        match token {
            Some(token) => {
                let sep = if url.contains('?') { '&' } else { '?' };
                format!("{url}{sep}auth={}", encode(&token))
            }
            None => url,
        }
    }

    /// PAM v2 secret-key signed GET URL.
    ///
    /// `Get All Channel Metadata` is rejected with "Signature missing" when the
    /// keyset disallows it for token auth, so it must be signed server-side.
    /// Param values must stay signature-safe: only characters that percent-encode
    /// identically on both sides (alphanumerics, `-`, `_`, `.`, `~`).
    fn signed_get_url(&self, path: &str, params: &[(&str, String)]) -> Option<String> {
        let secret_key = self.secret_key.as_deref()?;

        let mut pairs: Vec<String> = params
            .iter()
            .map(|(key, value)| format!("{key}={}", encode(value)))
            .collect();
        pairs.push(format!("timestamp={}", chrono::Utc::now().timestamp()));
        pairs.sort();
        let query = pairs.join("&");

        let signature = signature_v2(secret_key, &self.publish_key, path, &query);
        Some(format!(
            "https://ps.pndsn.com{path}?{query}&signature={}",
            encode(&signature)
        ))
    }

    pub async fn send_signal(
        &self,
        channel: &str,
        user_id: &str,
        payload: &Value,
    ) -> Result<(), String> {
        let encoded = encode(&payload.to_string());
        let url = self.with_auth(format!(
            "https://ps.pndsn.com/signal/{}/{}/0/{}/0/{}?uuid={}",
            self.publish_key,
            self.subscribe_key,
            encode(channel),
            encoded,
            encode(user_id)
        ));
        let res = self.http.get(url).send().await.map_err(|e| e.to_string())?;
        if !res.status().is_success() {
            return Err(format!("signal failed: {}", res.status()));
        }
        Ok(())
    }

    /// App Context: setUUIDMetadata — Rust Core SDK has no Objects write API.
    pub async fn set_uuid_metadata(&self, profile: &UserProfile) -> Result<(), String> {
        let url = self.with_auth(format!(
            "https://ps.pndsn.com/v2/objects/{}/uuids/{}?include=custom&uuid={}",
            self.subscribe_key,
            encode(&profile.user_id),
            encode(HUB_OBJECTS_UUID)
        ));
        let res = self
            .http
            .patch(url)
            .header("Content-Type", "application/json; charset=UTF-8")
            .json(&json!({
                "name": profile.display_name,
                "custom": {
                    "avatarColor": profile.avatar_color,
                    "avatarInitials": profile.avatar_initials,
                }
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if status.is_success() {
            tracing::debug!("set uuid metadata ok ({status}): {text}");
            Ok(())
        } else {
            Err(format!(
                "setUUIDMetadata — {}",
                describe_error(status, &text)
            ))
        }
    }

    pub async fn get_uuid_metadata(&self, user_id: &str) -> Result<Option<UserProfile>, String> {
        let url = self.with_auth(format!(
            "https://ps.pndsn.com/v2/objects/{}/uuids/{}?include=custom&uuid={}",
            self.subscribe_key,
            encode(user_id),
            encode(HUB_OBJECTS_UUID)
        ));
        let res = self.http.get(url).send().await.map_err(|e| e.to_string())?;
        let status = res.status();
        let body: Value = res.json().await.map_err(|e| e.to_string())?;
        if status.as_u16() == 404 {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(format!(
                "getUUIDMetadata — {}",
                describe_error_value(status, &body)
            ));
        }
        let data = body.get("data").unwrap_or(&body);
        let display_name = data
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(user_id)
            .to_string();
        let custom = data.get("custom").cloned().unwrap_or(json!({}));
        let avatar_color = custom
            .get("avatarColor")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(default_avatar_color);
        let initials = custom
            .get("avatarInitials")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| avatar_initials(&display_name));
        Ok(Some(UserProfile {
            user_id: user_id.to_string(),
            display_name,
            avatar_initials: initials,
            avatar_color,
        }))
    }

    /// App Context: getAllChannelMetadata (paginated).
    ///
    /// Only standard fields are requested, so no `include` param is needed —
    /// that also keeps every signed query value signature-safe.
    pub async fn get_all_channel_metadata(&self) -> Result<ChannelListing, String> {
        let path = format!("/v2/objects/{}/channels", self.subscribe_key);
        let mut out = Vec::new();
        let mut start: Option<String> = None;
        let mut complete = true;

        loop {
            let mut params: Vec<(&str, String)> = vec![
                ("limit", "100".into()),
                ("count", "true".into()),
                ("uuid", HUB_OBJECTS_UUID.into()),
            ];
            if let Some(token) = &start {
                params.push(("start", token.clone()));
            }

            // Signed when a secret key is available; otherwise fall back to the
            // PAM token, which works only if the keyset allows token-based getAll.
            let url = self.signed_get_url(&path, &params).unwrap_or_else(|| {
                let query = params
                    .iter()
                    .map(|(key, value)| format!("{key}={}", encode(value)))
                    .collect::<Vec<_>>()
                    .join("&");
                self.with_auth(format!("https://ps.pndsn.com{path}?{query}"))
            });

            let res = self.http.get(url).send().await.map_err(|e| e.to_string())?;
            let status = res.status();
            let body: Value = res.json().await.map_err(|e| e.to_string())?;
            if !status.is_success() {
                let err = format!(
                    "getAllChannelMetadata — {}",
                    describe_error_value(status, &body)
                );
                if out.is_empty() {
                    return Err(err);
                }
                tracing::warn!("{err}");
                complete = false;
                break;
            }

            if let Some(arr) = body.get("data").and_then(|d| d.as_array()) {
                for item in arr {
                    let Some(id) = item.get("id").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    out.push(ChannelMetadata {
                        id: id.to_string(),
                        name: item
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        description: item
                            .get("description")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                    });
                }
            }

            let next = body
                .get("next")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .filter(|s| !s.is_empty());
            match next {
                Some(token) if start.as_ref() != Some(&token) => start = Some(token),
                _ => break,
            }
        }

        Ok(ChannelListing {
            channels: out,
            complete,
        })
    }

    /// App Context: setChannelMetadata (create/update).
    pub async fn set_channel_metadata(
        &self,
        channel: &str,
        name: &str,
        description: &str,
    ) -> Result<(), String> {
        let url = self.with_auth(format!(
            "https://ps.pndsn.com/v2/objects/{}/channels/{}?include=custom&uuid={}",
            self.subscribe_key,
            encode(channel),
            encode(HUB_OBJECTS_UUID)
        ));
        let res = self
            .http
            .patch(url)
            .header("Content-Type", "application/json; charset=UTF-8")
            .json(&json!({
                "name": name,
                "description": description,
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if status.is_success() {
            tracing::debug!("set channel metadata ok ({status}): {text}");
            Ok(())
        } else {
            Err(format!(
                "setChannelMetadata — {}",
                describe_error(status, &text)
            ))
        }
    }

    pub async fn add_message_action(
        &self,
        channel: &str,
        message_timetoken: &str,
        user_id: &str,
        action_type: &str,
        value: &str,
    ) -> Result<(), String> {
        let url = self.with_auth(format!(
            "https://ps.pndsn.com/v1/message-actions/{}/channel/{}/message/{}?uuid={}",
            self.subscribe_key,
            encode(channel),
            message_timetoken,
            encode(user_id)
        ));
        let res = self
            .http
            .post(url)
            .header("Content-Type", "application/json; charset=UTF-8")
            .json(&json!({ "type": action_type, "value": value }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        // 200 = ok, 207 = stored but publish failed, 409 = already added (treat as ok)
        if status.as_u16() == 200 || status.as_u16() == 207 || status.as_u16() == 409 {
            tracing::debug!("message action ok ({status}): {text}");
            return Ok(());
        }
        Err(format!(
            "addMessageAction — {}",
            describe_error(status, &text)
        ))
    }
}

/// Turn a PubNub error body into something a user can act on.
///
/// App Context reports every failure with the same top-level message ("REST API
/// request processing error, check status for details") and puts the actual
/// cause in `error.details`, so passing the envelope through loses the reason.
fn describe_error(status: StatusCode, body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(value) => describe_error_value(status, &value),
        Err(_) if body.trim().is_empty() => status.to_string(),
        Err(_) => format!("{status}: {body}"),
    }
}

fn describe_error_value(status: StatusCode, value: &Value) -> String {
    let error = value.get("error");
    let message = error
        .and_then(|e| e.get("message"))
        .or_else(|| value.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("unknown error");

    let details: Vec<String> = error
        .and_then(|e| e.get("details"))
        .and_then(|d| d.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let detail = entry.get("message").and_then(|m| m.as_str())?;
                    Some(match entry.get("location").and_then(|l| l.as_str()) {
                        Some(location) => format!("{detail} ({location})"),
                        None => detail.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    if details.is_empty() {
        format!("{status}: {message}")
    } else {
        format!("{status}: {message} — {}", details.join("; "))
    }
}

/// PAM signature v2 for a GET request with an empty body.
/// `query` must already be percent-encoded and sorted.
fn signature_v2(secret_key: &str, publish_key: &str, path: &str, query: &str) -> String {
    let to_sign = format!("GET\n{publish_key}\n{path}\n{query}\n");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC accepts keys of any size");
    mac.update(to_sign.as_bytes());
    format!("v2.{}", URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Vector taken from the vendored SDK's own signature test so our
    /// hand-rolled signing stays byte-compatible with PubNub's implementation.
    #[test]
    fn signature_v2_matches_sdk_vector() {
        let signature = signature_v2(
            "secKey",
            "pubKey",
            "/publish/pubKey/subKey/0/my_channel/0/%22hello%21%22",
            "pnsdk=PubNub-Rust&store=1&timestamp=1679642098&ttl=10&uuid=userId",
        );
        assert_eq!("v2.AHl5lMpzyT4qcvvlqaszCjTUqU6dPb10a4_XSaYCNIQ", signature);
    }
}
