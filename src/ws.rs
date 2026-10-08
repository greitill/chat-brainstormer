use crate::hub::AppState;
use crate::models::{ClientEvent, ServerEvent};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver): (SplitSink<WebSocket, Message>, SplitStream<WebSocket>) =
        socket.split();
    let mut bus_rx = state.tx.subscribe();
    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<ServerEvent>();

    let mut user_id = format!("user.{}", Uuid::new_v4().as_simple());
    let mut display_name = String::from("Guest");
    let mut current_room: Option<String> = None;
    let active_room = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let active_room_reader = active_room.clone();

    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                event = bus_rx.recv() => {
                    let event = match classify_bus(event) {
                        BusFlow::Forward(event) => event,
                        BusFlow::Skip => continue,
                        BusFlow::Stop => break,
                    };
                    let room = active_room_reader.lock().await.clone();
                    if should_forward(&event, room.as_deref()) {
                        if write_event(&mut sender, &event).await.is_err() {
                            break;
                        }
                    }
                }
                event = client_rx.recv() => {
                    let Some(event) = event else { break };
                    if write_event(&mut sender, &event).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    while let Some(Ok(msg)) = receiver.next().await {
        let Message::Text(text) = msg else {
            continue;
        };

        let event: ClientEvent = match serde_json::from_str(&text) {
            Ok(e) => e,
            Err(err) => {
                let _ = client_tx.send(ServerEvent::Error {
                    message: format!("Bad client event: {err}"),
                });
                continue;
            }
        };

        match event {
            ClientEvent::Hello {
                display_name: name,
                user_id: claimed_id,
                avatar_color,
            } => {
                display_name = name.trim().to_string();
                if display_name.is_empty() {
                    display_name = "Guest".into();
                }
                if let Some(claimed) = claimed_id {
                    if is_stable_user_id(&claimed) {
                        user_id = claimed;
                    }
                }
                let profile = state
                    .register_profile(&user_id, &display_name, avatar_color.as_deref())
                    .await;
                let _ = client_tx.send(ServerEvent::Welcome {
                    user_id: user_id.clone(),
                    display_name: display_name.clone(),
                    avatar_initials: profile.avatar_initials,
                    avatar_color: profile.avatar_color,
                });
                // No App Context round trip here: startup loads the rooms and the
                // background refresh broadcasts later changes, so login stays fast.
                let _ = client_tx.send(ServerEvent::Rooms {
                    rooms: state.list_rooms(),
                });
            }
            ClientEvent::ListRooms => {
                state.sync_rooms_from_app_context().await;
                let _ = client_tx.send(ServerEvent::Rooms {
                    rooms: state.list_rooms(),
                });
            }
            ClientEvent::CreateRoom { name, topic } => {
                let topic = topic.unwrap_or_else(|| name.clone());
                let info = match state.create_room(&name, &topic).await {
                    Ok(info) => info,
                    Err(message) => {
                        let _ = client_tx.send(ServerEvent::Error { message });
                        continue;
                    }
                };
                let _ = state.tx.send(ServerEvent::Rooms {
                    rooms: state.list_rooms(),
                });
                if let Some(prev) = current_room.take() {
                    state.leave_room(&prev, &user_id);
                }
                match state.join_room(&info.slug, &user_id, &display_name).await {
                    Ok((room, history, online)) => {
                        current_room = Some(room.slug.clone());
                        *active_room.lock().await = Some(room.slug.clone());
                        let _ = client_tx.send(ServerEvent::RoomJoined {
                            room,
                            history,
                            online,
                        });
                    }
                    Err(message) => {
                        let _ = client_tx.send(ServerEvent::Error { message });
                    }
                }
            }
            ClientEvent::JoinRoom { room } => {
                if let Some(prev) = current_room.take() {
                    state.leave_room(&prev, &user_id);
                }
                match state.join_room(&room, &user_id, &display_name).await {
                    Ok((room, history, online)) => {
                        current_room = Some(room.slug.clone());
                        *active_room.lock().await = Some(room.slug.clone());
                        let _ = client_tx.send(ServerEvent::RoomJoined {
                            room,
                            history,
                            online,
                        });
                    }
                    Err(message) => {
                        let _ = client_tx.send(ServerEvent::Error { message });
                    }
                }
            }
            ClientEvent::LeaveRoom { room } => {
                state.leave_room(&room, &user_id);
                if current_room.as_deref() == Some(room.as_str()) {
                    current_room = None;
                    *active_room.lock().await = None;
                }
            }
            ClientEvent::SendMessage { room, text } => {
                let text = text.trim();
                if text.is_empty() {
                    continue;
                }
                if let Err(message) = state
                    .publish_chat(&room, &user_id, &display_name, text, "chat")
                    .await
                {
                    let _ = client_tx.send(ServerEvent::Error { message });
                }
            }
            ClientEvent::Typing { room, typing } => {
                let _ = state.typing(&room, &user_id, typing).await;
            }
            ClientEvent::React {
                room,
                message_timetoken,
                reaction,
            } => {
                if let Err(message) = state
                    .react(&room, &user_id, &message_timetoken, &reaction)
                    .await
                {
                    let _ = client_tx.send(ServerEvent::Error { message });
                }
            }
            ClientEvent::Read {
                room,
                message_timetoken,
            } => {
                let _ = state.mark_read(&room, &user_id, &message_timetoken).await;
            }
            ClientEvent::Analyze { room } => {
                if let Err(message) = state.analyze(&room, &user_id, &display_name).await {
                    let _ = client_tx.send(ServerEvent::Error { message });
                }
            }
            ClientEvent::LoadHistory { room } => {
                if let Some(info) = state.room_info(&room) {
                    match state.rest.fetch_history(&info.channel, 40).await {
                        Ok(history) => {
                            let online = state.online_users(&room);
                            let _ = client_tx.send(ServerEvent::RoomJoined {
                                room: info,
                                history,
                                online,
                            });
                        }
                        Err(message) => {
                            let _ = client_tx.send(ServerEvent::Error { message });
                        }
                    }
                }
            }
        }
    }

    if let Some(room) = current_room {
        state.leave_room(&room, &user_id);
    }
    send_task.abort();
}

enum BusFlow {
    Forward(ServerEvent),
    Skip,
    Stop,
}

/// Decide what a broadcast-bus result means for a client's send loop.
///
/// Lagging behind is recoverable: the socket is still open, so treating it as
/// terminal would leave the client able to send while never receiving again.
fn classify_bus(result: Result<ServerEvent, broadcast::error::RecvError>) -> BusFlow {
    match result {
        Ok(event) => BusFlow::Forward(event),
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            tracing::warn!("WS client lagged, dropped {skipped} event(s); resyncing");
            BusFlow::Skip
        }
        Err(broadcast::error::RecvError::Closed) => BusFlow::Stop,
    }
}

fn is_stable_user_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("user.") else {
        return false;
    };
    // uuid simple = 32 hex chars; also allow longer hex if format evolves
    (32..=64).contains(&rest.len()) && rest.chars().all(|c| c.is_ascii_hexdigit())
}

fn should_forward(event: &ServerEvent, room: Option<&str>) -> bool {
    match event {
        ServerEvent::Rooms { .. } => true,
        ServerEvent::Message { room: r, .. }
        | ServerEvent::Typing { room: r, .. }
        | ServerEvent::Presence { room: r, .. }
        | ServerEvent::Reaction { room: r, .. }
        | ServerEvent::ReadReceipt { room: r, .. }
        | ServerEvent::MessageDeleted { room: r, .. }
        | ServerEvent::MessageEdited { room: r, .. }
        | ServerEvent::Analysis { room: r, .. } => room == Some(r.as_str()),
        ServerEvent::Welcome { .. }
        | ServerEvent::RoomJoined { .. }
        | ServerEvent::Error { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::RoomInfo;

    fn rooms_event(name: &str) -> ServerEvent {
        ServerEvent::Rooms {
            rooms: vec![RoomInfo {
                slug: name.to_string(),
                channel: format!("probability-amplifier.room.{name}"),
                name: name.to_string(),
                topic: String::new(),
                members: 0,
            }],
        }
    }

    fn slug_of(event: &ServerEvent) -> String {
        match event {
            ServerEvent::Rooms { rooms } => rooms[0].slug.clone(),
            _ => unreachable!("test only publishes Rooms events"),
        }
    }

    /// A client that falls behind the bus must keep receiving later events.
    /// Regression: treating `Lagged` as terminal killed the send loop while the
    /// socket stayed open, so the client could publish but never see anything.
    #[tokio::test]
    async fn lagging_client_skips_then_resumes() {
        let (tx, mut rx) = broadcast::channel::<ServerEvent>(4);

        // Overflow the buffer while the client is not reading.
        for i in 0..10 {
            let _ = tx.send(rooms_event(&format!("flood-{i}")));
        }
        let _ = tx.send(rooms_event("after-lag"));

        // Drive the same decision the send loop makes, and collect what a client
        // would actually have been sent.
        let mut lagged = false;
        let mut delivered = Vec::new();
        loop {
            match classify_bus(rx.recv().await) {
                BusFlow::Forward(event) => delivered.push(slug_of(&event)),
                BusFlow::Skip => lagged = true,
                BusFlow::Stop => break,
            }
            if delivered.last().is_some_and(|s| s == "after-lag") {
                drop(tx);
                break;
            }
        }

        assert!(lagged, "expected the client to report a lag");
        assert!(
            delivered.contains(&"after-lag".to_string()),
            "lagged client never resynced, delivered {delivered:?}"
        );
    }

    #[tokio::test]
    async fn closed_bus_stops_the_loop() {
        let (tx, mut rx) = broadcast::channel::<ServerEvent>(4);
        drop(tx);
        assert!(matches!(classify_bus(rx.recv().await), BusFlow::Stop));
    }
}

async fn write_event(
    sender: &mut SplitSink<WebSocket, Message>,
    event: &ServerEvent,
) -> Result<(), ()> {
    let Ok(json) = serde_json::to_string(event) else {
        return Err(());
    };
    sender
        .send(Message::Text(json.into()))
        .await
        .map_err(|_| ())
}
