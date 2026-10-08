const IDENTITY_KEY = 'sparkline.v2.identity';

const DEFAULT_AVATAR_COLOR = '#0f7a5f';

function loadIdentity() {
  try {
    return JSON.parse(localStorage.getItem(IDENTITY_KEY) || 'null');
  } catch {
    return null;
  }
}

function saveIdentity(userId, displayName, avatarColor) {
  localStorage.setItem(
    IDENTITY_KEY,
    JSON.stringify({ userId, displayName, avatarColor }),
  );
}

function avatarInitials(name) {
  const initials = String(name || '')
    .split(/\s+/)
    .filter(Boolean)
    .map((w) => [...w].find((c) => /[0-9A-Za-zÀ-ÿ]/.test(c)))
    .filter(Boolean)
    .map((c) => c.toUpperCase())
    .slice(0, 3)
    .join('');
  return initials || '?';
}

const savedIdentity = loadIdentity();

const state = {
  ws: null,
  userId: savedIdentity?.userId || null,
  displayName: savedIdentity?.displayName || null,
  avatarColor: savedIdentity?.avatarColor || DEFAULT_AVATAR_COLOR,
  avatarInitials: avatarInitials(savedIdentity?.displayName || ''),
  rooms: [],
  currentRoom: null,
  messages: new Map(),
  typing: new Map(),
  online: [],
  // Room slug of an in-flight analysis, or null when idle.
  analyzing: null,
};

const els = {
  gate: document.getElementById('gate'),
  app: document.getElementById('app'),
  helloForm: document.getElementById('hello-form'),
  displayName: document.getElementById('display-name'),
  avatarColor: document.getElementById('avatar-color'),
  avatarPreview: document.getElementById('avatar-preview'),
  roomList: document.getElementById('room-list'),
  you: document.getElementById('you'),
  createRoom: document.getElementById('create-room'),
  roomTitle: document.getElementById('room-title'),
  roomMeta: document.getElementById('room-meta'),
  presence: document.getElementById('presence'),
  onlineList: document.getElementById('online-list'),
  analyze: document.getElementById('analyze'),
  messages: document.getElementById('messages'),
  typing: document.getElementById('typing'),
  status: document.getElementById('status'),
  composer: document.getElementById('composer'),
  messageInput: document.getElementById('message-input'),
  analysisBody: document.getElementById('analysis-body'),
};

function send(event) {
  if (state.ws?.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(event));
  }
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  state.ws = new WebSocket(`${proto}://${location.host}/ws`);
  state.ws.onopen = () => {
    send({
      event: 'hello',
      display_name: state.displayName,
      user_id: state.userId || undefined,
      avatar_color: state.avatarColor || undefined,
    });
  };
  state.ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    handleServer(msg);
  };
  state.ws.onclose = () => {
    // The pending reply died with the socket.
    if (state.analyzing) setAnalyzing(null);
    setTimeout(connect, 1500);
  };
}

function handleServer(msg) {
  switch (msg.event) {
    case 'welcome':
      state.userId = msg.user_id;
      state.displayName = msg.display_name;
      state.avatarColor = msg.avatar_color || state.avatarColor;
      state.avatarInitials = msg.avatar_initials || avatarInitials(msg.display_name);
      saveIdentity(msg.user_id, msg.display_name, state.avatarColor);
      els.you.innerHTML = '';
      els.you.appendChild(
        avatarEl(state.avatarInitials, state.avatarColor, 'sm'),
      );
      els.you.appendChild(
        document.createTextNode(` Signed in as ${msg.display_name}`),
      );
      updateAvatarPreview();
      break;
    case 'rooms':
      state.rooms = msg.rooms;
      renderRooms();
      break;
    case 'room_joined':
      state.currentRoom = msg.room.slug;
      state.messages = new Map(
        msg.history.map((m) => {
          m.timetoken = String(m.timetoken);
          return [m.timetoken, m];
        }),
      );
      state.online = msg.online || [];
      els.roomTitle.textContent = msg.room.name;
      els.roomMeta.textContent = `${msg.room.topic} · ${msg.room.channel}`;
      els.messageInput.disabled = false;
      els.composer.querySelector('button').disabled = false;
      // Leaving the room means its analysis will never reach us, so stop waiting
      // on it rather than blocking the button until the timeout.
      if (state.analyzing && state.analyzing !== msg.room.slug) setAnalyzing(null);
      syncAnalyzeButton();
      renderRooms();
      renderMessages();
      renderOnline();
      if (msg.history.length) {
        const last = msg.history[msg.history.length - 1];
        send({
          event: 'read',
          room: msg.room.slug,
          message_timetoken: last.timetoken,
        });
      }
      break;
    case 'message': {
      if (msg.room !== state.currentRoom) break;
      msg.message.timetoken = String(msg.message.timetoken);
      // The same message can arrive more than once (local fan-out plus the
      // PubNub subscribe loop). Only ack the first sighting of someone else's
      // message, otherwise every duplicate feeds another receipt round trip.
      const isNew = !state.messages.has(msg.message.timetoken);
      state.messages.set(msg.message.timetoken, msg.message);
      renderMessages();
      if (isNew && msg.message.envelope.author_id !== state.userId) {
        send({
          event: 'read',
          room: msg.room,
          message_timetoken: msg.message.timetoken,
        });
      }
      break;
    }
    case 'typing':
      if (msg.room !== state.currentRoom || msg.user_id === state.userId) break;
      if (msg.typing) state.typing.set(msg.user_id, msg.display_name);
      else state.typing.delete(msg.user_id);
      renderTyping();
      break;
    case 'presence':
      if (msg.room !== state.currentRoom) break;
      state.online = msg.online || [];
      renderOnline();
      break;
    case 'reaction':
      if (msg.room !== state.currentRoom) break;
      applyReaction(msg);
      renderMessages();
      break;
    case 'read_receipt':
      if (msg.room !== state.currentRoom) break;
      applyRead(msg);
      renderMessages();
      break;
    case 'message_deleted':
      if (msg.room !== state.currentRoom) break;
      if (state.messages.delete(String(msg.message_timetoken))) renderMessages();
      break;
    case 'message_edited': {
      if (msg.room !== state.currentRoom) break;
      const target = state.messages.get(String(msg.message_timetoken));
      if (!target) break;
      target.envelope.text = msg.text;
      target.edited = true;
      renderMessages();
      break;
    }
    case 'analysis':
      if (msg.room === state.analyzing) setAnalyzing(null);
      if (msg.room !== state.currentRoom) break;
      renderAnalysis(msg.analysis);
      break;
    case 'error':
      console.error(msg.message);
      showStatus(msg.message);
      if (state.analyzing) setAnalyzing(null);
      break;
  }
}

function showStatus(text) {
  if (!els.status) return;
  els.status.textContent = text || '';
  if (text) {
    clearTimeout(showStatus._t);
    showStatus._t = setTimeout(() => {
      els.status.textContent = '';
    }, 5000);
  }
}

function applyReaction(msg) {
  const key = String(msg.message_timetoken);
  const m = state.messages.get(key);
  if (!m) return;
  m.reactions = m.reactions || {};
  const list = new Set(m.reactions[msg.reaction] || []);
  if (msg.added !== false) list.add(msg.user_id);
  else list.delete(msg.user_id);
  m.reactions[msg.reaction] = [...list];
}

function applyRead(msg) {
  const m = state.messages.get(msg.message_timetoken);
  if (!m) return;
  m.readers = m.readers || [];
  if (!m.readers.includes(msg.user_id)) m.readers.push(msg.user_id);
}

function avatarEl(initials, color, sizeClass = '') {
  const span = document.createElement('span');
  span.className = ['avatar', sizeClass].filter(Boolean).join(' ');
  span.style.background = color || DEFAULT_AVATAR_COLOR;
  span.textContent = initials || '?';
  span.setAttribute('aria-hidden', 'true');
  return span;
}

function envelopeAvatar(envelope) {
  const initials =
    envelope.avatar_initials || avatarInitials(envelope.author_name);
  const color = envelope.avatar_color || DEFAULT_AVATAR_COLOR;
  return { initials, color };
}

function renderRooms() {
  els.roomList.innerHTML = '';
  for (const room of state.rooms) {
    const li = document.createElement('li');
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = room.slug === state.currentRoom ? 'active' : '';
    btn.innerHTML = `<strong>${escapeHtml(room.name)}</strong><span class="topic">${escapeHtml(room.topic)} · ${room.members} in room</span>`;
    btn.onclick = () => send({ event: 'join_room', room: room.slug });
    li.appendChild(btn);
    els.roomList.appendChild(li);
  }
}

function renderMessages() {
  const items = [...state.messages.values()].sort((a, b) =>
    a.timetoken.localeCompare(b.timetoken),
  );
  els.messages.innerHTML = '';
  for (const message of items) {
    const mine =
      message.envelope.author_id === state.userId ||
      (!!state.displayName &&
        message.envelope.author_name === state.displayName);
    const { initials, color } = envelopeAvatar(message.envelope);
    const row = document.createElement('div');
    row.className = [
      'message-row',
      mine ? 'mine' : 'theirs',
      message.envelope.kind === 'analysis' ? 'analysis' : '',
    ]
      .filter(Boolean)
      .join(' ');

    const avatar = avatarEl(initials, color);
    avatar.title = message.envelope.author_name;

    const article = document.createElement('article');
    article.className = 'bubble';

    const time = formatTime(message.timetoken);
    const header = document.createElement('header');
    const who = document.createElement('strong');
    who.textContent = mine ? 'You' : message.envelope.author_name;
    header.appendChild(who);
    const timeEl = document.createElement('time');
    timeEl.textContent = time;
    header.appendChild(timeEl);
    article.appendChild(header);

    const p = document.createElement('p');
    p.textContent = message.envelope.message?.text ?? message.envelope.text ?? '';
    if (message.edited) {
      const edited = document.createElement('span');
      edited.className = 'edited-tag';
      edited.textContent = '(edited)';
      edited.title = 'Edited after posting';
      p.appendChild(document.createTextNode(' '));
      p.appendChild(edited);
    }
    article.appendChild(p);

    const reactRow = document.createElement('div');
    reactRow.className = 'react-row';
    for (const emoji of ['🔥', '💡', '👍', '❓']) {
      const reactors = message.reactions?.[emoji] || [];
      const count = reactors.length;
      const mineReact = reactors.includes(state.userId);
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = mineReact ? 'active' : '';
      btn.textContent = count ? `${emoji} ${count}` : emoji;
      btn.onclick = () => {
        applyReaction({
          message_timetoken: message.timetoken,
          reaction: emoji,
          user_id: state.userId,
          added: true,
        });
        renderMessages();
        send({
          event: 'react',
          room: state.currentRoom,
          message_timetoken: String(message.timetoken),
          reaction: emoji,
        });
      };
      reactRow.appendChild(btn);
    }
    article.appendChild(reactRow);

    if (message.readers?.length) {
      const readRow = document.createElement('div');
      readRow.className = 'read-row';
      const label = document.createElement('span');
      label.textContent = `Read by ${message.readers.length}`;
      readRow.appendChild(label);
      article.appendChild(readRow);
    }

    if (mine) {
      row.appendChild(article);
      row.appendChild(avatar);
    } else {
      row.appendChild(avatar);
      row.appendChild(article);
    }
    els.messages.appendChild(row);
  }
  els.messages.scrollTop = els.messages.scrollHeight;
}

function renderTyping() {
  const names = [...state.typing.values()];
  els.typing.textContent = names.length
    ? `${names.join(', ')} ${names.length === 1 ? 'is' : 'are'} typing…`
    : '';
}

function renderOnline() {
  const online = state.online || [];
  els.presence.textContent = `${online.length} online`;
  els.onlineList.innerHTML = '';

  if (!online.length) {
    const empty = document.createElement('li');
    empty.className = 'empty';
    empty.textContent = 'No one in this room yet';
    els.onlineList.appendChild(empty);
    return;
  }

  for (const user of online) {
    const li = document.createElement('li');
    if (user.user_id === state.userId) li.classList.add('me');
    const initials = user.avatar_initials || avatarInitials(user.display_name);
    const color = user.avatar_color || DEFAULT_AVATAR_COLOR;
    li.appendChild(avatarEl(initials, color, 'sm'));
    li.appendChild(
      document.createTextNode(
        user.user_id === state.userId
          ? `${user.display_name} (you)`
          : user.display_name,
      ),
    );
    els.onlineList.appendChild(li);
  }
}

// The server answers an analyze request with either an `analysis` or an `error`,
// but neither arrives if the socket drops or the user leaves the room first
// (`should_forward` only delivers `analysis` to the room it belongs to), so the
// pending state needs its own escape hatch.
const ANALYSIS_TIMEOUT_MS = 90000;
let analysisTimer = null;

function setAnalyzing(room) {
  state.analyzing = room;
  clearTimeout(analysisTimer);
  if (room) {
    analysisTimer = setTimeout(() => {
      setAnalyzing(null);
      showStatus('Analysis timed out. Try again.');
    }, ANALYSIS_TIMEOUT_MS);
  }
  syncAnalyzeButton();
}

function syncAnalyzeButton() {
  const pending = Boolean(state.analyzing);
  els.analyze.disabled = pending || !state.currentRoom;
  els.analyze.innerHTML = pending
    ? '<span class="spinner" aria-hidden="true"></span>Analyzing…'
    : 'Analyze';
  els.analysisBody.setAttribute('aria-busy', String(pending));
}

function renderAnalysisPending(roomName) {
  els.analysisBody.classList.remove('empty');
  els.analysisBody.innerHTML = `
    <p class="analysis-pending">
      <span class="spinner" aria-hidden="true"></span>
      Analyzing ${escapeHtml(roomName)}…
    </p>
    <p class="hint">Reading recent messages and summarizing.</p>
  `;
}

function renderAnalysis(analysis) {
  els.analysisBody.classList.remove('empty');
  els.analysisBody.innerHTML = `
    <p>${escapeHtml(analysis.summary)}</p>
    ${listBlock('Themes', analysis.themes)}
    ${listBlock('Promising ideas', analysis.promising_ideas)}
    ${listBlock('Open questions', analysis.open_questions)}
    ${listBlock('Next steps', analysis.next_steps)}
    <p class="hint">Source: ${escapeHtml(analysis.source)}${analysis.model ? ` (${escapeHtml(analysis.model)})` : ''}</p>
  `;
}

function listBlock(title, items) {
  if (!items?.length) return '';
  return `<h3>${title}</h3><ul>${items.map((i) => `<li>${escapeHtml(i)}</li>`).join('')}</ul>`;
}

function formatTime(timetoken) {
  const n = Number(timetoken);
  if (!Number.isFinite(n) || n <= 0) return '';
  const ms = String(timetoken).length >= 15 ? Math.floor(n / 10000) : n < 1e12 ? n * 1000 : n;
  return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function updateAvatarPreview() {
  if (!els.avatarPreview) return;
  const name = els.displayName.value.trim() || state.displayName || '?';
  state.avatarColor = els.avatarColor?.value || state.avatarColor || DEFAULT_AVATAR_COLOR;
  els.avatarPreview.textContent = avatarInitials(name);
  els.avatarPreview.style.background = state.avatarColor;
}

if (state.displayName) {
  els.displayName.value = state.displayName;
}
if (els.avatarColor) {
  els.avatarColor.value = state.avatarColor || DEFAULT_AVATAR_COLOR;
}
updateAvatarPreview();
els.displayName.addEventListener('input', updateAvatarPreview);
els.avatarColor?.addEventListener('input', updateAvatarPreview);

els.helloForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const name = els.displayName.value.trim();
  if (!name) return;
  if (state.displayName && state.displayName !== name) {
    state.userId = null;
  }
  state.displayName = name;
  state.avatarColor = els.avatarColor?.value || DEFAULT_AVATAR_COLOR;
  state.avatarInitials = avatarInitials(name);
  els.gate.classList.add('hidden');
  els.app.classList.remove('hidden');
  connect();
});

els.createRoom.addEventListener('click', () => {
  const name = prompt('Room name');
  if (!name?.trim()) return;
  const topic = prompt('Topic', name) || name;
  send({ event: 'create_room', name: name.trim(), topic: topic.trim() });
});

els.analyze.addEventListener('click', () => {
  if (!state.currentRoom || state.analyzing) return;
  const room = state.currentRoom;
  send({ event: 'analyze', room });
  setAnalyzing(room);
  renderAnalysisPending(state.rooms.find((r) => r.slug === room)?.name || room);
});

let typingTimer = null;
els.messageInput.addEventListener('input', () => {
  if (!state.currentRoom) return;
  send({ event: 'typing', room: state.currentRoom, typing: true });
  clearTimeout(typingTimer);
  typingTimer = setTimeout(() => {
    send({ event: 'typing', room: state.currentRoom, typing: false });
  }, 1200);
});

els.composer.addEventListener('submit', (e) => {
  e.preventDefault();
  const text = els.messageInput.value.trim();
  if (!text || !state.currentRoom) return;
  send({ event: 'send_message', room: state.currentRoom, text });
  els.messageInput.value = '';
  send({ event: 'typing', room: state.currentRoom, typing: false });
});
