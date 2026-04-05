import { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import './App.css';

// Configure base URLs for all 5 nodes
const NODES = [
  { id: 'Node 1', url: 'http://localhost:5001' },
  { id: 'Node 2', url: 'http://localhost:5002' },
  { id: 'Node 3', url: 'http://localhost:5003' },
  { id: 'Node 4', url: 'http://localhost:5004' },
  { id: 'Node 5', url: 'http://localhost:5005' },
];

const API_BASE = 'http://localhost:5001'; // default entry point

function App() {
  // --- State ---
  const [nodeStatuses, setNodeStatuses] = useState(
    NODES.map((n) => ({ ...n, online: false, leader: null, messageCount: 0 }))
  );
  const [messages, setMessages] = useState([]);
  const [sender, setSender] = useState('');
  const [receiver, setReceiver] = useState('');
  const [content, setContent] = useState('');
  const [sending, setSending] = useState(false);
  const [toast, setToast] = useState(null);
  const [editingId, setEditingId] = useState(null);
  const [editContent, setEditContent] = useState('');

  // --- Toast ---
  const showToast = useCallback((message, type = 'success') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  }, []);

  // --- Fetch node statuses ---
  const fetchStatuses = useCallback(async () => {
    const results = await Promise.allSettled(
      NODES.map((node) =>
        axios.get(`${node.url}/`, { timeout: 2000 }).then((res) => ({
          ...node,
          online: true,
          leader: res.data.leader,
          messageCount: res.data.messages,
        }))
      )
    );

    setNodeStatuses(
      results.map((r, i) =>
        r.status === 'fulfilled'
          ? r.value
          : { ...NODES[i], online: false, leader: null, messageCount: 0 }
      )
    );
  }, []);

  // --- Fetch messages ---
  const fetchMessages = useCallback(async () => {
    try {
      const res = await axios.get(`${API_BASE}/messages`, { timeout: 5000 });
      if (res.data.messages) {
        setMessages(res.data.messages);
      }
    } catch {
      // silently fail — messages section will display empty
    }
  }, []);

  // --- Send message ---
  const handleSend = async (e) => {
    e.preventDefault();
    if (!content.trim()) return;

    setSending(true);
    try {
      const res = await axios.post(
        `${API_BASE}/send`,
        {
          sender: sender.trim() || 'Anonymous',
          receiver: receiver.trim() || 'All',
          content: content.trim(),
        },
        { timeout: 10000 }
      );

      if (res.data.error) {
        showToast(res.data.error, 'error');
      } else {
        showToast('Message sent successfully');
        setContent('');
        // Refresh messages & statuses after sending
        await Promise.all([fetchMessages(), fetchStatuses()]);
      }
    } catch (err) {
      showToast('Failed to send message', 'error');
    } finally {
      setSending(false);
    }
  };

  // --- Start editing a message ---
  const handleEdit = (msg) => {
    setEditingId(msg.id);
    setEditContent(msg.content);
  };

  // --- Cancel editing ---
  const handleCancelEdit = () => {
    setEditingId(null);
    setEditContent('');
  };

  // --- Save edited message ---
  const handleSaveEdit = async (id) => {
    if (!editContent.trim()) return;
    try {
      const res = await axios.put(
        `${API_BASE}/edit`,
        { id, content: editContent.trim() },
        { timeout: 10000 }
      );
      if (res.data.error) {
        showToast(res.data.error, 'error');
      } else {
        showToast('Message updated successfully');
        setEditingId(null);
        setEditContent('');
        await fetchMessages();
      }
    } catch {
      showToast('Failed to update message', 'error');
    }
  };

  // --- Initial load & polling ---
  useEffect(() => {
    fetchStatuses();
    fetchMessages();
    const interval = setInterval(() => {
      fetchStatuses();
    }, 5000);
    return () => clearInterval(interval);
  }, [fetchStatuses, fetchMessages]);

  // --- Determine current leader URL ---
  const leaderUrl = nodeStatuses.find((n) => n.online && n.leader)?.leader;

  // --- Format timestamp ---
  const formatTime = (ts) => {
    if (!ts) return '';
    try {
      const d = new Date(ts);
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch {
      return ts;
    }
  };

  return (
    <div className="app">
      {/* Header */}
      <header className="header">
        <h1 className="header__title">
          <span className="header__icon">⚡</span>
          Distributed Messaging System
        </h1>
        <p className="header__subtitle">
          Real-time message routing with Raft consensus &amp; quorum replication
        </p>
      </header>

      {/* ===== Node Status Panel ===== */}
      <section className="panel" id="node-status-panel">
        <div className="panel__header">
          <h2 className="panel__title">
            <span className="panel__title-icon panel__title-icon--blue">⬡</span>
            Cluster Nodes
          </h2>
          <span className="status-chip">
            {nodeStatuses.filter((n) => n.online).length} / {NODES.length} online
          </span>
        </div>

        <div className="nodes-grid">
          {nodeStatuses.map((node) => {
            const isLeader = node.online && node.leader === node.url;
            return (
              <div
                key={node.id}
                className={`node-card${isLeader ? ' node-card--leader' : ''}${!node.online ? ' node-card--offline' : ''
                  }`}
              >
                <div className="node-card__name">{node.id}</div>
                <div
                  className={`node-card__status ${node.online ? 'node-card__status--online' : 'node-card__status--offline'
                    }`}
                >
                  <span
                    className={`node-card__dot ${node.online ? 'node-card__dot--online' : 'node-card__dot--offline'
                      }`}
                  />
                  {node.online ? 'Online' : 'Offline'}
                </div>
                {isLeader && <div className="node-card__leader-badge">★ Leader</div>}
              </div>
            );
          })}
        </div>
      </section>

      {/* ===== Main Grid: Send + Messages ===== */}
      <div className="main-grid">
        {/* Send Message Panel */}
        <section className="panel" id="send-message-panel">
          <div className="panel__header">
            <h2 className="panel__title">
              <span className="panel__title-icon panel__title-icon--green">↗</span>
              Send Message
            </h2>
          </div>

          <form onSubmit={handleSend}>
            <div className="form-group">
              <label className="form-label" htmlFor="sender-input">
                Sender
              </label>
              <input
                id="sender-input"
                className="form-input"
                type="text"
                placeholder="e.g. Alice"
                value={sender}
                onChange={(e) => setSender(e.target.value)}
              />
            </div>

            <div className="form-group">
              <label className="form-label" htmlFor="receiver-input">
                Receiver
              </label>
              <input
                id="receiver-input"
                className="form-input"
                type="text"
                placeholder="e.g. Bob"
                value={receiver}
                onChange={(e) => setReceiver(e.target.value)}
              />
            </div>

            <div className="form-group">
              <label className="form-label" htmlFor="message-input">
                Message
              </label>
              <textarea
                id="message-input"
                className="form-input"
                placeholder="Type your message…"
                rows={3}
                value={content}
                onChange={(e) => setContent(e.target.value)}
              />
            </div>

            <button
              id="send-btn"
              className="btn btn--primary"
              type="submit"
              disabled={sending || !content.trim()}
            >
              {sending ? '⏳ Sending…' : '📨 Send Message'}
            </button>
          </form>
        </section>

        {/* Messages Display Panel */}
        <section className="panel" id="messages-panel">
          <div className="panel__header">
            <h2 className="panel__title">
              <span className="panel__title-icon panel__title-icon--purple">✉</span>
              Messages
              {messages.length > 0 && (
                <span className="status-chip">{messages.length}</span>
              )}
            </h2>
            <button
              id="refresh-btn"
              className="btn btn--secondary"
              onClick={() => {
                fetchMessages();
                fetchStatuses();
              }}
            >
              ↻ Refresh
            </button>
          </div>

          <div className="messages-scroll">
            {messages.length === 0 ? (
              <div className="empty-state">
                <div className="empty-state__icon">💬</div>
                <div className="empty-state__text">
                  No messages yet — send one to get started
                </div>
              </div>
            ) : (
              [...messages].reverse().map((msg) => (
                <div key={msg.id} className="message-card">
                  <div className="message-card__header">
                    <div className="message-card__route">
                      <span>{msg.sender}</span>
                      <span className="message-card__arrow">→</span>
                      <span>{msg.receiver}</span>
                    </div>
                    <div className="message-card__actions">
                      <span className="message-card__time">
                        Last updated: {formatTime(msg.timestamp)}
                      </span>
                      {msg.edited && (
                        <span className="message-card__edited-badge">Edited</span>
                      )}
                      {editingId !== msg.id && (
                        <button
                          className="btn btn--edit"
                          onClick={() => handleEdit(msg)}
                        >
                          ✎ Edit
                        </button>
                      )}
                    </div>
                  </div>

                  {editingId === msg.id ? (
                    <div className="message-card__edit">
                      <input
                        className="form-input"
                        type="text"
                        value={editContent}
                        onChange={(e) => setEditContent(e.target.value)}
                        autoFocus
                      />
                      <div className="message-card__edit-btns">
                        <button
                          className="btn btn--save"
                          onClick={() => handleSaveEdit(msg.id)}
                          disabled={!editContent.trim()}
                        >
                          ✓ Save
                        </button>
                        <button
                          className="btn btn--cancel"
                          onClick={handleCancelEdit}
                        >
                          ✕ Cancel
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="message-card__content">{msg.content}</div>
                  )}
                </div>
              ))
            )}
          </div>
        </section>
      </div>

      {/* Toast */}
      {toast && (
        <div className={`toast toast--${toast.type}`}>{toast.message}</div>
      )}
    </div>
  );
}

export default App;
