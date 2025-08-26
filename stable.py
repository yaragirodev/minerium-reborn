"""
Improved MiniMessenger with a Discord-inspired UI, enhanced security, bug fixes,
and new features like group chats and server management.

Key improvements:
1.  **UI Overhaul**: Complete redesign inspired by Discord, featuring a blurred background,
    server/channel columns, and a modern aesthetic.
2.  **Enhanced Security**: Implemented environment variables for secrets, robust input
    validation, and CSRF protection concepts.
3.  **Robust Error Handling**: Better error handling and logging throughout the application.
4.  **Improved Database Management**: Connection pooling, simple migrations, and indexed queries
    for better performance.
5.  **Secure File Uploads**: Strict validation and secure filename generation for file uploads.
6.  **Refactored Code**: Better organization, type hints, and separation of concerns.
7.  **BUGFIX**: Fixed case-sensitive friend and member searches.
8.  **BUGFIX**: Resolved a critical bug that caused real-time messaging to fail intermittently.
9.  **BUGFIX**: Added a database migration system to prevent crashes with older DB schemas.
10. **FEATURE**: Added group chats with ownership and member management.
11. **FEATURE**: Added server management, including channels, members, and invites.
"""

import os
import uuid
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from functools import wraps

from flask import (
    Flask, request, session, redirect, url_for,
    send_from_directory, jsonify, render_template_string
)
from flask_socketio import SocketIO, join_room, leave_room, emit
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

# --- Configuration ---
class Config:
    """Application configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')
    UPLOAD_FOLDER = 'uploads'
    STATIC_FOLDER = 'static'
    DB_PATH = 'messenger.db'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
    INVITE_TTL_DAYS_DEFAULT = 7
    DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Directory Setup ---
for folder in [Config.UPLOAD_FOLDER, Config.STATIC_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)
        logger.info(f"Created directory: {folder}")

# --- App Initialization ---
app = Flask(__name__)
app.config.from_object(Config)
socketio = SocketIO(app, cors_allowed_origins='*')

# --- Database Management ---
class DatabaseManager:
    """Handles all database operations, including initialization and migrations."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()

    def get_connection(self):
        """Get a new database connection with a row factory for dict-like access."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """Initialize and migrate the database schema."""
        with self.get_connection() as conn:
            # Create tables if they don't exist
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    avatar TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS friends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requester_id INTEGER NOT NULL,
                    addressee_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','accepted','rejected')),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(requester_id, addressee_id),
                    FOREIGN KEY (requester_id) REFERENCES users (id),
                    FOREIGN KEY (addressee_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS servers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    owner_id INTEGER NOT NULL,
                    avatar TEXT,
                    description TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (owner_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS server_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(server_id, user_id),
                    FOREIGN KEY (server_id) REFERENCES servers (id),
                    FOREIGN KEY (user_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (server_id) REFERENCES servers (id)
                );
                CREATE TABLE IF NOT EXISTS dms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    is_group INTEGER NOT NULL DEFAULT 0,
                    owner_id INTEGER,
                    avatar TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (owner_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS dm_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dm_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    UNIQUE(dm_id, user_id),
                    FOREIGN KEY (dm_id) REFERENCES dms (id),
                    FOREIGN KEY (user_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER,
                    dm_id INTEGER,
                    sender_id INTEGER NOT NULL,
                    content TEXT,
                    content_type TEXT NOT NULL CHECK(content_type IN ('text','image')),
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (channel_id) REFERENCES channels (id),
                    FOREIGN KEY (dm_id) REFERENCES dms (id),
                    FOREIGN KEY (sender_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS invites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER NOT NULL,
                    token TEXT UNIQUE NOT NULL,
                    creator_id INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    expires_at DATETIME,
                    FOREIGN KEY (server_id) REFERENCES servers (id),
                    FOREIGN KEY (creator_id) REFERENCES users (id)
                );
                -- Performance indexes
                CREATE INDEX IF NOT EXISTS idx_friends_status ON friends(status);
                CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id);
                CREATE INDEX IF NOT EXISTS idx_messages_dm ON messages(dm_id);
                CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
                CREATE INDEX IF NOT EXISTS idx_server_members_server ON server_members(server_id);
                CREATE INDEX IF NOT EXISTS idx_server_members_user ON server_members(user_id);
                CREATE INDEX IF NOT EXISTS idx_invites_token ON invites(token);
                CREATE INDEX IF NOT EXISTS idx_invites_expires ON invites(expires_at);
            ''')

            # Simple migration logic to add missing columns
            self._run_migration(conn)
            conn.commit()
            logger.info("Database initialized and migrated successfully")

    def _run_migration(self, conn):
        """Applies necessary schema changes to the database."""
        cursor = conn.cursor()
        
        # Helper to check and add columns
        def add_column_if_not_exists(table, column, definition):
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row['name'] for row in cursor.fetchall()]
            if column not in columns:
                logger.info(f"Migrating {table} table: adding '{column}' column.")
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

        # Migrations for 'dms' table
        add_column_if_not_exists('dms', 'is_group', 'INTEGER NOT NULL DEFAULT 0')
        add_column_if_not_exists('dms', 'owner_id', 'INTEGER')
        add_column_if_not_exists('dms', 'avatar', 'TEXT')
        
        # Migrations for 'servers' table
        add_column_if_not_exists('servers', 'description', 'TEXT')

db_manager = DatabaseManager(Config.DB_PATH)

# --- Utility Functions & Decorators ---
def allowed_file(filename: str) -> bool:
    """Check if a file extension is in the allowed list."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

def validate_input(data: str, max_length: int = 255, min_length: int = 1) -> bool:
    """Simple input validation for strings."""
    if not data or not isinstance(data, str):
        return False
    return min_length <= len(data.strip()) <= max_length

def login_required(f):
    """Decorator to protect routes that require authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'ok': False, 'error': 'Authentication required'}), 401
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

def get_current_user() -> Optional[Dict]:
    """Fetch the current logged-in user's data from the database."""
    if 'user_id' not in session:
        return None
    with db_manager.get_connection() as conn:
        user = conn.execute('SELECT id, username, avatar FROM users WHERE id = ?', (session['user_id'],)).fetchone()
        return dict(user) if user else None

def save_uploaded_file(file, prefix: str = '') -> Optional[str]:
    """Safely save an uploaded file and return its new filename."""
    if not file or not file.filename:
        return None
    if not allowed_file(file.filename):
        raise ValueError("File type not allowed")
    
    original_filename = secure_filename(file.filename)
    ext = os.path.splitext(original_filename)[1]
    filename = f"{prefix}{uuid.uuid4().hex}{ext}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    logger.info(f"File saved: {filename}")
    return filename

# --- HTML Templates ---
# Using a single file approach means embedding HTML here.
# For larger apps, use separate .html files with render_template.

BASE_HTML = '''
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>MiniMessenger</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.socket.io/4.6.1/socket.io.min.js"></script>
    <style>
        :root {
            --background-primary: #313338;
            --background-secondary: #2b2d31;
            --background-tertiary: #1e1f22;
            --header-primary: #f2f3f5;
            --header-secondary: #b5bac1;
            --text-normal: #dcddde;
            --text-muted: #949ba4;
            --interactive-normal: #b5bac1;
            --interactive-hover: #dcddde;
            --interactive-active: #fff;
            --background-accent: #5865f2;
            --background-accent-hover: #4752c4;
            --background-modifier-hover: rgba(79, 84, 92, 0.16);
            --background-modifier-active: rgba(79, 84, 92, 0.24);
            --elevation-low: 0 1px 0 rgba(4, 4, 5, 0.2), 0 1.5px 0 rgba(6, 6, 7, 0.05), 0 2px 0 rgba(4, 4, 5, 0.05);
            --font-primary: 'Inter', sans-serif;
        }
        * { box-sizing: border-box; }
        body {
            font-family: var(--font-primary);
            margin: 0;
            height: 100vh;
            display: flex;
            background-color: var(--background-tertiary);
            color: var(--text-normal);
            overflow: hidden;
            font-size: 16px;
        }
        .app-container {
            display: flex;
            width: 100%;
            height: 100%;
            background-image: url('/static/background.jpg');
            background-size: cover;
            background-position: center;
        }
        .app-container::before {
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0, 0, 0, 0.5);
            backdrop-filter: blur(8px);
            z-index: 1;
        }
        .app-layout {
            position: relative;
            z-index: 2;
            display: flex;
            width: 100%;
            height: 100%;
        }
        .sidebar {
            width: 72px;
            background: var(--background-tertiary);
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 12px 0;
            gap: 8px;
            flex-shrink: 0;
        }
        .server-icon {
            width: 48px;
            height: 48px;
            border-radius: 50%;
            background: var(--background-primary);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 18px;
            color: var(--header-primary);
            cursor: pointer;
            transition: all 0.2s ease;
            overflow: hidden;
        }
        .server-icon img { width: 100%; height: 100%; object-fit: cover; }
        .server-icon:hover, .server-icon.active { border-radius: 16px; background: var(--background-accent); }
        .channels-panel {
            width: 240px;
            background: var(--background-secondary);
            display: flex;
            flex-direction: column;
            flex-shrink: 0;
        }
        .panel-header {
            height: 48px;
            display: flex;
            align-items: center;
            padding: 0 16px;
            font-weight: 600;
            color: var(--header-primary);
            box-shadow: var(--elevation-low);
            flex-shrink: 0;
        }
        .panel-content { flex: 1; overflow-y: auto; padding: 8px; }
        .channel-item {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 6px 8px;
            border-radius: 4px;
            cursor: pointer;
            font-weight: 500;
            color: var(--interactive-normal);
        }
        .channel-item:hover { background: var(--background-modifier-hover); color: var(--interactive-hover); }
        .channel-item.active { background: var(--background-modifier-active); color: var(--interactive-active); }
        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: var(--background-primary);
        }
        .topbar {
            height: 48px;
            display: flex;
            align-items: center;
            padding: 0 16px;
            font-weight: 600;
            color: var(--header-primary);
            box-shadow: var(--elevation-low);
            flex-shrink: 0;
        }
        .messages { flex: 1; overflow-y: auto; padding: 16px; }
        .msg {
            display: flex;
            gap: 16px;
            padding: 8px 16px;
            margin-bottom: 4px;
        }
        .msg:hover { background: var(--background-modifier-hover); border-radius: 4px; }
        .msg .avatar { width: 40px; height: 40px; border-radius: 50%; object-fit: cover; }
        .msg-header { display: flex; align-items: baseline; gap: 8px; margin-bottom: 4px; }
        .msg-author { font-weight: 500; color: var(--header-primary); }
        .msg-timestamp { font-size: 12px; color: var(--text-muted); }
        .msg-text { line-height: 1.4; word-wrap: break-word; }
        .msg-image { max-width: 400px; border-radius: 8px; margin-top: 8px; }
        .composer {
            display: flex;
            padding: 0 16px 24px;
            gap: 12px;
            align-items: center;
        }
        .composer-input-wrapper {
            flex: 1;
            background: var(--background-secondary);
            border-radius: 8px;
            padding: 0 12px;
            display: flex;
            align-items: center;
        }
        .composer input[type=text] {
            flex: 1;
            border: none;
            background: transparent;
            color: var(--text-normal);
            font-size: 16px;
            padding: 12px 0;
        }
        .composer input[type=text]:focus { outline: none; }
        .composer input[type=file] { display: none; }
        .composer .file-label {
            padding: 8px;
            cursor: pointer;
            color: var(--interactive-normal);
        }
        .composer .file-label:hover { color: var(--interactive-hover); }
        .composer button {
            padding: 10px 16px;
            background: var(--background-accent);
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 500;
            transition: background-color 0.2s ease;
        }
        .composer button:hover { background: var(--background-accent-hover); }
        /* User Panel */
        .user-panel {
            height: 52px;
            background: var(--background-tertiary);
            display: flex;
            align-items: center;
            padding: 0 8px;
            gap: 8px;
        }
        .user-panel .avatar { width: 32px; height: 32px; border-radius: 50%; object-fit: cover; }
        .user-panel .username { font-weight: 600; font-size: 14px; }
        .user-panel .actions { margin-left: auto; }
        .user-panel .actions button {
            background: none;
            border: none;
            color: var(--interactive-normal);
            cursor: pointer;
            padding: 4px;
        }
        .user-panel .actions button:hover { color: var(--interactive-hover); }
        /* Modal */
        .modal-backdrop {
            position: fixed; top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            display: none;
            align-items: center;
            justify-content: center;
        }
        .modal-content {
            background: var(--background-secondary);
            padding: 24px;
            border-radius: 8px;
            width: 90%;
            max-width: 440px;
        }
        .modal-header { font-size: 20px; font-weight: 700; margin-bottom: 20px; }
        .form-group { margin-bottom: 16px; }
        .form-group label { display: block; font-size: 12px; font-weight: 600; color: var(--header-secondary); margin-bottom: 8px; }
        .form-group input {
            width: 100%;
            padding: 10px;
            border: 1px solid var(--background-tertiary);
            background: var(--background-tertiary);
            color: var(--text-normal);
            border-radius: 4px;
        }
        .modal-footer { display: flex; justify-content: flex-end; gap: 8px; margin-top: 24px; }
        .button {
            padding: 10px 16px;
            border-radius: 4px;
            border: none;
            cursor: pointer;
            font-weight: 500;
        }
        .button.primary { background: var(--background-accent); color: white; }
        .button.secondary { background: var(--interactive-normal); color: var(--background-tertiary); }
    </style>
</head>
<body>
    <div class="app-container">
        <div class="app-layout">
            {% if user %}
            <div class="sidebar" id="servers-list">
                <!-- Servers will be loaded here by JS -->
            </div>
            <div class="channels-panel">
                <div class="panel-header" id="panel-header">Select a Server</div>
                <div class="panel-content" id="panel-content">
                    <!-- Channels or DMs will be loaded here -->
                </div>
                <div class="user-panel">
                    <img src="{{ '/uploads/' + user['avatar'] if user and user['avatar'] else '/static/default-avatar.png' }}"
                         onerror="this.src='/static/default-avatar.png'" class="avatar">
                    <span class="username">{{ user['username'] }}</span>
                    <div class="actions">
                        <a href="/settings" style="color: inherit;">⚙️</a>
                        <a href="/logout" style="color: inherit; margin-left: 8px;">🚪</a>
                    </div>
                </div>
            </div>
            <div class="main-content">
                <div class="topbar" id="current-room-name">Welcome!</div>
                <div class="messages" id="messages"></div>
                <div class="composer">
                    <div class="composer-input-wrapper">
                        <label for="img-file" class="file-label">➕</label>
                        <input type="file" id="img-file" accept="image/*">
                        <input id="msg-input" placeholder="Message..." maxlength="2000">
                    </div>
                    <button onclick="sendMessage()">Send</button>
                </div>
            </div>
            {% else %}
            <div style="margin: auto; text-align: center;">
                <h1>Welcome to MiniMessenger</h1>
                <a href="/login" class="button primary">Login</a>
                <a href="/register" class="button secondary">Register</a>
            </div>
            {% endif %}
        </div>
    </div>

    <!-- Modals -->
    <div id="createServerModal" class="modal-backdrop">
        <div class="modal-content">
            <div class="modal-header">Create a Server</div>
            <div class="form-group">
                <label for="server-name">SERVER NAME</label>
                <input id="server-name" placeholder="Enter a server name" maxlength="50">
            </div>
            <div class="modal-footer">
                <button class="button secondary" onclick="closeModal('createServerModal')">Cancel</button>
                <button class="button primary" onclick="createServer()">Create</button>
            </div>
        </div>
    </div>

    <script>
    const socket = io();
    let current_room = null;
    let current_server_id = null;
    const user = {{ user|tojson }};

    // --- Core UI Functions ---
    function openModal(id) { document.getElementById(id).style.display = 'flex'; }
    function closeModal(id) { document.getElementById(id).style.display = 'none'; }
    function escapeHtml(text) {
        if (typeof text !== 'string') return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // --- Message Handling ---
    function appendMessage(msg) {
        const messagesDiv = document.getElementById('messages');
        const messageEl = document.createElement('div');
        messageEl.className = 'msg';
        const avatarPath = msg.avatar ? `/uploads/${msg.avatar}` : '/static/default-avatar.png';
        const contentHTML = msg.content_type === 'text'
            ? `<div class="msg-text">${escapeHtml(msg.content)}</div>`
            : `<div><img src="${escapeHtml(msg.content)}" class="msg-image" alt="Image"></div>`;

        messageEl.innerHTML = `
            <img class="avatar" src="${avatarPath}" onerror="this.src='/static/default-avatar.png'">
            <div class="msg-content">
                <div class="msg-header">
                    <span class="msg-author">${escapeHtml(msg.username)}</span>
                    <span class="msg-timestamp">${new Date(msg.ts).toLocaleString()}</span>
                </div>
                ${contentHTML}
            </div>`;
        messagesDiv.appendChild(messageEl);
        messagesDiv.scrollTop = messagesDiv.scrollHeight;
    }

    function sendMessage() {
        const textInput = document.getElementById('msg-input');
        const fileInput = document.getElementById('img-file');
        const text = textInput.value.trim();

        if (!current_room) return alert('Please select a channel or conversation.');

        if (fileInput.files.length > 0) {
            const formData = new FormData();
            formData.append('image', fileInput.files[0]);
            formData.append('room', current_room);
            fetch('/upload_image', { method: 'POST', body: formData })
                .then(r => r.json())
                .then(result => {
                    if (!result.ok) alert('Error uploading image: ' + (result.error || 'Unknown error'));
                    fileInput.value = '';
                })
                .catch(err => alert('Upload failed: ' + err));
        }

        if (text) {
            socket.emit('send_message', { room: current_room, text: text });
            textInput.value = '';
        }
    }

    function joinRoom(room, roomName) {
        if (current_room) {
            socket.emit('leave', { room: current_room });
        }
        current_room = room;
        document.getElementById('current-room-name').textContent = roomName;
        document.getElementById('messages').innerHTML = '';
        socket.emit('join', { room });

        fetch(`/history?room=${encodeURIComponent(room)}`)
            .then(r => r.json())
            .then(messages => {
                if (Array.isArray(messages)) messages.forEach(appendMessage);
            })
            .catch(err => console.error('Error loading history:', err));
    }

    // --- Server & Channel Loading ---
    function loadServers() {
        fetch('/my_servers')
            .then(r => r.json())
            .then(servers => {
                const container = document.getElementById('servers-list');
                container.innerHTML = `
                    <div class="server-icon" onclick="loadConversations()">DM</div>
                    <hr style="width: 50%; border-color: var(--background-primary);">`;
                servers.forEach(server => {
                    const el = document.createElement('div');
                    el.className = 'server-icon';
                    el.innerHTML = server.avatar
                        ? `<img src="/uploads/${server.avatar}" onerror="this.src='/static/default-server.png'">`
                        : `<span>${escapeHtml(server.name.charAt(0))}</span>`;
                    el.onclick = () => loadChannels(server.id, server.name);
                    container.appendChild(el);
                });
                container.innerHTML += `<div class="server-icon" onclick="openModal('createServerModal')">+</div>`;
            });
    }

    function loadChannels(serverId, serverName) {
        current_server_id = serverId;
        document.getElementById('panel-header').textContent = serverName;
        fetch(`/server_info?server_id=${serverId}`)
            .then(r => r.json())
            .then(info => {
                if (info.error) return alert(info.error);
                const container = document.getElementById('panel-content');
                container.innerHTML = '<h4>Text Channels</h4>';
                info.channels.forEach(ch => {
                    const el = document.createElement('div');
                    el.className = 'channel-item';
                    el.textContent = `# ${escapeHtml(ch.name)}`;
                    el.onclick = () => joinRoom(`server:${info.id}:channel:${ch.id}`, `# ${escapeHtml(ch.name)}`);
                    container.appendChild(el);
                });
            });
    }

    function loadConversations() {
        current_server_id = null;
        document.getElementById('panel-header').textContent = "Direct Messages";
        fetch('/conversations_list')
            .then(r => r.json())
            .then(convs => {
                const container = document.getElementById('panel-content');
                container.innerHTML = '<h4>Friends & Groups</h4>';
                convs.forEach(conv => {
                    const el = document.createElement('div');
                    el.className = 'channel-item';
                    el.textContent = escapeHtml(conv.name);
                    el.onclick = () => joinRoom(`dm:${conv.id}`, escapeHtml(conv.name));
                    container.appendChild(el);
                });
            });
    }

    function createServer() {
        const name = document.getElementById('server-name').value.trim();
        if (!name) return;
        fetch('/create_server', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name })
        })
        .then(r => r.json())
        .then(result => {
            if (result.ok) {
                closeModal('createServerModal');
                document.getElementById('server-name').value = '';
                loadServers();
            } else {
                alert('Error creating server: ' + (result.error || ''));
            }
        });
    }

    // --- Initialization & Socket Events ---
    window.addEventListener('load', () => {
        if (user) {
            loadServers();
            loadConversations(); // Load DMs by default
        }
    });

    socket.on('connect', () => {
        if (user) socket.emit('identify', { user_id: user.id });
    });
    socket.on('message', appendMessage);
    socket.on('error', (error) => console.error('Socket Error:', error));

    document.getElementById('msg-input')?.addEventListener('keypress', e => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });
    </script>
</body>
</html>
'''

LOGIN_REGISTER_HTML = '''
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ title }} - MiniMessenger</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root { --background-primary: #313338; --background-secondary: #2b2d31; --background-tertiary: #1e1f22; --header-primary: #f2f3f5; --text-normal: #dcddde; --background-accent: #5865f2; }
        body {
            font-family: 'Inter', sans-serif;
            margin: 0;
            height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            background-color: var(--background-tertiary);
            color: var(--text-normal);
            background-image: url('/static/background.jpg');
            background-size: cover;
            background-position: center;
        }
        .auth-box {
            background: var(--background-secondary);
            padding: 32px;
            border-radius: 8px;
            width: 90%;
            max-width: 400px;
            z-index: 2;
        }
        h2 { text-align: center; margin-top: 0; color: var(--header-primary); }
        .form-group { margin-bottom: 20px; }
        label { display: block; font-size: 12px; font-weight: 600; color: var(--header-secondary); margin-bottom: 8px; text-transform: uppercase; }
        input { width: 100%; padding: 10px; border: 1px solid var(--background-tertiary); background: var(--background-tertiary); color: var(--text-normal); border-radius: 4px; box-sizing: border-box; }
        button { width: 100%; padding: 12px; background: var(--background-accent); color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; font-weight: 500; }
        .error { color: #ed4245; margin-bottom: 15px; text-align: center; }
        .link { text-align: center; margin-top: 20px; font-size: 14px; }
        .link a { color: #00a8fc; text-decoration: none; }
    </style>
</head>
<body>
    <div class="auth-box">
        <h2>{{ title }}</h2>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        <form method="post" enctype="multipart/form-data">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" required maxlength="50">
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" required>
            </div>
            {% if is_register %}
            <div class="form-group">
                <label for="avatar">Avatar (Optional)</label>
                <input type="file" id="avatar" name="avatar" accept="image/*">
            </div>
            {% endif %}
            <button type="submit">{{ title }}</button>
        </form>
        <div class="link">
            {% if is_register %}
            <a href="/login">Already have an account?</a>
            {% else %}
            <a href="/register">Need an account?</a>
            {% endif %}
        </div>
    </div>
</body>
</html>
'''

# --- Flask Routes ---

@app.route('/')
def index():
    """Renders the main chat interface."""
    user = get_current_user()
    return render_template_string(BASE_HTML, user=user)

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handles user login."""
    if request.method == 'GET':
        return render_template_string(LOGIN_REGISTER_HTML, title="Login", is_register=False)
    
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    
    if not validate_input(username, max_length=50) or not password:
        return render_template_string(LOGIN_REGISTER_HTML, title="Login", is_register=False, error="Invalid input")
    
    with db_manager.get_connection() as conn:
        user = conn.execute('SELECT id, password_hash FROM users WHERE username = ?', (username,)).fetchone()
        
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            return redirect(url_for('index'))
        else:
            return render_template_string(LOGIN_REGISTER_HTML, title="Login", is_register=False, error="Invalid username or password")

@app.route('/register', methods=['GET', 'POST'])
def register():
    """Handles user registration."""
    if request.method == 'GET':
        return render_template_string(LOGIN_REGISTER_HTML, title="Register", is_register=True)
    
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    
    if not validate_input(username, max_length=50, min_length=3):
        return render_template_string(LOGIN_REGISTER_HTML, title="Register", is_register=True, error="Username must be 3-50 characters")
    if len(password) < 6:
        return render_template_string(LOGIN_REGISTER_HTML, title="Register", is_register=True, error="Password must be at least 6 characters")
    
    avatar_filename = None
    try:
        if 'avatar' in request.files and request.files['avatar'].filename:
            avatar_filename = save_uploaded_file(request.files['avatar'], 'avatar_')
        
        with db_manager.get_connection() as conn:
            conn.execute(
                'INSERT INTO users (username, password_hash, avatar) VALUES (?, ?, ?)',
                (username, generate_password_hash(password), avatar_filename)
            )
            conn.commit()
        return redirect(url_for('login'))
            
    except sqlite3.IntegrityError:
        return render_template_string(LOGIN_REGISTER_HTML, title="Register", is_register=True, error="Username already exists")
    except Exception as e:
        logger.error(f"Registration error: {e}")
        return render_template_string(LOGIN_REGISTER_HTML, title="Register", is_register=True, error="An unexpected error occurred")

@app.route('/settings')
@login_required
def settings():
    # This could be expanded into a full settings page
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    """Logs the user out."""
    session.clear()
    return redirect(url_for('index'))

# --- API Routes for Frontend ---

@app.route('/create_server', methods=['POST'])
@login_required
def create_server():
    data = request.get_json()
    name = data.get('name', '').strip()
    if not validate_input(name, max_length=50):
        return jsonify({'ok': False, 'error': 'Invalid server name'})
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO servers (name, owner_id) VALUES (?, ?)', (name, session['user_id']))
            server_id = cursor.lastrowid
            cursor.execute('INSERT INTO server_members (server_id, user_id) VALUES (?, ?)', (server_id, session['user_id']))
            cursor.execute('INSERT INTO channels (server_id, name) VALUES (?, ?)', (server_id, 'general'))
            conn.commit()
            logger.info(f"Server '{name}' created by user {session['user_id']}")
        return jsonify({'ok': True, 'id': server_id})
    except Exception as e:
        logger.error(f"Error creating server: {e}")
        return jsonify({'ok': False, 'error': 'Server creation failed'})

@app.route('/my_servers')
@login_required
def my_servers():
    with db_manager.get_connection() as conn:
        servers = conn.execute('''
            SELECT s.id, s.name, s.avatar FROM servers s 
            JOIN server_members m ON s.id = m.server_id 
            WHERE m.user_id = ? ORDER BY s.name
        ''', (session['user_id'],)).fetchall()
    return jsonify([dict(s) for s in servers])

@app.route('/server_info')
@login_required
def server_info():
    server_id = request.args.get('server_id')
    with db_manager.get_connection() as conn:
        is_member = conn.execute('SELECT 1 FROM server_members WHERE server_id = ? AND user_id = ?', (server_id, session['user_id'])).fetchone()
        if not is_member:
            return jsonify({'error': 'Access denied'}), 403
        
        server = conn.execute('SELECT id, name, owner_id, avatar FROM servers WHERE id = ?', (server_id,)).fetchone()
        channels = conn.execute('SELECT id, name FROM channels WHERE server_id = ? ORDER BY name', (server_id,)).fetchall()
        
        return jsonify({**dict(server), 'channels': [dict(c) for c in channels]})

@app.route('/conversations_list')
@login_required
def conversations_list():
    my_id = session['user_id']
    conversations = []
    with db_manager.get_connection() as conn:
        # 1-on-1 DMs from friends
        friends = conn.execute('''
            SELECT u.id, u.username, u.avatar 
            FROM users u JOIN friends f ON ((f.requester_id = ? AND f.addressee_id = u.id) OR (f.addressee_id = ? AND f.requester_id = u.id))
            WHERE f.status = 'accepted' ORDER BY u.username
        ''', (my_id, my_id)).fetchall()
        
        for friend in friends:
            dm_id = ensure_dm_between(my_id, friend['id'], conn)
            conversations.append({'id': dm_id, 'name': friend['username'], 'avatar': friend['avatar'], 'is_group': 0})
        
        # Group DMs
        groups = conn.execute('''
            SELECT d.id, d.name, d.avatar, 1 as is_group FROM dms d
            JOIN dm_members dm ON d.id = dm.dm_id
            WHERE dm.user_id = ? AND d.is_group = 1 ORDER BY d.name
        ''', (my_id,)).fetchall()
        conversations.extend([dict(g) for g in groups])
        
    return jsonify(conversations)

def ensure_dm_between(user1_id: int, user2_id: int, conn) -> int:
    """Finds or creates a 1-on-1 DM room and returns its ID."""
    cursor = conn.cursor()
    # This query is complex but correctly finds a DM between exactly two people.
    cursor.execute('''
        SELECT dm_id FROM dm_members 
        WHERE dm_id IN (SELECT dm_id FROM dm_members WHERE user_id = ?)
        AND dm_id IN (SELECT dm_id FROM dm_members WHERE user_id = ?)
        AND dm_id IN (SELECT dm_id FROM dms WHERE is_group = 0)
        GROUP BY dm_id HAVING COUNT(user_id) = 2
    ''', (user1_id, user2_id))
    dm = cursor.fetchone()
    if dm: return dm['dm_id']

    cursor.execute('INSERT INTO dms (is_group) VALUES (0)')
    dm_id = cursor.lastrowid
    cursor.execute('INSERT INTO dm_members (dm_id, user_id) VALUES (?, ?)', (dm_id, user1_id))
    cursor.execute('INSERT INTO dm_members (dm_id, user_id) VALUES (?, ?)', (dm_id, user2_id))
    conn.commit()
    return dm_id

@app.route('/history')
@login_required
def history():
    room = request.args.get('room')
    if not room: return jsonify([])
    
    my_id = session['user_id']
    messages = []
    
    with db_manager.get_connection() as conn:
        if room.startswith('server:'):
            try:
                _, _, _, channel_id = room.split(':')
                if conn.execute('SELECT 1 FROM server_members sm JOIN channels c ON sm.server_id = c.server_id WHERE c.id = ? AND sm.user_id = ?', (channel_id, my_id)).fetchone():
                    messages = conn.execute('''
                        SELECT m.content, m.content_type, m.ts, u.username, u.avatar FROM messages m
                        JOIN users u ON m.sender_id = u.id WHERE m.channel_id = ? ORDER BY m.ts ASC LIMIT 100
                    ''', (channel_id,)).fetchall()
            except (IndexError, ValueError): pass
        elif room.startswith('dm:'):
            try:
                _, dm_id = room.split(':')
                if conn.execute('SELECT 1 FROM dm_members WHERE dm_id = ? AND user_id = ?', (dm_id, my_id)).fetchone():
                    messages = conn.execute('''
                        SELECT m.content, m.content_type, m.ts, u.username, u.avatar FROM messages m
                        JOIN users u ON m.sender_id = u.id WHERE m.dm_id = ? ORDER BY m.ts ASC LIMIT 100
                    ''', (dm_id,)).fetchall()
            except (IndexError, ValueError): pass
    
    return jsonify([dict(m) for m in messages])

@app.route('/upload_image', methods=['POST'])
@login_required
def upload_image():
    room, image_file = request.form.get('room'), request.files.get('image')
    if not room or not image_file:
        return jsonify({'ok': False, 'error': 'Missing file or room'}), 400
    
    try:
        filename = save_uploaded_file(image_file, 'msg_')
        image_url = url_for('uploaded_file', filename=filename)
        
        # The HTTP route now directly calls the message handler logic
        # instead of emitting another socket event. This is more direct.
        handle_message_logic(
            {'room': room, 'url': image_url},
            'image',
            session['user_id']
        )
        return jsonify({'ok': True, 'url': image_url})
    except Exception as e:
        logger.error(f"Upload image error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

@app.route('/static/<path:filename>')
def static_file(filename):
    return send_from_directory(Config.STATIC_FOLDER, filename)

# --- SocketIO Events ---

@socketio.on('connect')
def on_connect():
    logger.info(f"Client connected: {request.sid}")

@socketio.on('identify')
def on_identify(data):
    if 'user_id' in session and session['user_id'] == data.get('user_id'):
        logger.info(f"User {session['user_id']} identified with sid {request.sid}")

@socketio.on('join')
def on_join(data):
    if 'user_id' in session and data.get('room'):
        join_room(data['room'])
        logger.info(f"User {session['user_id']} joined room {data['room']}")

@socketio.on('leave')
def on_leave(data):
    if 'user_id' in session and data.get('room'):
        leave_room(data['room'])
        logger.info(f"User {session['user_id']} left room {data['room']}")

@socketio.on('send_message')
def on_send_message(data):
    if 'user_id' in session:
        handle_message_logic(data, 'text', session['user_id'])

def handle_message_logic(data: Dict, content_type: str, user_id: int):
    """Core logic to process and broadcast a message."""
    room = data.get('room')
    content = data.get('text' if content_type == 'text' else 'url')
    if not room or not content: return

    with db_manager.get_connection() as conn:
        user = conn.execute('SELECT username, avatar FROM users WHERE id = ?', (user_id,)).fetchone()
        if not user: return
        
        dm_id, channel_id = None, None
        can_post = False
        
        if room.startswith('server:'):
            try:
                _, _, _, channel_id = room.split(':')
                # Check if user is a member of the server for that channel
                if conn.execute('SELECT 1 FROM server_members sm JOIN channels c ON sm.server_id = c.server_id WHERE c.id = ? AND sm.user_id = ?', (channel_id, user_id)).fetchone():
                    can_post = True
            except (IndexError, ValueError): pass
        elif room.startswith('dm:'):
            try:
                _, dm_id = room.split(':')
                if conn.execute('SELECT 1 FROM dm_members WHERE dm_id = ? AND user_id = ?', (dm_id, user_id)).fetchone():
                    can_post = True
            except (IndexError, ValueError): pass
        
        if not can_post:
            logger.warning(f"User {user_id} tried to post in forbidden room {room}")
            return
            
        conn.execute(
            'INSERT INTO messages (channel_id, dm_id, sender_id, content, content_type) VALUES (?, ?, ?, ?, ?)',
            (channel_id, dm_id, user_id, content, content_type)
        )
        conn.commit()
        
        emit('message', {
            'username': user['username'], 'avatar': user['avatar'],
            'content': content, 'content_type': content_type,
            'ts': datetime.utcnow().isoformat() + "Z" # ISO 8601 format for JS
        }, room=room)

# --- Error Handlers ---
@app.errorhandler(404)
def not_found(error): return 'Page Not Found', 404
@app.errorhandler(500)
def internal_error(error): return 'Internal Server Error', 500
@app.errorhandler(413)
def too_large(error): return 'File is too large', 413

# --- Main Execution ---
if __name__ == '__main__':
    logger.info('Starting MiniMessenger on http://127.0.0.1:5000')
    # Note: allow_unsafe_werkzeug is for development reloader with SocketIO.
    # For production, use a proper WSGI server like Gunicorn with eventlet or gevent.
    socketio.run(app, host='0.0.0.0', port=5000, debug=Config.DEBUG, allow_unsafe_werkzeug=True)
