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
    MAX_CONTENT_LENGTH = 25 * 1024 * 1024  # 25MB max file size
    ALLOWED_EXTENSIONS = {
        'png', 'jpg', 'jpeg', 'gif', 'webp',  # Images
        'mp4', 'webm', 'mov', 'avi',        # Videos
        'mp3', 'wav', 'ogg', 'm4a',         # Audios
        'pdf', 'doc', 'docx', 'txt', 'zip'  # Files
    }
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

# --- HTML Templates (Defined early to prevent NameError) ---
BASE_HTML = '''
<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8"> <meta name="viewport" content="width=device-width, initial-scale=1"> <title>MiniMessenger</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.socket.io/4.6.1/socket.io.min.js"></script>
    <style>
        :root { --background-primary: #313338; --background-secondary: #2b2d31; --background-tertiary: #1e1f22; --header-primary: #f2f3f5; --header-secondary: #b5bac1; --text-normal: #dcddde; --text-muted: #949ba4; --interactive-normal: #b5bac1; --interactive-hover: #dcddde; --interactive-active: #fff; --background-accent: #5865f2; --background-accent-hover: #4752c4; --button-danger: #da373c; --button-danger-hover: #a1282c; --background-modifier-hover: rgba(79, 84, 92, 0.16); --background-modifier-active: rgba(79, 84, 92, 0.24); --elevation-low: 0 1px 0 rgba(4, 4, 5, 0.2), 0 1.5px 0 rgba(6, 6, 7, 0.05), 0 2px 0 rgba(4, 4, 5, 0.05); --font-primary: 'Inter', sans-serif; }
        * { box-sizing: border-box; }
        body { font-family: var(--font-primary); margin: 0; height: 100vh; display: flex; background-color: var(--background-tertiary); color: var(--text-normal); overflow: hidden; font-size: 16px; }
        .app-container { display: flex; width: 100%; height: 100%; background-image: url('/static/background.jpg'); background-size: cover; background-position: center; }
        .app-container::before { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.5); backdrop-filter: blur(8px); z-index: 1; }
        .app-layout { position: relative; z-index: 2; display: flex; width: 100%; height: 100%; }
        .sidebar { width: 72px; background: var(--background-tertiary); display: flex; flex-direction: column; align-items: center; padding: 12px 0; gap: 8px; flex-shrink: 0; }
        .server-icon { width: 48px; height: 48px; border-radius: 50%; background: var(--background-primary); display: flex; align-items: center; justify-content: center; font-weight: 600; font-size: 18px; color: var(--header-primary); cursor: pointer; transition: all 0.2s ease; overflow: hidden; }
        .server-icon img { width: 100%; height: 100%; object-fit: cover; }
        .server-icon:hover, .server-icon.active { border-radius: 16px; background: var(--background-accent); }
        .channels-panel { width: 240px; background: var(--background-secondary); display: flex; flex-direction: column; flex-shrink: 0; }
        .panel-header { height: 48px; display: flex; align-items: center; justify-content: space-between; padding: 0 16px; font-weight: 600; color: var(--header-primary); box-shadow: var(--elevation-low); flex-shrink: 0; }
        .panel-header-actions a { color: var(--interactive-normal); text-decoration: none; font-size: 20px; }
        .panel-content { flex: 1; overflow-y: auto; padding: 8px; }
        .channel-item { display: flex; align-items: center; gap: 8px; padding: 6px 8px; border-radius: 4px; cursor: pointer; font-weight: 500; color: var(--interactive-normal); }
        .channel-item:hover { background: var(--background-modifier-hover); color: var(--interactive-hover); }
        .channel-item.active { background: var(--background-modifier-active); color: var(--interactive-active); }
        .main-content { flex: 1; display: flex; flex-direction: column; background: var(--background-primary); }
        .topbar { height: 48px; display: flex; align-items: center; padding: 0 16px; font-weight: 600; color: var(--header-primary); box-shadow: var(--elevation-low); flex-shrink: 0; }
        .messages { flex: 1; overflow-y: auto; padding: 16px; }
        .msg { display: flex; gap: 16px; padding: 8px 16px; margin-bottom: 4px; position: relative; }
        .msg:hover { background: var(--background-modifier-hover); border-radius: 4px; }
        .msg .avatar { width: 40px; height: 40px; border-radius: 50%; object-fit: cover; }
        .msg-header { display: flex; align-items: baseline; gap: 8px; margin-bottom: 4px; }
        .msg-author { font-weight: 500; color: var(--header-primary); }
        .msg-timestamp { font-size: 12px; color: var(--text-muted); }
        .msg-text { line-height: 1.4; word-wrap: break-word; }
        .msg-text.deleted { font-style: italic; color: var(--text-muted); }
        .msg-media { max-width: 400px; border-radius: 8px; margin-top: 8px; }
        .msg-file { display: block; background: var(--background-secondary); padding: 12px; border-radius: 4px; text-decoration: none; color: var(--interactive-hover); margin-top: 8px; max-width: 400px; }
        .delete-btn { position: absolute; top: 4px; right: 8px; background: var(--button-danger); color: white; border: none; border-radius: 4px; cursor: pointer; padding: 2px 6px; font-size: 12px; display: none; }
        .msg:hover .delete-btn.visible { display: block; }
        .composer { display: flex; padding: 0 16px 24px; gap: 12px; align-items: center; }
        .composer-input-wrapper { flex: 1; background: var(--background-secondary); border-radius: 8px; padding: 0; display: flex; align-items: center; }
        .composer-btn { padding: 10px; cursor: pointer; color: var(--interactive-normal); font-size: 20px; }
        .composer-btn:hover { color: var(--interactive-hover); }
        .composer input[type=text] { flex: 1; border: none; background: transparent; color: var(--text-normal); font-size: 16px; padding: 12px; }
        .composer input[type=text]:focus { outline: none; }
        .composer input[type=file] { display: none; }
        .user-panel { height: 52px; background: var(--background-tertiary); display: flex; align-items: center; padding: 0 8px; gap: 8px; }
        .user-panel .avatar { width: 32px; height: 32px; border-radius: 50%; object-fit: cover; }
        .user-panel .username { font-weight: 600; font-size: 14px; }
        .user-panel .actions { margin-left: auto; }
        .user-panel .actions a { color: var(--interactive-normal); text-decoration: none; font-size: 18px; margin-left: 8px; }
        .modal-backdrop { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.85); z-index: 1000; display: none; align-items: center; justify-content: center; }
        .modal-content { background: var(--background-secondary); padding: 24px; border-radius: 8px; width: 90%; max-width: 440px; }
        .modal-header { font-size: 20px; font-weight: 700; margin-bottom: 20px; }
        .form-group { margin-bottom: 16px; }
        .form-group label { display: block; font-size: 12px; font-weight: 600; color: var(--header-secondary); margin-bottom: 8px; }
        .form-group input { width: 100%; padding: 10px; border: 1px solid var(--background-tertiary); background: var(--background-tertiary); color: var(--text-normal); border-radius: 4px; }
        .modal-footer { display: flex; justify-content: flex-end; gap: 8px; margin-top: 24px; }
        .button { padding: 10px 16px; border-radius: 4px; border: none; cursor: pointer; font-weight: 500; }
        .button.primary { background: var(--background-accent); color: white; }
        .button.secondary { background: #6a7480; color: white; }
        #cameraModal video { width: 100%; border-radius: 8px; }
    </style>
</head>
<body>
    <div class="app-container">
        <div class="app-layout">
            {% if user %}
            <div class="sidebar" id="servers-list"></div>
            <div class="channels-panel">
                <div class="panel-header" id="panel-header">
                    <span id="panel-header-text">Select a Server</span>
                    <div class="panel-header-actions" id="panel-header-actions"></div>
                </div>
                <div class="panel-content" id="panel-content"></div>
                <div class="user-panel">
                    <img src="{{ '/uploads/' + user['avatar'] if user and user['avatar'] else '/static/default-avatar.png' }}" onerror="this.src='/static/default-avatar.png'" class="avatar">
                    <span class="username">{{ user['username'] }}</span>
                    <div class="actions"> <a href="/settings">⚙️</a> <a href="/logout">🚪</a> </div>
                </div>
            </div>
            <div class="main-content">
                <div class="topbar" id="current-room-name">Welcome!</div>
                <div class="messages" id="messages"></div>
                <div class="composer">
                    <div class="composer-input-wrapper">
                        <label for="file-input" class="composer-btn">➕</label>
                        <input type="file" id="file-input" onchange="uploadFile(this.files[0])">
                        <input id="msg-input" placeholder="Сообщение..." maxlength="2000">
                        <div class="composer-btn" onclick="toggleRecording()">🎤</div>
                        <div class="composer-btn" onclick="openCamera()">📷</div>
                    </div>
                </div>
            </div>
            {% else %}
            <div style="margin: auto; text-align: center; z-index: 2;">
                <h1 style="color: white;">Welcome to MiniMessenger</h1>
                <a href="/login" class="button primary">Войти</a> <a href="/register" class="button secondary">Регистрация</a>
            </div>
            {% endif %}
        </div>
    </div>

    <div id="createServerModal" class="modal-backdrop"><div class="modal-content"> <div class="modal-header">Создать сервер</div> <div class="form-group"> <label for="server-name">НАЗВАНИЕ</label> <input id="server-name" placeholder="Введите название" maxlength="50"> </div> <div class="modal-footer"> <button class="button secondary" onclick="closeModal('createServerModal')">Отмена</button> <button class="button primary" onclick="createServer()">Создать</button> </div> </div></div>
    <div id="createGroupModal" class="modal-backdrop"><div class="modal-content"> <div class="modal-header">Создать группу</div> <div class="form-group"> <label for="group-name">НАЗВАНИЕ</label> <input id="group-name" placeholder="Введите название" maxlength="50"> </div> <div class="form-group"> <label for="group-members">УЧАСТНИКИ (ники через запятую)</label> <input id="group-members" placeholder="user1, user2, ..."> </div> <div class="modal-footer"> <button class="button secondary" onclick="closeModal('createGroupModal')">Отмена</button> <button class="button primary" onclick="createGroup()">Создать</button> </div> </div></div>
    <div id="cameraModal" class="modal-backdrop"><div class="modal-content"> <div class="modal-header">Сделать фото</div> <video id="camera-feed" autoplay></video> <div class="modal-footer"> <button class="button secondary" onclick="closeCamera()">Отмена</button> <button class="button primary" onclick="capturePhoto()">Сделать снимок</button> </div> </div></div>

    <script>
    const socket = io();
    let current_room = null;
    const user = {{ user|tojson }};
    let mediaRecorder; let audioChunks = []; let isRecording = false;

    function openModal(id) { document.getElementById(id).style.display = 'flex'; }
    function closeModal(id) { document.getElementById(id).style.display = 'none'; }
    function escapeHtml(text) {
        if (typeof text !== 'string') return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function appendMessage(msg) {
        const messagesDiv = document.getElementById('messages');
        const messageEl = document.createElement('div');
        messageEl.className = 'msg';
        messageEl.id = `msg-${msg.id}`;
        const avatarPath = msg.avatar ? `/uploads/${msg.avatar}` : '/static/default-avatar.png';
        
        let contentHTML = '', deleteButtonHTML = '';
        if (msg.deleted) {
            contentHTML = `<div class="msg-text deleted">(сообщение удалено)</div>`;
        } else {
            switch(msg.content_type) {
                case 'text': contentHTML = `<div class="msg-text">${escapeHtml(msg.content)}</div>`; break;
                case 'image': contentHTML = `<div><img src="${escapeHtml(msg.content)}" class="msg-media" alt="Image"></div>`; break;
                case 'video': contentHTML = `<div><video src="${escapeHtml(msg.content)}" class="msg-media" controls></video></div>`; break;
                case 'audio': contentHTML = `<div><audio src="${escapeHtml(msg.content)}" controls></audio></div>`; break;
                case 'file':
                    const fileName = msg.content.split('/').pop();
                    contentHTML = `<a href="${escapeHtml(msg.content)}" class="msg-file" target="_blank" download>📄 ${escapeHtml(fileName)}</a>`;
                    break;
            }
            if (msg.sender_id === user.id) {
                deleteButtonHTML = `<button class="delete-btn" onclick="deleteMessage(${msg.id})">🗑️</button>`;
            }
        }

        messageEl.innerHTML = `
            <img class="avatar" src="${avatarPath}" onerror="this.src='/static/default-avatar.png'">
            <div class="msg-content">
                <div class="msg-header"> <span class="msg-author">${escapeHtml(msg.username)}</span> <span class="msg-timestamp">${new Date(msg.ts).toLocaleString()}</span> </div>
                ${contentHTML}
            </div>
            ${deleteButtonHTML}`;
        
        messagesDiv.appendChild(messageEl);
        if (msg.sender_id === user.id && !msg.deleted) {
            messageEl.addEventListener('mousemove', e => {
                if (e.shiftKey) messageEl.querySelector('.delete-btn')?.classList.add('visible');
                else messageEl.querySelector('.delete-btn')?.classList.remove('visible');
            });
            messageEl.addEventListener('mouseleave', () => messageEl.querySelector('.delete-btn')?.classList.remove('visible'));
        }
        messagesDiv.scrollTop = messagesDiv.scrollHeight;
    }

    function sendMessage() {
        const textInput = document.getElementById('msg-input');
        const text = textInput.value.trim();
        if (text && current_room) {
            socket.emit('send_message', { room: current_room, text: text });
            textInput.value = '';
        }
    }
    
    function uploadFile(file) {
        if (!file || !current_room) return;
        const formData = new FormData();
        formData.append('file', file);
        formData.append('room', current_room);
        fetch('/upload_file', { method: 'POST', body: formData })
            .then(r => r.json()).then(res => { if(!res.ok) alert(res.error); });
        document.getElementById('file-input').value = '';
    }

    function deleteMessage(messageId) { socket.emit('delete_message', { message_id: messageId }); }
    socket.on('message_deleted', (data) => {
        const msgEl = document.getElementById(`msg-${data.message_id}`);
        if (msgEl) {
            msgEl.querySelector('.msg-content').innerHTML = `<div class="msg-text deleted">(сообщение удалено)</div>`;
            msgEl.querySelector('.delete-btn')?.remove();
        }
    });

    function joinRoom(room, roomName) {
        if (current_room) socket.emit('leave', { room: current_room });
        current_room = room;
        document.getElementById('current-room-name').textContent = roomName;
        document.getElementById('messages').innerHTML = '';
        socket.emit('join', { room });
        fetch(`/history?room=${encodeURIComponent(room)}`).then(r => r.json()).then(messages => {
            if (Array.isArray(messages)) messages.forEach(appendMessage);
        });
    }

    function loadServers() {
        fetch('/my_servers').then(r => r.json()).then(servers => {
            const container = document.getElementById('servers-list');
            container.innerHTML = `<div class="server-icon" onclick="loadConversations()">DM</div> <hr style="width: 50%; border-color: var(--background-primary);">`;
            servers.forEach(server => {
                const el = document.createElement('div');
                el.className = 'server-icon';
                el.innerHTML = server.avatar ? `<img src="/uploads/${server.avatar}" onerror="this.src='/static/default-server.png'">` : `<span>${escapeHtml(server.name.charAt(0))}</span>`;
                el.onclick = () => loadChannels(server.id, server.name);
                container.appendChild(el);
            });
            container.innerHTML += `<div class="server-icon" onclick="openModal('createServerModal')">+</div>`;
        });
    }

    function loadChannels(serverId, serverName) {
        document.getElementById('panel-header-text').textContent = serverName;
        fetch(`/server_info?server_id=${serverId}`).then(r => r.json()).then(info => {
            if (info.error) return alert(info.error);
            const actions = document.getElementById('panel-header-actions');
            actions.innerHTML = info.is_owner ? `<a href="/server_settings/${serverId}">⚙️</a>` : '';
            const container = document.getElementById('panel-content');
            container.innerHTML = '<h4>Каналы</h4>';
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
        document.getElementById('panel-header-text').textContent = "Личные сообщения";
        document.getElementById('panel-header-actions').innerHTML = `<a href="#" onclick="openModal('createGroupModal')" title="Создать группу">+</a>`;
        fetch('/conversations_list').then(r => r.json()).then(convs => {
            const container = document.getElementById('panel-content');
            container.innerHTML = '<h4>Друзья и Группы</h4>';
            convs.forEach(conv => {
                const el = document.createElement('div');
                el.className = 'channel-item';
                const settingsIcon = (conv.is_group && conv.is_owner) ? `<a href="/group_settings/${conv.id}" style="margin-left:auto; text-decoration:none; color: var(--interactive-normal)">⚙️</a>` : '';
                el.innerHTML = `<span>${escapeHtml(conv.name)}</span>${settingsIcon}`;
                el.onclick = (e) => { if (e.target.tagName !== 'A') joinRoom(`dm:${conv.id}`, escapeHtml(conv.name)); };
                container.appendChild(el);
            });
        });
    }

    function createServer() {
        const name = document.getElementById('server-name').value.trim();
        if (!name) return;
        fetch('/create_server', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name }) })
        .then(r => r.json()).then(result => {
            if (result.ok) { closeModal('createServerModal'); document.getElementById('server-name').value = ''; loadServers(); } 
            else { alert('Ошибка: ' + (result.error || '')); }
        });
    }
    
    function createGroup() {
        const name = document.getElementById('group-name').value.trim();
        const membersRaw = document.getElementById('group-members').value.trim();
        if (!name || !membersRaw) return;
        const members = membersRaw.split(',').map(m => m.trim()).filter(Boolean);
        fetch('/create_group', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, members }) })
        .then(r => r.json()).then(result => {
            if (result.ok) { closeModal('createGroupModal'); document.getElementById('group-name').value = ''; document.getElementById('group-members').value = ''; loadConversations(); }
            else { alert('Ошибка: ' + (result.error || '')); }
        });
    }

    async function toggleRecording() {
        const micButton = document.querySelector('.composer-btn:nth-child(2)');
        if (isRecording) {
            mediaRecorder.stop();
            isRecording = false;
            micButton.style.color = 'var(--interactive-normal)';
        } else {
            try {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                mediaRecorder = new MediaRecorder(stream);
                mediaRecorder.ondataavailable = e => audioChunks.push(e.data);
                mediaRecorder.onstop = () => {
                    const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
                    uploadFile(new File([audioBlob], "voice-message.webm"));
                    audioChunks = [];
                    stream.getTracks().forEach(track => track.stop());
                };
                mediaRecorder.start();
                isRecording = true;
                micButton.style.color = 'var(--button-danger)';
            } catch (err) { console.error("Ошибка доступа к микрофону:", err); alert("Не удалось получить доступ к микрофону."); }
        }
    }

    async function openCamera() {
        const video = document.getElementById('camera-feed');
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ video: true });
            video.srcObject = stream;
            openModal('cameraModal');
        } catch (err) { console.error("Ошибка доступа к камере:", err); alert("Не удалось получить доступ к камере."); }
    }

    function closeCamera() {
        const video = document.getElementById('camera-feed');
        if (video.srcObject) {
            video.srcObject.getTracks().forEach(track => track.stop());
            video.srcObject = null;
        }
        closeModal('cameraModal');
    }

    function capturePhoto() {
        const video = document.getElementById('camera-feed');
        const canvas = document.createElement('canvas');
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        canvas.getContext('2d').drawImage(video, 0, 0);
        canvas.toBlob(blob => {
            uploadFile(new File([blob], "camera-shot.png", { type: "image/png" }));
        }, 'image/png');
        closeCamera();
    }

    window.addEventListener('load', () => { if (user) { loadServers(); loadConversations(); } });
    socket.on('connect', () => { if (user) socket.emit('identify', { user_id: user.id }); });
    socket.on('message', appendMessage);
    document.getElementById('msg-input')?.addEventListener('keypress', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); } });
    </script>
</body>
</html>
'''
LOGIN_REGISTER_HTML = '''
<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8"> <meta name="viewport" content="width=device-width, initial-scale=1"> <title>{{ title }} - MiniMessenger</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root { --background-primary: #313338; --background-secondary: #2b2d31; --background-tertiary: #1e1f22; --header-primary: #f2f3f5; --text-normal: #dcddde; --background-accent: #5865f2; }
        body { font-family: 'Inter', sans-serif; margin: 0; height: 100vh; display: flex; align-items: center; justify-content: center; background-color: var(--background-tertiary); color: var(--text-normal); background-image: url('/static/background.jpg'); background-size: cover; background-position: center; }
        .auth-box { background: var(--background-secondary); padding: 32px; border-radius: 8px; width: 90%; max-width: 400px; z-index: 2; }
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
            <div class="form-group"> <label for="username">Никнейм</label> <input type="text" id="username" name="username" required maxlength="50"> </div>
            <div class="form-group"> <label for="password">Пароль</label> <input type="password" id="password" name="password" required> </div>
            {% if is_register %}
            <div class="form-group"> <label for="avatar">Аватар (необязательно)</label> <input type="file" id="avatar" name="avatar" accept="image/*"> </div>
            {% endif %}
            <button type="submit">{{ title }}</button>
        </form>
        <div class="link">
            {% if is_register %} <a href="/login">Уже есть аккаунт?</a> {% else %} <a href="/register">Нужен аккаунт?</a> {% endif %}
        </div>
    </div>
</body>
</html>
'''
SETTINGS_HTML = '''
<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8"> <meta name="viewport" content="width=device-width, initial-scale=1"> <title>Настройки - MiniMessenger</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root { --background-primary: #313338; --background-secondary: #2b2d31; --background-tertiary: #1e1f22; --header-primary: #f2f3f5; --text-normal: #dcddde; --background-accent: #5865f2; --button-danger: #da373c; --button-danger-hover: #a1282c;}
        body { font-family: 'Inter', sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; background-color: var(--background-tertiary); color: var(--text-normal); }
        .container { width: 100%; max-width: 600px; background-color: var(--background-secondary); padding: 2rem; border-radius: 8px; }
        h2 { color: var(--header-primary); margin-top: 0; }
        .form-group { margin-bottom: 1.5rem; }
        label { display: block; font-size: 12px; font-weight: 600; color: var(--header-secondary); margin-bottom: 8px; text-transform: uppercase; }
        input { width: 100%; padding: 10px; border: 1px solid var(--background-tertiary); background: var(--background-tertiary); color: var(--text-normal); border-radius: 4px; box-sizing: border-box; }
        .button { padding: 10px 16px; border-radius: 4px; border: none; cursor: pointer; font-weight: 500; }
        .button.primary { background: var(--background-accent); color: white; }
        .button.danger { background: var(--button-danger); color: white; }
        .button.danger:hover { background: var(--button-danger-hover); }
        .alert { padding: 1rem; border-radius: 4px; margin-bottom: 1rem; }
        .alert.success { background-color: #2f4c3a; color: #a3e4b7; }
        .alert.error { background-color: #4c2f2f; color: #e4a3a3; }
        .danger-zone { margin-top: 2rem; padding-top: 1.5rem; border-top: 1px solid var(--background-tertiary); }
    </style>
</head>
<body>
    <div class="container">
        <h2>Мой аккаунт</h2>
        {% if message %} <div class="alert {{ 'success' if success else 'error' }}">{{ message }}</div> {% endif %}
        <form method="post" enctype="multipart/form-data">
            <div class="form-group"> <label for="username">Никнейм</label> <input type="text" id="username" name="username" value="{{ user.username }}" required maxlength="50"> </div>
            <div class="form-group"> <label for="avatar">Аватар</label> <input type="file" id="avatar" name="avatar" accept="image/*"> </div>
            <div class="form-group"> <label for="banner">Баннер профиля</label> <input type="file" id="banner" name="banner" accept="image/*"> </div>
            <button type="submit" class="button primary">Сохранить</button>
        </form>
        <div class="danger-zone">
            <h3>Удалить аккаунт</h3>
            <p>Это действие необратимо. Пожалуйста, будьте уверены.</p>
            <form method="post" action="{{ url_for('delete_account') }}" onsubmit="return confirm('Вы абсолютно уверены, что хотите удалить свой аккаунт?');">
                <button type="submit" class="button danger">Удалить аккаунт</button>
            </form>
        </div>
        <div style="text-align: center; margin-top: 2rem;"><a href="/" style="color: var(--text-muted);">Вернуться в чат</a></div>
    </div>
</body>
</html>
'''
GROUP_SETTINGS_HTML = '''
<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8"> <meta name="viewport" content="width=device-width, initial-scale=1"> <title>Настройки группы - MiniMessenger</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root { --background-primary: #313338; --background-secondary: #2b2d31; --background-tertiary: #1e1f22; --header-primary: #f2f3f5; --text-normal: #dcddde; --background-accent: #5865f2; --button-danger: #da373c; --button-danger-hover: #a1282c;}
        body { font-family: 'Inter', sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; background-color: var(--background-tertiary); color: var(--text-normal); }
        .container { width: 100%; max-width: 600px; background-color: var(--background-secondary); padding: 2rem; border-radius: 8px; }
        h2 { color: var(--header-primary); margin-top: 0; }
        .form-group { margin-bottom: 1.5rem; }
        label { display: block; font-size: 12px; font-weight: 600; color: var(--header-secondary); margin-bottom: 8px; text-transform: uppercase; }
        input { width: 100%; padding: 10px; border: 1px solid var(--background-tertiary); background: var(--background-tertiary); color: var(--text-normal); border-radius: 4px; box-sizing: border-box; }
        .button { padding: 10px 16px; border-radius: 4px; border: none; cursor: pointer; font-weight: 500; }
        .button.primary { background: var(--background-accent); color: white; }
        .button.danger { background: var(--button-danger); color: white; }
        .alert { padding: 1rem; border-radius: 4px; margin-bottom: 1rem; }
        .alert.success { background-color: #2f4c3a; color: #a3e4b7; }
        .alert.error { background-color: #4c2f2f; color: #e4a3a3; }
        .members-list { list-style: none; padding: 0; }
        .member-item { display: flex; justify-content: space-between; align-items: center; padding: 8px; background: var(--background-primary); border-radius: 4px; margin-bottom: 8px; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Настройки группы: {{ group.name }}</h2>
        {% if message %} <div class="alert {{ 'success' if success else 'error' }}">{{ message }}</div> {% endif %}
        <form method="post" enctype="multipart/form-data">
            <div class="form-group"> <label for="name">Название группы</label> <input type="text" id="name" name="name" value="{{ group.name }}" required maxlength="50"> </div>
            <div class="form-group"> <label for="avatar">Аватар группы</label> <input type="file" id="avatar" name="avatar" accept="image/*"> </div>
            <button type="submit" class="button primary">Сохранить</button>
        </form>
        <div style="margin-top: 2rem;">
            <h3>Участники</h3>
            <ul class="members-list">
                {% for member in members %}
                <li class="member-item">
                    <span>{{ member.username }} {% if member.id == group.owner_id %}(👑 Владелец){% endif %}</span>
                    {% if member.id != group.owner_id %}
                    <form method="post" action="{{ url_for('remove_group_member', group_id=group.id) }}" style="display: inline;">
                        <input type="hidden" name="user_id" value="{{ member.id }}">
                        <button type="submit" class="button danger" style="padding: 4px 8px;">Удалить</button>
                    </form>
                    {% endif %}
                </li>
                {% endfor %}
            </ul>
        </div>
        <div style="text-align: center; margin-top: 2rem;"><a href="/" style="color: var(--text-muted);">Вернуться в чат</a></div>
    </div>
</body>
</html>
'''

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
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL,
                    avatar TEXT, banner TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS friends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, requester_id INTEGER NOT NULL, addressee_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','accepted','rejected')), created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(requester_id, addressee_id), FOREIGN KEY (requester_id) REFERENCES users (id), FOREIGN KEY (addressee_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS servers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, owner_id INTEGER NOT NULL, avatar TEXT, description TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (owner_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS server_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, server_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                    joined_at DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(server_id, user_id),
                    FOREIGN KEY (server_id) REFERENCES servers (id), FOREIGN KEY (user_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, server_id INTEGER NOT NULL, name TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (server_id) REFERENCES servers (id)
                );
                CREATE TABLE IF NOT EXISTS dms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, is_group INTEGER NOT NULL DEFAULT 0, owner_id INTEGER, avatar TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (owner_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS dm_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, dm_id INTEGER NOT NULL, user_id INTEGER NOT NULL, UNIQUE(dm_id, user_id),
                    FOREIGN KEY (dm_id) REFERENCES dms (id), FOREIGN KEY (user_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, channel_id INTEGER, dm_id INTEGER, sender_id INTEGER NOT NULL,
                    content TEXT, content_type TEXT NOT NULL CHECK(content_type IN ('text','image','video','audio','file')),
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP, deleted INTEGER DEFAULT 0,
                    FOREIGN KEY (channel_id) REFERENCES channels (id), FOREIGN KEY (dm_id) REFERENCES dms (id), FOREIGN KEY (sender_id) REFERENCES users (id)
                );
                CREATE TABLE IF NOT EXISTS invites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, server_id INTEGER NOT NULL, token TEXT UNIQUE NOT NULL, creator_id INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, expires_at DATETIME,
                    FOREIGN KEY (server_id) REFERENCES servers (id), FOREIGN KEY (creator_id) REFERENCES users (id)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id);
                CREATE INDEX IF NOT EXISTS idx_messages_dm ON messages(dm_id);
            ''')
            self._run_migration(conn)
            conn.commit()
            logger.info("Database initialized and migrated successfully")

    def _run_migration(self, conn):
        cursor = conn.cursor()
        def add_column_if_not_exists(table, column, definition):
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row['name'] for row in cursor.fetchall()]
            if column not in columns:
                logger.info(f"Migrating {table} table: adding '{column}' column.")
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        add_column_if_not_exists('dms', 'is_group', 'INTEGER NOT NULL DEFAULT 0')
        add_column_if_not_exists('dms', 'owner_id', 'INTEGER')
        add_column_if_not_exists('dms', 'avatar', 'TEXT')
        add_column_if_not_exists('servers', 'description', 'TEXT')
        add_column_if_not_exists('users', 'banner', 'TEXT')
        add_column_if_not_exists('messages', 'deleted', 'INTEGER DEFAULT 0')

db_manager = DatabaseManager(Config.DB_PATH)

# --- Utility Functions & Decorators ---
def get_file_type(filename):
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    if ext in {'png', 'jpg', 'jpeg', 'gif', 'webp'}: return 'image'
    if ext in {'mp4', 'webm', 'mov', 'avi'}: return 'video'
    if ext in {'mp3', 'wav', 'ogg', 'm4a'}: return 'audio'
    return 'file'

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

def validate_input(data: str, max_length: int = 255, min_length: int = 1) -> bool:
    if not data or not isinstance(data, str): return False
    return min_length <= len(data.strip()) <= max_length

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            if request.is_json: return jsonify({'ok': False, 'error': 'Authentication required'}), 401
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

def get_current_user(full=False) -> Optional[Dict]:
    if 'user_id' not in session: return None
    with db_manager.get_connection() as conn:
        columns = '*' if full else 'id, username, avatar'
        user = conn.execute(f'SELECT {columns} FROM users WHERE id = ?', (session['user_id'],)).fetchone()
        return dict(user) if user else None

def save_uploaded_file(file, prefix: str = '') -> Optional[str]:
    if not file or not file.filename: return None
    if not allowed_file(file.filename): raise ValueError("File type not allowed")
    original_filename = secure_filename(file.filename)
    ext = os.path.splitext(original_filename)[1]
    filename = f"{prefix}{uuid.uuid4().hex}{ext}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    logger.info(f"File saved: {filename}")
    return filename

# --- Flask Routes ---

@app.route('/')
def index():
    user = get_current_user()
    return render_template_string(BASE_HTML, user=user)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET': return render_template_string(LOGIN_REGISTER_HTML, title="Вход", is_register=False)
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    if not validate_input(username, max_length=50) or not password:
        return render_template_string(LOGIN_REGISTER_HTML, title="Вход", is_register=False, error="Неверные данные")
    with db_manager.get_connection() as conn:
        user = conn.execute('SELECT id, password_hash FROM users WHERE username = ? COLLATE NOCASE', (username,)).fetchone()
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            return redirect(url_for('index'))
        else:
            return render_template_string(LOGIN_REGISTER_HTML, title="Вход", is_register=False, error="Неверный никнейм или пароль")

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET': return render_template_string(LOGIN_REGISTER_HTML, title="Регистрация", is_register=True)
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    if not validate_input(username, max_length=50, min_length=3):
        return render_template_string(LOGIN_REGISTER_HTML, title="Регистрация", is_register=True, error="Никнейм должен быть 3-50 символов")
    if len(password) < 6:
        return render_template_string(LOGIN_REGISTER_HTML, title="Регистрация", is_register=True, error="Пароль должен быть не менее 6 символов")
    try:
        avatar_filename = save_uploaded_file(request.files.get('avatar'), 'avatar_')
        with db_manager.get_connection() as conn:
            conn.execute('INSERT INTO users (username, password_hash, avatar) VALUES (?, ?, ?)', (username, generate_password_hash(password), avatar_filename))
            conn.commit()
        return redirect(url_for('login'))
    except sqlite3.IntegrityError:
        return render_template_string(LOGIN_REGISTER_HTML, title="Регистрация", is_register=True, error="Такой никнейм уже занят")
    except Exception as e:
        logger.error(f"Registration error: {e}")
        return render_template_string(LOGIN_REGISTER_HTML, title="Регистрация", is_register=True, error="Произошла ошибка")

@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    user = get_current_user(full=True)
    message, success = None, False
    if request.method == 'POST':
        new_username = request.form.get('username', '').strip()
        if not validate_input(new_username, min_length=3):
            message = "Никнейм должен быть 3-50 символов"
        else:
            try:
                with db_manager.get_connection() as conn:
                    if new_username.lower() != user['username'].lower():
                         conn.execute('UPDATE users SET username = ? WHERE id = ?', (new_username, user['id']))
                    if 'avatar' in request.files and request.files['avatar'].filename:
                        conn.execute('UPDATE users SET avatar = ? WHERE id = ?', (save_uploaded_file(request.files['avatar'], 'avatar_'), user['id']))
                    if 'banner' in request.files and request.files['banner'].filename:
                        conn.execute('UPDATE users SET banner = ? WHERE id = ?', (save_uploaded_file(request.files['banner'], 'banner_'), user['id']))
                    conn.commit()
                    message, success = "Профиль обновлен!", True
                    user = get_current_user(full=True)
            except sqlite3.IntegrityError: message = "Этот никнейм уже занят."
            except Exception as e: logger.error(f"Settings update error: {e}"); message = "Ошибка обновления."
    return render_template_string(SETTINGS_HTML, user=user, message=message, success=success)

@app.route('/delete_account', methods=['POST'])
@login_required
def delete_account():
    user_id = session['user_id']
    with db_manager.get_connection() as conn:
        conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
        conn.commit()
    session.clear()
    return redirect(url_for('login'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/group_settings/<int:group_id>', methods=['GET', 'POST'])
@login_required
def group_settings(group_id):
    with db_manager.get_connection() as conn:
        group = conn.execute('SELECT * FROM dms WHERE id = ? AND is_group = 1', (group_id,)).fetchone()
        if not group or group['owner_id'] != session['user_id']:
            return "Группа не найдена или у вас нет прав", 404

        message, success = None, False
        if request.method == 'POST':
            new_name = request.form.get('name', '').strip()
            if not validate_input(new_name):
                message = "Неверное название"
            else:
                try:
                    conn.execute('UPDATE dms SET name = ? WHERE id = ?', (new_name, group_id))
                    if 'avatar' in request.files and request.files['avatar'].filename:
                        avatar = save_uploaded_file(request.files['avatar'], 'group_')
                        conn.execute('UPDATE dms SET avatar = ? WHERE id = ?', (avatar, group_id))
                    conn.commit()
                    message, success = "Настройки сохранены!", True
                    group = conn.execute('SELECT * FROM dms WHERE id = ?', (group_id,)).fetchone()
                except Exception as e:
                    logger.error(f"Group settings error: {e}")
                    message = "Ошибка сохранения"
        
        members = conn.execute('SELECT u.id, u.username FROM users u JOIN dm_members dm ON u.id = dm.user_id WHERE dm.dm_id = ?', (group_id,)).fetchall()
    return render_template_string(GROUP_SETTINGS_HTML, group=group, members=members, message=message, success=success)

@app.route('/remove_group_member/<int:group_id>', methods=['POST'])
@login_required
def remove_group_member(group_id):
    with db_manager.get_connection() as conn:
        group = conn.execute('SELECT owner_id FROM dms WHERE id = ? AND is_group = 1', (group_id,)).fetchone()
        if not group or group['owner_id'] != session['user_id']:
            return redirect(url_for('index'))
        
        user_to_remove_id = request.form.get('user_id')
        if user_to_remove_id and int(user_to_remove_id) != session['user_id']:
            conn.execute('DELETE FROM dm_members WHERE dm_id = ? AND user_id = ?', (group_id, user_to_remove_id))
            conn.commit()
    return redirect(url_for('group_settings', group_id=group_id))

# --- API Routes for Frontend ---

@app.route('/create_server', methods=['POST'])
@login_required
def create_server():
    name = request.json.get('name', '').strip()
    if not validate_input(name, max_length=50): return jsonify({'ok': False, 'error': 'Неверное название'})
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO servers (name, owner_id) VALUES (?, ?)', (name, session['user_id']))
            server_id = cursor.lastrowid
            cursor.execute('INSERT INTO server_members (server_id, user_id) VALUES (?, ?)', (server_id, session['user_id']))
            cursor.execute('INSERT INTO channels (server_id, name) VALUES (?, ?)', (server_id, 'general'))
            conn.commit()
        return jsonify({'ok': True, 'id': server_id})
    except Exception as e:
        logger.error(f"Error creating server: {e}")
        return jsonify({'ok': False, 'error': 'Ошибка создания сервера'})

@app.route('/create_group', methods=['POST'])
@login_required
def create_group():
    data = request.json
    name, members_usernames = data.get('name', '').strip(), data.get('members', [])
    if not validate_input(name, max_length=50) or not members_usernames:
        return jsonify({'ok': False, 'error': 'Неверные данные'})
    with db_manager.get_connection() as conn:
        placeholders = ','.join('?' for _ in members_usernames)
        members = conn.execute(f'SELECT id FROM users WHERE username IN ({placeholders}) COLLATE NOCASE', members_usernames).fetchall()
        member_ids = {m['id'] for m in members}
        member_ids.add(session['user_id'])
        if len(member_ids) < 2: return jsonify({'ok': False, 'error': 'Не удалось найти участников'})
        cursor = conn.cursor()
        cursor.execute('INSERT INTO dms (name, is_group, owner_id) VALUES (?, 1, ?)', (name, session['user_id']))
        dm_id = cursor.lastrowid
        for user_id in member_ids:
            cursor.execute('INSERT OR IGNORE INTO dm_members (dm_id, user_id) VALUES (?, ?)', (dm_id, user_id))
        conn.commit()
    return jsonify({'ok': True, 'id': dm_id})

@app.route('/my_servers')
@login_required
def my_servers():
    with db_manager.get_connection() as conn:
        servers = conn.execute('SELECT s.id, s.name, s.avatar FROM servers s JOIN server_members m ON s.id = m.server_id WHERE m.user_id = ? ORDER BY s.name', (session['user_id'],)).fetchall()
    return jsonify([dict(s) for s in servers])

@app.route('/server_info')
@login_required
def server_info():
    server_id = request.args.get('server_id')
    with db_manager.get_connection() as conn:
        if not conn.execute('SELECT 1 FROM server_members WHERE server_id = ? AND user_id = ?', (server_id, session['user_id'])).fetchone():
            return jsonify({'error': 'Доступ запрещен'}), 403
        server = conn.execute('SELECT id, name, owner_id, avatar FROM servers WHERE id = ?', (server_id,)).fetchone()
        channels = conn.execute('SELECT id, name FROM channels WHERE server_id = ? ORDER BY name', (server_id,)).fetchall()
        response = {**dict(server), 'channels': [dict(c) for c in channels], 'is_owner': server['owner_id'] == session['user_id']}
        return jsonify(response)

@app.route('/conversations_list')
@login_required
def conversations_list():
    my_id = session['user_id']
    conversations = []
    with db_manager.get_connection() as conn:
        friends = conn.execute("SELECT u.id, u.username FROM users u JOIN friends f ON ((f.requester_id = ? AND f.addressee_id = u.id) OR (f.addressee_id = ? AND f.requester_id = u.id)) WHERE f.status = 'accepted' ORDER BY u.username", (my_id, my_id)).fetchall()
        for friend in friends:
            dm_id = ensure_dm_between(my_id, friend['id'], conn)
            conversations.append({'id': dm_id, 'name': friend['username'], 'is_group': 0})
        groups = conn.execute("SELECT d.id, d.name, d.owner_id FROM dms d JOIN dm_members dm ON d.id = dm.dm_id WHERE dm.user_id = ? AND d.is_group = 1 GROUP BY d.id ORDER BY d.name", (my_id,)).fetchall()
        for g in groups:
            conversations.append({'id': g['id'], 'name': g['name'], 'is_group': 1, 'is_owner': g['owner_id'] == my_id})
    return jsonify(conversations)

def ensure_dm_between(user1_id: int, user2_id: int, conn) -> int:
    cursor = conn.cursor()
    cursor.execute("SELECT dm_id FROM dm_members WHERE dm_id IN (SELECT dm_id FROM dm_members WHERE user_id = ?) AND dm_id IN (SELECT dm_id FROM dm_members WHERE user_id = ?) AND dm_id IN (SELECT id FROM dms WHERE is_group = 0) GROUP BY dm_id HAVING COUNT(user_id) = 2", (user1_id, user2_id))
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
        query = "SELECT m.id, m.content, m.content_type, m.ts, m.deleted, m.sender_id, u.username, u.avatar FROM messages m JOIN users u ON m.sender_id = u.id WHERE {condition} ORDER BY m.ts ASC LIMIT 100"
        if room.startswith('server:'):
            try:
                _, _, _, channel_id = room.split(':')
                if conn.execute('SELECT 1 FROM server_members sm JOIN channels c ON sm.server_id = c.server_id WHERE c.id = ? AND sm.user_id = ?', (channel_id, my_id)).fetchone():
                    messages = conn.execute(query.format(condition='m.channel_id = ?'), (channel_id,)).fetchall()
            except (IndexError, ValueError): pass
        elif room.startswith('dm:'):
            try:
                _, dm_id = room.split(':')
                if conn.execute('SELECT 1 FROM dm_members WHERE dm_id = ? AND user_id = ?', (dm_id, my_id)).fetchone():
                    messages = conn.execute(query.format(condition='m.dm_id = ?'), (dm_id,)).fetchall()
            except (IndexError, ValueError): pass
    return jsonify([dict(m) for m in messages])

@app.route('/upload_file', methods=['POST'])
@login_required
def upload_file():
    room, file = request.form.get('room'), request.files.get('file')
    if not room or not file: return jsonify({'ok': False, 'error': 'Файл или комната отсутствуют'}), 400
    try:
        filename = save_uploaded_file(file, 'file_')
        file_url = url_for('uploaded_file', filename=filename)
        content_type = get_file_type(filename)
        
        # This is the fix: Instead of calling a complex handler,
        # we emit a simple event that the server-side socket handler will process.
        # This ensures we are in the correct Socket.IO context.
        socketio.emit('save_file_message', {
            'room': room,
            'url': file_url,
            'content_type': content_type,
            'user_id': session['user_id']
        })
        return jsonify({'ok': True})
    except ValueError as e:
        return jsonify({'ok': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Upload file error: {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

@app.route('/static/<path:filename>')
def static_file(filename):
    return send_from_directory(Config.STATIC_FOLDER, filename)

# --- SocketIO Events ---

def create_and_broadcast_message(user_id: int, room: str, content: str, content_type: str):
    """
    A central function to create a message in the DB and broadcast it via Socket.IO.
    This can be called from any context (HTTP or Socket.IO).
    """
    if not room or not content: return
    with db_manager.get_connection() as conn:
        user = conn.execute('SELECT username, avatar FROM users WHERE id = ?', (user_id,)).fetchone()
        if not user: return

        dm_id, channel_id, can_post = None, None, False
        if room.startswith('server:'):
            try:
                _, _, _, channel_id = room.split(':')
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

        cursor = conn.cursor()
        cursor.execute('INSERT INTO messages (channel_id, dm_id, sender_id, content, content_type) VALUES (?, ?, ?, ?, ?)',
                       (channel_id, dm_id, user_id, content, content_type))
        message_id = cursor.lastrowid
        conn.commit()
        
        full_message_data = {
            'id': message_id, 'sender_id': user_id, 'username': user['username'],
            'avatar': user['avatar'], 'content': content, 'content_type': content_type,
            'ts': datetime.utcnow().isoformat() + "Z", 'deleted': 0
        }
        socketio.emit('message', full_message_data, room=room)


@socketio.on('connect')
def on_connect(): logger.info(f"Client connected: {request.sid}")

@socketio.on('identify')
def on_identify(data):
    if 'user_id' in session and session['user_id'] == data.get('user_id'):
        logger.info(f"User {session['user_id']} identified with sid {request.sid}")

@socketio.on('join')
def on_join(data):
    if 'user_id' in session and data.get('room'):
        join_room(data['room']); logger.info(f"User {session['user_id']} joined room {data['room']}")

@socketio.on('leave')
def on_leave(data):
    if 'user_id' in session and data.get('room'):
        leave_room(data['room']); logger.info(f"User {session['user_id']} left room {data['room']}")

@socketio.on('send_message')
def on_send_message(data):
    if 'user_id' in session:
        create_and_broadcast_message(session['user_id'], data.get('room'), data.get('text'), 'text')

@socketio.on('save_file_message')
def on_save_file_message(data):
    # This event is emitted by the server itself after a successful file upload
    user_id = data.get('user_id')
    if 'user_id' in session and session['user_id'] == user_id: # Security check
        create_and_broadcast_message(
            user_id,
            data.get('room'),
            data.get('url'),
            data.get('content_type')
        )

@socketio.on('delete_message')
def on_delete_message(data):
    user_id, message_id = session.get('user_id'), data.get('message_id')
    if not user_id or not message_id: return
    with db_manager.get_connection() as conn:
        msg = conn.execute('SELECT sender_id, channel_id, dm_id FROM messages WHERE id = ?', (message_id,)).fetchone()
        if msg and msg['sender_id'] == user_id:
            conn.execute('UPDATE messages SET deleted = 1, content = NULL, content_type = "text" WHERE id = ?', (message_id,))
            conn.commit()
            room = None
            if msg['channel_id']:
                server_id = conn.execute('SELECT server_id FROM channels WHERE id = ?', (msg['channel_id'],)).fetchone()['server_id']
                room = f"server:{server_id}:channel:{msg['channel_id']}"
            elif msg['dm_id']: room = f"dm:{msg['dm_id']}"
            if room: emit('message_deleted', {'message_id': message_id}, room=room)

# --- Error Handlers ---
@app.errorhandler(404)
def not_found(error): return 'Страница не найдена', 404
@app.errorhandler(500)
def internal_error(error): return 'Внутренняя ошибка сервера', 500
@app.errorhandler(413)
def too_large(error): return 'Файл слишком большой', 413

# --- Main Execution ---
if __name__ == '__main__':
    logger.info('Starting MiniMessenger on http://127.0.0.1:5000')
    socketio.run(app, host='0.0.0.0', port=5000, debug=Config.DEBUG, allow_unsafe_werkzeug=True)
