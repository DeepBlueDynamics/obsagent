// OBS Agentic Interface Client App
let ws = null;
let reconnectInterval = 5000;
let currentAgentMessageId = null;
let currentTraceBody = null;
let isObsRecording = false;
let hasAutoSetDimensions = false;
let hasPlayedErrorSoundThisTurn = false;

// DOM Elements
const statusIndicator = document.querySelector('.status-indicator');
const statusText = document.querySelector('.status-text');
const streamVal = document.getElementById('streaming-value');
const recordVal = document.getElementById('recording-value');
const virtualcamVal = document.getElementById('virtualcam-value');
const resolutionVal = document.getElementById('resolution-value');
const currentSceneName = document.getElementById('current-scene-name');
const sceneGrid = document.getElementById('scene-grid');
const audioMixer = document.getElementById('audio-mixer');
const chatMessages = document.getElementById('chat-messages');
const chatForm = document.getElementById('chat-form');
const chatInput = document.getElementById('chat-input');
const btnClearInput = document.getElementById('btn-clear-input');
const btnReconnectObs = document.getElementById('btn-reconnect-obs');
const btnClearHistory = document.getElementById('btn-clear-history');
const suggestionChips = document.querySelectorAll('.chip');
const modelSelect = document.getElementById('model-select');
const btnToggleRecord = document.getElementById('btn-toggle-record');
const recordBtnText = document.getElementById('record-btn-text');
const recordBtnIcon = document.getElementById('record-btn-icon');

// Window Resizing & Hotwire Elements
const windowSelect = document.getElementById('window-select');
const btnScanWindows = document.getElementById('btn-scan-windows');
const windowW = document.getElementById('window-w');
const windowH = document.getElementById('window-h');
const windowX = document.getElementById('window-x');
const windowY = document.getElementById('window-y');
const windowObsSource = document.getElementById('window-obs-source');
const btnHotwireWindow = document.getElementById('btn-hotwire-window');
const sizePresetBtns = document.querySelectorAll('.size-preset-btn');
let currentHotwireSourceName = null;

// Connect to WebSocket Server
function connect() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const wsUrl = `${protocol}//${host}/ws`;
    
    console.log(`Connecting to WebSocket at ${wsUrl}`);
    ws = new WebSocket(wsUrl);
    
    ws.onopen = () => {
        console.log('WebSocket connected');
        updateStatusIndicator('connecting', 'Connecting to OBS...');
        // Fetch running windows list on connect
        setTimeout(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'command', action: 'list_system_windows' }));
            }
        }, 500);
    };
    
    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            handleWebSocketMessage(data);
        } catch (err) {
            console.error('Error parsing WebSocket message:', err, event.data);
        }
    };
    
    ws.onclose = () => {
        console.log('WebSocket disconnected. Reconnecting...');
        updateStatusIndicator('offline', 'Disconnected');
        clearOBSDashboard();
        setTimeout(connect, reconnectInterval);
    };
    
    ws.onerror = (err) => {
        console.error('WebSocket error:', err);
    };
}

function handleWebSocketMessage(msg) {
    switch (msg.type) {
        case 'set_listening':
            toggleListening(msg.enabled);
            break;

        case 'obs_status':
            updateOBSDashboard(msg.data);
            break;
            
        case 'agent_message_start':
            startAgentMessage(msg.message_id);
            break;
            
        case 'agent_message_chunk':
            appendAgentMessageChunk(msg.message_id, msg.text);
            break;
            
        case 'play_audio':
            if (isObsRecording) {
                playSoundDing();
            } else {
                playAudioBase64(msg.audio_base64);
            }
            break;
            
        case 'agent_message_end':
            endAgentMessage(msg.message_id);
            break;
            
        case 'agent_trace_step':
            addAgentTraceStep(msg.message_id, msg.step_type, msg.content);
            if (msg.step_type === 'error') {
                triggerErrorSound();
            }
            break;
            
        case 'system_notification':
            addSystemMessage(msg.text);
            if (msg.text.toLowerCase().includes('error') || msg.text.toLowerCase().includes('failed')) {
                triggerErrorSound();
            }
            break;
            
        case 'obs_connected':
            updateStatusIndicator('online', 'Connected to OBS');
            break;
            
        case 'obs_disconnected':
            updateStatusIndicator('offline', 'OBS Disconnected');
            clearOBSDashboard();
            break;
            
        case 'chat_history':
            renderChatHistory(msg.messages);
            break;

        case 'model_switched':
            if (modelSelect) {
                modelSelect.value = msg.model;
            }
            break;

        case 'system_windows_list':
            populateWindowSelect(msg.windows);
            break;
            
        case 'window_resized_details':
            handleWindowResized(msg.details);
            break;
            
        default:
            console.log('Unknown message type:', msg);
    }
}

function playAudioBase64(base64Data) {
    try {
        const audioUrl = `data:audio/mp3;base64,${base64Data}`;
        const audio = new Audio(audioUrl);
        audio.play().catch(err => {
            console.error("Failed to play audio:", err);
        });
    } catch (err) {
        console.error("Error creating/playing audio:", err);
    }
}

function playSoundDing() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const now = ctx.currentTime;
        
        // Note 1: C5 (523.25 Hz)
        const osc1 = ctx.createOscillator();
        const gain1 = ctx.createGain();
        osc1.type = 'sine';
        osc1.frequency.setValueAtTime(523.25, now);
        gain1.gain.setValueAtTime(0.08, now);
        gain1.gain.exponentialRampToValueAtTime(0.01, now + 0.08);
        osc1.connect(gain1);
        gain1.connect(ctx.destination);
        osc1.start(now);
        osc1.stop(now + 0.08);
        
        // Note 2: G5 (783.99 Hz)
        const osc2 = ctx.createOscillator();
        const gain2 = ctx.createGain();
        osc2.type = 'sine';
        osc2.frequency.setValueAtTime(783.99, now + 0.08);
        gain2.gain.setValueAtTime(0.08, now + 0.08);
        gain2.gain.exponentialRampToValueAtTime(0.0001, now + 0.4);
        osc2.connect(gain2);
        gain2.connect(ctx.destination);
        osc2.start(now + 0.08);
        osc2.stop(now + 0.4);
        console.log("[Audio] Played Ding sound (Recording mode active)");
    } catch (err) {
        console.error("Failed to play Ding sound:", err);
    }
}

function playSoundDonDong() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const now = ctx.currentTime;
        
        // Tone 1: Low warning note (140 Hz)
        const osc1 = ctx.createOscillator();
        const gain1 = ctx.createGain();
        osc1.type = 'triangle';
        osc1.frequency.setValueAtTime(140, now);
        gain1.gain.setValueAtTime(0.12, now);
        gain1.gain.exponentialRampToValueAtTime(0.01, now + 0.15);
        osc1.connect(gain1);
        gain1.connect(ctx.destination);
        osc1.start(now);
        osc1.stop(now + 0.15);
        
        // Tone 2: Lower warning note (100 Hz)
        const osc2 = ctx.createOscillator();
        const gain2 = ctx.createGain();
        osc2.type = 'triangle';
        osc2.frequency.setValueAtTime(100, now + 0.15);
        gain2.gain.setValueAtTime(0.12, now + 0.15);
        gain2.gain.exponentialRampToValueAtTime(0.0001, now + 0.45);
        osc2.connect(gain2);
        gain2.connect(ctx.destination);
        osc2.start(now + 0.15);
        osc2.stop(now + 0.45);
        console.log("[Audio] Played Don Dong warning sound (Recording mode active)");
    } catch (err) {
        console.error("Failed to play Don Dong sound:", err);
    }
}

function triggerErrorSound() {
    if (isObsRecording && !hasPlayedErrorSoundThisTurn) {
        playSoundDonDong();
        hasPlayedErrorSoundThisTurn = true;
    }
}

function updateStatusIndicator(status, text) {
    statusIndicator.className = `status-indicator ${status}`;
    statusText.textContent = text;
}

function clearOBSDashboard() {
    isObsRecording = false;
    hasAutoSetDimensions = false;
    streamVal.textContent = 'OFFLINE';
    streamVal.parentElement.parentElement.className = 'status-card';
    recordVal.textContent = 'OFFLINE';
    recordVal.parentElement.parentElement.className = 'status-card';
    virtualcamVal.textContent = 'OFFLINE';
    virtualcamVal.parentElement.parentElement.className = 'status-card';
    currentSceneName.textContent = 'None';
    sceneGrid.innerHTML = '<div class="loading-text">OBS not connected.</div>';
    audioMixer.innerHTML = '<div class="loading-text">OBS not connected.</div>';
    
    if (btnToggleRecord && recordBtnText && recordBtnIcon) {
        btnToggleRecord.style.background = 'rgba(239, 68, 68, 0.15)';
        btnToggleRecord.style.color = '#f87171';
        btnToggleRecord.style.borderColor = 'rgba(239, 68, 68, 0.3)';
        btnToggleRecord.style.boxShadow = 'none';
        recordBtnText.textContent = 'Start OBS Recording';
        recordBtnIcon.setAttribute('data-lucide', 'video');
        lucide.createIcons();
    }
}

// Update the Left Panel Dashboard
function updateOBSDashboard(data) {
    if (!data) return;
    
    // Status metrics
    const isStreaming = data.streaming;
    const isRecording = data.recording;
    isObsRecording = isRecording;
    const isVirtualCam = data.virtual_cam;
    
    streamVal.textContent = isStreaming ? 'STREAMING' : 'OFFLINE';
    streamVal.parentElement.parentElement.className = `status-card ${isStreaming ? 'active-streaming' : ''}`;
    
    recordVal.textContent = isRecording ? 'RECORDING' : 'OFFLINE';
    recordVal.parentElement.parentElement.className = `status-card ${isRecording ? 'active-recording' : ''}`;
    
    virtualcamVal.textContent = isVirtualCam ? 'ACTIVE' : 'OFFLINE';
    virtualcamVal.parentElement.parentElement.className = `status-card ${isVirtualCam ? 'active-virtualcam' : ''}`;
    
    // Update top record button styling
    if (btnToggleRecord && recordBtnText && recordBtnIcon) {
        if (isRecording) {
            btnToggleRecord.style.background = 'var(--accent-red)';
            btnToggleRecord.style.color = '#ffffff';
            btnToggleRecord.style.borderColor = 'transparent';
            btnToggleRecord.style.boxShadow = '0 0 12px var(--accent-red)';
            recordBtnText.textContent = 'Stop OBS Recording';
            recordBtnIcon.setAttribute('data-lucide', 'video-off');
        } else {
            btnToggleRecord.style.background = 'rgba(239, 68, 68, 0.15)';
            btnToggleRecord.style.color = '#f87171';
            btnToggleRecord.style.borderColor = 'rgba(239, 68, 68, 0.3)';
            btnToggleRecord.style.boxShadow = 'none';
            recordBtnText.textContent = 'Start OBS Recording';
            recordBtnIcon.setAttribute('data-lucide', 'video');
        }
        lucide.createIcons();
    }
    
    // Video Canvas Size
    if (resolutionVal) {
        if (data.base_width && data.base_height) {
            resolutionVal.textContent = `${data.base_width}x${data.base_height}`;
            resolutionVal.parentElement.parentElement.className = 'status-card active-resolution';

            // Update dynamic Canvas preset button
            const btnPresetCanvas = document.getElementById('btn-preset-canvas');
            if (btnPresetCanvas) {
                btnPresetCanvas.setAttribute('data-width', data.base_width);
                btnPresetCanvas.setAttribute('data-height', data.base_height);
                btnPresetCanvas.textContent = `${data.base_width}p`;
            }

            // Auto-fill width/height inputs with the actual canvas size on first connection
            if (!hasAutoSetDimensions) {
                if (windowW) windowW.value = data.base_width;
                if (windowH) windowH.value = data.base_height;
                hasAutoSetDimensions = true;
            }
        } else {
            resolutionVal.textContent = 'UNKNOWN';
            resolutionVal.parentElement.parentElement.className = 'status-card';
        }
    }
    
    // Active scene
    currentSceneName.textContent = data.current_scene || 'None';
    
    // Scenes List
    if (data.scenes && data.scenes.length > 0) {
        sceneGrid.innerHTML = '';
        data.scenes.forEach(scene => {
            const btn = document.createElement('button');
            const isActive = scene === data.current_scene;
            btn.className = `scene-btn ${isActive ? 'active' : ''}`;
            btn.textContent = scene;
            btn.addEventListener('click', () => {
                sendDirectCommand('switch_scene', { scene_name: scene });
            });
            sceneGrid.appendChild(btn);
        });
    } else {
        sceneGrid.innerHTML = '<div class="loading-text">No scenes found.</div>';
    }
    
    // Audio Mixer
    if (data.audio_inputs && data.audio_inputs.length > 0) {
        audioMixer.innerHTML = '';
        data.audio_inputs.forEach(input => {
            const track = document.createElement('div');
            track.className = 'audio-track';
            
            const volumeDb = input.volume_db !== null ? `${input.volume_db.toFixed(1)} dB` : `${(input.volume_mul * 100).toFixed(0)}%`;
            const muteIcon = input.muted ? 'volume-x' : 'volume-2';
            
            track.innerHTML = `
                <div class="track-header">
                    <span class="track-name">${input.name}</span>
                    <div class="track-controls">
                        <button class="btn-mute ${input.muted ? 'muted' : ''}" data-input-name="${input.name}">
                            <i data-lucide="${muteIcon}"></i>
                        </button>
                    </div>
                </div>
                <div class="slider-container">
                    <input type="range" min="0" max="1" step="0.01" class="volume-slider ${input.muted ? 'muted-slider' : ''}" 
                           value="${input.volume_mul}" data-input-name="${input.name}">
                    <span>${volumeDb}</span>
                </div>
            `;
            
            // Wire up mute toggle
            const muteBtn = track.querySelector('.btn-mute');
            muteBtn.addEventListener('click', () => {
                sendDirectCommand('set_mute', { input_name: input.name, muted: !input.muted });
            });
            
            // Wire up volume slider
            const slider = track.querySelector('.volume-slider');
            slider.addEventListener('input', (e) => {
                const vol = parseFloat(e.target.value);
                // Simple DB calculation just for display
                const dbSpan = slider.nextElementSibling;
                if (vol === 0) {
                    dbSpan.textContent = '-inf dB';
                } else {
                    const db = 20 * Math.log10(vol);
                    dbSpan.textContent = `${db.toFixed(1)} dB`;
                }
            });
            slider.addEventListener('change', (e) => {
                const vol = parseFloat(e.target.value);
                sendDirectCommand('set_volume', { input_name: input.name, volume: vol });
            });
            
            audioMixer.appendChild(track);
        });
        lucide.createIcons({ attrs: { class: 'audio-icon' } });
    } else {
        audioMixer.innerHTML = '<div class="loading-text">No audio inputs.</div>';
    }
}

// Chat UI functions
function startAgentMessage(messageId) {
    currentAgentMessageId = messageId;
    hasPlayedErrorSoundThisTurn = false;
    
    // Create new agent message block
    const msgDiv = document.createElement('div');
    msgDiv.className = 'message agent-message';
    msgDiv.id = `msg-${messageId}`;
    
    const contentDiv = document.createElement('div');
    contentDiv.className = 'message-content';
    
    const textP = document.createElement('p');
    textP.className = 'message-text';
    textP.innerHTML = '<em>Thinking...</em>';
    contentDiv.appendChild(textP);
    
    // Add execution trace wrapper
    const traceDiv = document.createElement('div');
    traceDiv.className = 'execution-trace';
    traceDiv.innerHTML = `
        <div class="trace-header">
            <span>AI Reasoning & OBS Tool Execution Logs</span>
            <i data-lucide="chevron-down"></i>
        </div>
        <div class="trace-body" style="display: none;"></div>
    `;
    
    const traceHeader = traceDiv.querySelector('.trace-header');
    const traceBody = traceDiv.querySelector('.trace-body');
    currentTraceBody = traceBody;
    
    traceHeader.addEventListener('click', () => {
        const isHidden = traceBody.style.display === 'none';
        traceBody.style.display = isHidden ? 'flex' : 'none';
        const chevron = traceHeader.querySelector('[data-lucide]');
        if (chevron) {
            chevron.setAttribute('data-lucide', isHidden ? 'chevron-up' : 'chevron-down');
        }
        lucide.createIcons();
    });
    
    msgDiv.appendChild(contentDiv);
    msgDiv.appendChild(traceDiv);
    chatMessages.appendChild(msgDiv);
    
    lucide.createIcons();
    scrollToBottom();
}

function appendAgentMessageChunk(messageId, text) {
    const msgDiv = document.getElementById(`msg-${messageId}`);
    if (!msgDiv) return;
    
    const textP = msgDiv.querySelector('.message-text');
    if (textP.innerHTML.includes('<em>Thinking...</em>')) {
        textP.innerHTML = '';
        textP.rawText = '';
    }
    if (textP.rawText === undefined) {
        textP.rawText = textP.textContent || '';
    }
    textP.rawText += text;
    
    let display = textP.rawText;
    if (display.includes('<voice_summary>')) {
        display = display.split('<voice_summary>')[0];
    }
    textP.textContent = display.trim();
    scrollToBottom();
}

function endAgentMessage(messageId) {
    currentAgentMessageId = null;
    currentTraceBody = null;
    scrollToBottom();
}

function addAgentTraceStep(messageId, stepType, content) {
    let traceBody = currentTraceBody;
    if (!traceBody) {
        const msgDiv = document.getElementById(`msg-${messageId}`);
        if (msgDiv) {
            traceBody = msgDiv.querySelector('.trace-body');
        }
    }
    
    if (!traceBody) return;
    
    const stepDiv = document.createElement('div');
    stepDiv.className = `trace-step ${stepType}`;
    
    let icon = 'brain';
    if (stepType === 'tool-call') icon = 'play';
    if (stepType === 'tool-result') icon = 'check-circle-2';
    if (stepType === 'error') icon = 'alert-triangle';
    
    stepDiv.innerHTML = `
        <span class="trace-step-icon"><i data-lucide="${icon}"></i></span>
        <span class="trace-step-content">${content}</span>
    `;
    
    traceBody.appendChild(stepDiv);
    lucide.createIcons();
    scrollToBottom();
}

function addSystemMessage(text) {
    const msgDiv = document.createElement('div');
    msgDiv.className = 'message system-message';
    msgDiv.innerHTML = `<div class="message-content"><p>${text}</p></div>`;
    chatMessages.appendChild(msgDiv);
    scrollToBottom();
}

function addSelfMessage(text) {
    const msgDiv = document.createElement('div');
    msgDiv.className = 'message user-message';
    msgDiv.innerHTML = `<div class="message-content"><p>${text}</p></div>`;
    chatMessages.appendChild(msgDiv);
    scrollToBottom();
}

function scrollToBottom() {
    chatMessages.scrollTop = chatMessages.scrollHeight;
    // Delay slightly to ensure layout reflow has finished computing new heights
    setTimeout(() => {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }, 20);
}

// Command Transmission
function sendChatMessage(text, isVoice = false) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        addSystemMessage('Cannot send message. Server connection is offline.');
        return;
    }
    
    addSelfMessage(text);
    ws.send(JSON.stringify({
        type: 'chat',
        text: text,
        model: modelSelect ? modelSelect.value : 'claude',
        is_voice: isVoice
    }));
}

function sendDirectCommand(action, params = {}) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        addSystemMessage('Cannot run command. Server connection is offline.');
        return;
    }
    
    ws.send(JSON.stringify({
        type: 'command',
        action: action,
        params: params
    }));
}

// Event Listeners
chatForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const text = chatInput.value.trim();
    if (!text) return;
    
    sendChatMessage(text);
    chatInput.value = '';
    if (btnClearInput) btnClearInput.style.display = 'none';
});

// Clear Input Button logic
if (chatInput && btnClearInput) {
    chatInput.addEventListener('input', () => {
        btnClearInput.style.display = chatInput.value ? 'flex' : 'none';
    });
    
    btnClearInput.addEventListener('click', () => {
        chatInput.value = '';
        chatInput.focus();
        btnClearInput.style.display = 'none';
    });
}

// Reconnect OBS Button
btnReconnectObs.addEventListener('click', () => {
    sendDirectCommand('reconnect_obs');
    addSystemMessage('Requesting OBS connection reset...');
});

// Toggle Recording Button
if (btnToggleRecord) {
    btnToggleRecord.addEventListener('click', () => {
        sendDirectCommand('toggle_record');
    });
}

// Clear History Button
if (btnClearHistory) {
    btnClearHistory.addEventListener('click', () => {
        sendDirectCommand('clear_history');
        // Clear chat messages container, keeping the welcome message
        chatMessages.innerHTML = `
            <div class="message system-message">
                <div class="message-content">
                    <p>Welcome! I'm your OBS AI Assistant. Ask me to change scenes, toggle source visibility, mute audio, or control recording and streaming. E.g. "Switch to BRB scene and mute the mic."</p>
                </div>
            </div>
        `;
        addSystemMessage('Chat history cleared.');
    });
}

function renderChatHistory(messages) {
    if (!messages) return;
    
    // Clear chat container, keeping only the welcome message
    chatMessages.innerHTML = `
        <div class="message system-message">
            <div class="message-content">
                <p>Welcome! I'm your OBS AI Assistant. Ask me to change scenes, toggle source visibility, mute audio, or control recording and streaming. E.g. "Switch to BRB scene and mute the mic."</p>
            </div>
        </div>
    `;
    
    messages.forEach(msg => {
        if (msg.role === 'user') {
            addSelfMessage(msg.text);
        } else if (msg.role === 'assistant') {
            // Create assistant message block
            const msgDiv = document.createElement('div');
            msgDiv.className = 'message agent-message';
            
            const contentDiv = document.createElement('div');
            contentDiv.className = 'message-content';
            
            const textP = document.createElement('p');
            textP.className = 'message-text';
            let display = msg.text;
            if (display.includes('<voice_summary>')) {
                display = display.split('<voice_summary>')[0];
            }
            textP.textContent = display.trim();
            contentDiv.appendChild(textP);
            msgDiv.appendChild(contentDiv);
            
            chatMessages.appendChild(msgDiv);
        }
    });
    scrollToBottom();
}

// Prompt Chips
suggestionChips.forEach(chip => {
    chip.addEventListener('click', () => {
        const text = chip.getAttribute('data-prompt');
        chatInput.value = '';
        if (btnClearInput) btnClearInput.style.display = 'none';
        sendChatMessage(text);
    });
});

function populateWindowSelect(windows) {
    if (!windowSelect) return;
    windowSelect.innerHTML = '<option value="">Select running window...</option>';
    if (windows && windows.length > 0) {
        windows.forEach(win => {
            const opt = document.createElement('option');
            opt.value = win.title;
            opt.textContent = `[${win.process_name}] ${win.title}`;
            windowSelect.appendChild(opt);
        });
    } else {
        const opt = document.createElement('option');
        opt.value = "";
        opt.textContent = "No windows detected.";
        windowSelect.appendChild(opt);
    }
}

function handleWindowResized(details) {
    addSystemMessage(`Window "${details.title}" resized to ${details.width}x${details.height} at (${details.x}, ${details.y}).`);
    if (currentHotwireSourceName) {
        const activeScene = currentSceneName ? currentSceneName.textContent : null;
        if (!activeScene || activeScene === 'None') {
            addSystemMessage('Error: Cannot hotwire. No active OBS scene selected.');
            currentHotwireSourceName = null;
            return;
        }
        addSystemMessage(`Hotwiring "${details.title}" into OBS source "${currentHotwireSourceName}"...`);
        sendDirectCommand('set_obs_window_capture', {
            scene_name: activeScene,
            source_name: currentHotwireSourceName,
            window_identifier: details.obs_identifier
        });
        currentHotwireSourceName = null;
    }
}

// Window control listeners
if (btnScanWindows) {
    btnScanWindows.addEventListener('click', () => {
        sendDirectCommand('list_system_windows');
        addSystemMessage('Scanning system windows...');
    });
}

sizePresetBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        const w = btn.getAttribute('data-width');
        const h = btn.getAttribute('data-height');
        if (windowW) windowW.value = w;
        if (windowH) windowH.value = h;
    });
});

if (btnHotwireWindow) {
    btnHotwireWindow.addEventListener('click', () => {
        const targetWindow = windowSelect ? windowSelect.value : '';
        if (!targetWindow) {
            addSystemMessage('Error: Please select a target window from the dropdown.');
            return;
        }
        const w = parseInt(windowW.value) || 1280;
        const h = parseInt(windowH.value) || 720;
        const x = parseInt(windowX.value) || 0;
        const y = parseInt(windowY.value) || 0;
        const sourceName = windowObsSource ? windowObsSource.value.trim() : 'Window Capture';
        if (!sourceName) {
            addSystemMessage('Error: Please specify an OBS source name.');
            return;
        }
        currentHotwireSourceName = sourceName;
        addSystemMessage(`Restoring & resizing "${targetWindow}"...`);
        sendDirectCommand('resize_window', {
            window_title: targetWindow,
            width: w,
            height: h,
            x: x,
            y: y
        });
    });
}

// Voice Transcription & Listening Logic
let isListening = false;
let mediaRecorder = null;
let audioStream = null;
let audioContext = null;
let audioAnalyser = null;
let audioChunks = [];
let silenceStartTime = null;
let isSpeaking = false;
let hasSpokenDuringSession = false;
let recordingStartTime = Date.now();

const btnVoice = document.getElementById('btn-voice');
const voiceIcon = document.getElementById('voice-icon');

function toggleListening(forceState) {
    const targetState = forceState !== undefined ? forceState : !isListening;
    if (targetState === isListening) return;

    isListening = targetState;
    if (isListening) {
        if (btnVoice) btnVoice.classList.add('active');
        if (voiceIcon) {
            voiceIcon.setAttribute('data-lucide', 'mic');
            lucide.createIcons();
        }
        addSystemMessage("Real-time voice listening enabled. (Addressing name: 'OBSy')");
        startRecordingStream();
    } else {
        if (btnVoice) btnVoice.classList.remove('active');
        if (voiceIcon) {
            voiceIcon.setAttribute('data-lucide', 'mic-off');
            lucide.createIcons();
        }
        addSystemMessage("Voice listening disabled.");
        stopRecordingStream();
    }
}

async function startRecordingStream() {
    try {
        audioStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        
        mediaRecorder = new MediaRecorder(audioStream, { mimeType: 'audio/webm' });
        audioChunks = [];
        
        mediaRecorder.ondataavailable = (event) => {
            if (event.data.size > 0) {
                audioChunks.push(event.data);
            }
        };
        
        mediaRecorder.onstop = async () => {
            if (audioChunks.length > 0 && isListening && hasSpokenDuringSession) {
                const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
                audioChunks = [];
                sendAudioForTranscription(audioBlob);
            } else {
                if (audioChunks.length > 0) {
                    console.log("No speech detected during this period (volume below threshold). Skipping transcription.");
                }
                audioChunks = [];
            }
            hasSpokenDuringSession = false; // Reset for next session
            
            // Safe asynchronous restart if still listening
            if (isListening && mediaRecorder && mediaRecorder.state === 'inactive') {
                try {
                    mediaRecorder.start();
                    recordingStartTime = Date.now();
                } catch (err) {
                    console.error("Failed to restart mediaRecorder:", err);
                }
            }
        };

        mediaRecorder.start();
        recordingStartTime = Date.now();
        hasSpokenDuringSession = false;

        // Silence detection
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        const source = audioContext.createMediaStreamSource(audioStream);
        audioAnalyser = audioContext.createAnalyser();
        audioAnalyser.fftSize = 512;
        source.connect(audioAnalyser);
        
        const bufferLength = audioAnalyser.frequencyBinCount;
        const dataArray = new Uint8Array(bufferLength);
        
        silenceStartTime = null;
        isSpeaking = false;
        
        function analyzeAudio() {
            if (!isListening) return;
            
            audioAnalyser.getByteFrequencyData(dataArray);
            let sum = 0;
            for (let i = 0; i < bufferLength; i++) {
                sum += dataArray[i];
            }
            const averageVolume = sum / bufferLength;
            
            // Speech detection threshold (0-255 scale)
            const speechThreshold = 12; 
            
            if (averageVolume > speechThreshold) {
                if (!isSpeaking) {
                    console.log("Speech detected...");
                    isSpeaking = true;
                }
                hasSpokenDuringSession = true;
                silenceStartTime = null;
            } else {
                if (isSpeaking) {
                    if (!silenceStartTime) {
                        silenceStartTime = Date.now();
                    } else if (Date.now() - silenceStartTime > 1500) { 
                        console.log("Silence detected, transcribing...");
                        isSpeaking = false;
                        silenceStartTime = null;
                        
                        if (mediaRecorder && mediaRecorder.state === 'recording') {
                            mediaRecorder.stop();
                        }
                    }
                } else {
                    // Periodic reset if user has not spoken for 4 seconds, to keep the pre-speech buffer short
                    if (Date.now() - recordingStartTime > 4000) {
                        if (mediaRecorder && mediaRecorder.state === 'recording') {
                            mediaRecorder.stop();
                        }
                    }
                }
            }
            
            if (isListening) {
                requestAnimationFrame(analyzeAudio);
            }
        }
        
        analyzeAudio();
        
    } catch (err) {
        console.error('Mic access error:', err);
        addSystemMessage("Error: Could not access microphone.");
        toggleListening(false);
    }
}

function stopRecordingStream() {
    if (mediaRecorder && mediaRecorder.state !== 'inactive') {
        mediaRecorder.stop();
    }
    if (audioStream) {
        audioStream.getTracks().forEach(track => track.stop());
        audioStream = null;
    }
    if (audioContext && audioContext.state !== 'closed') {
        audioContext.close();
        audioContext = null;
    }
    mediaRecorder = null;
    audioAnalyser = null;
    audioChunks = [];
}

async function sendAudioForTranscription(blob) {
    const formData = new FormData();
    formData.append('file', blob, 'audio.webm');
    
    try {
        const response = await fetch('/api/transcribe', {
            method: 'POST',
            body: formData
        });
        
        if (response.ok) {
            const result = await response.json();
            const transcript = result.text ? result.text.trim() : '';
            if (transcript.length > 0) {
                handleVoiceTranscript(transcript);
            }
        } else {
            console.error('Failed to transcribe:', response.statusText);
        }
    } catch (err) {
        console.error('Error during transcription:', err);
    }
}

function handleVoiceTranscript(transcript) {
    const canHearMeRegex = /\bcan\s+you\s+hear\s+me\b/i;
    const isCanHearMe = canHearMeRegex.test(transcript);
    
    if (isObsRecording) {
        // Voice activation mode: only trigger if wake word is spoken
        const wakeWordRegex = /^(?:hey\s+)?obs(?:[- ]?y)?[:,]?\s*/i;
        
        if (wakeWordRegex.test(transcript)) {
            const cleanedText = transcript.replace(wakeWordRegex, '').trim();
            if (cleanedText.length > 0) {
                addSystemMessage(`Voice command detected: "${transcript}"`);
                const voiceFlag = canHearMeRegex.test(cleanedText);
                sendChatMessage(cleanedText, voiceFlag);
            }
        } else {
            console.log(`Heard (ignored in recording mode): "${transcript}"`);
        }
    } else {
        // Dictation mode: if they say "can you hear me", trigger it immediately as a voice command
        if (isCanHearMe) {
            addSystemMessage(`Voice query detected: "${transcript}"`);
            sendChatMessage(transcript, true);
        } else {
            // Just type what we heard into the text input box
            if (chatInput) {
                const currentVal = chatInput.value.trim();
                chatInput.value = (currentVal ? currentVal + " " : "") + transcript;
                if (btnClearInput) btnClearInput.style.display = 'flex';
                chatInput.focus();
                console.log(`Heard (dictation): "${transcript}"`);
            }
        }
    }
}

if (btnVoice) {
    btnVoice.addEventListener('click', () => {
        toggleListening();
    });
}

// Start Client
connect();
clearOBSDashboard();

