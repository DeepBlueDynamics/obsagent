# 🚀 OBS Agentic Control Interface (obsagent)

> [!NOTE]
> **🤖 100% Coded by AI**: This entire repository and application was engineered 100% autonomously by **Antigravity**, an agentic AI coding assistant.

A powerful, self-contained agentic interface built in Rust that allows you to control OBS Studio via natural language. The system features an advanced reasoning loop powered by Claude (`claude-3-5-sonnet`) and OpenAI (`gpt-4o`), automatically connected to OBS Studio via WebSocket v5.

---

## ✨ Features

- **📊 Real-Time Interactive Dashboard**: Monitors streaming, recording, virtual camera, active program scenes, and audio mixer volume levels dynamically.
- **🖥️ Canvas Size & Output Resolution Detection**: Automatically queries your OBS Canvas and scaled output resolutions, displaying them in a dedicated status card and feeding them into the AI agent's system prompt context.
- **🧠 Intelligent Auto-Fallback Agent Loop**: 
  - Routes requests through a robust agent loop with support for both Claude and OpenAI.
  - **Dynamic failover**: If the selected provider key or network request fails, the backend automatically switches to the alternative provider (Claude ⇄ OpenAI) and finishes your request seamlessly without duplicating message history.
- **🔌 Windows Hotwire & WGC Binding**:
  - Remotely scans, restores, focuses, and resizes target application windows on your host OS.
  - Automatically binds windows into OBS using **Windows Graphics Capture (WGC / Windows 10 method)** to prevent black/grayed-out capture screens.
  - Uses an **intelligent fuzzy resolver** to match window titles/classes against OBS's active window pool to ensure 100% reliable binding.
- **🎙️ Real-Time Voice Activation ("OBSy")**: 
  - Uses browser-side VAD (Voice Activity Detection) and OpenAI's Whisper API for high-fidelity speech-to-text.
  - Triggers on the wake word `"OBSy"`. Allows natural language control directly from your microphone.
- **🎛️ Manual Sidebar Controls**: Instant buttons to trigger scene changes, volume adjustments, and transitions that stay fully in sync with the AI's state.

---

## 📋 Prerequisites

1. **OBS Studio** (v28.0 or later, featuring native WebSocket support).
2. **Rust Toolchain** (pre-configured in this workspace environment).
3. **GitHub CLI (`gh`)** (for repository authentication and deployment).

---

## ⚙️ Setup & Configuration

### 1. Enable OBS WebSocket Server
1. Open OBS Studio.
2. Navigate to **Tools** ➔ **WebSocket Server Settings**.
3. Check **Enable WebSocket server**.
4. Take note of the port (default: `4455`) and the password (or generate a new one).

### 2. Environment Variables
To start the server, configure your credentials and connection endpoints as environment variables:

```bash
# Set your API keys (one or both are supported)
export ANTHROPIC_API_KEY="your-anthropic-api-key"
export OPENAI_API_KEY="your-openai-api-key"
export ELEVENLABS_API_KEY="your-elevenlabs-api-key"

# Set the connection host for OBS Studio
# When running inside a container, set to "host.docker.internal" to connect to the host
export OBS_HOST="host.docker.internal"
export OBS_PORT="4455"
export OBS_PASSWORD="your-obs-websocket-password"
```

---

## 🏃 Running the Application

To compile and launch the Axum web server, run the following in the workspace directory:

```bash
cargo run
```

The server will initialize and serve the frontend dashboard at `http://localhost:8080`.

Open your browser, connect OBS, and start commanding your streams via text or voice!
