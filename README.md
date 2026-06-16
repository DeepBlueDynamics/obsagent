# OBS Agentic Control Interface

A powerful, self-contained agentic interface built in Rust that allows you to control OBS Studio via natural language using an agent loop powered by Claude (`claude-3-5-sonnet`) and OBS WebSocket v5.

## Features

- **Real-Time Interactive Dashboard**: Monitors streaming, recording, virtual camera, active scenes, and audio mixer volume levels dynamically.
- **Agentic Chat Control**: Chat with an LLM agent that has full access to OBS actions. You can type commands like `"Switch to the BRB scene and mute the mic"` or `"Check if I am recording, and if not, start recording"`.
- **Collapsible Reasoning Logs**: Watch the agent's thought process, tool execution calls, and OBS responses in real-time.
- **Quick Action Sidebar**: Instant buttons to trigger scene switches, mute tracks, and adjust volume sliders manually, fully syncing with the AI's state.
- **Responsive Media Layout**: Media layouts dynamically scale and adjust without horizontal cropping.

---

## Prerequisites

1. **OBS Studio** (v28.0 or later, which has built-in WebSocket support).
2. **Rust Toolchain** (installed automatically in this environment).

---

## Setup & Configuration

### 1. Enable OBS WebSocket Server
1. Open OBS Studio.
2. Navigate to **Tools** -> **WebSocket Server Settings**.
3. Check **Enable WebSocket server**.
4. Take note of the port (default is `4455`).
5. Check **Enable Authentication** and note the password (or generate a new one).

### 2. Environment Variables
To run the server, you need to provide your API keys, OBS address, and password as environment variables:

```bash
# Set your Anthropic API Key (Already set in this environment for Claude support)
export ANTHROPIC_API_KEY="your-anthropic-api-key"

# Set your OpenAI API Key (For OpenAI support)
export OPENAI_API_KEY="your-openai-api-key"

# Set the host where OBS is running (default: 127.0.0.1)
# When running inside a container, set this to "host.docker.internal" to connect to the host
export OBS_HOST="host.docker.internal"

# Set the OBS WebSocket port (default: 4455)
export OBS_PORT="4455"

# Set your OBS WebSocket password (if authentication is enabled)
export OBS_PASSWORD="your-obs-websocket-password"
```

---

## Running the Application

To build and run the Rust server, run the following command in the workspace directory:

```bash
cargo run
```

The server will start at `http://localhost:8080`.

Open your browser and visit [http://localhost:8080](http://localhost:8080) to interact with the dashboard and start chatting with the agent!
# obsagent
# obsagent
