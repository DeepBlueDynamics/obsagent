use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock, Mutex};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use tower_http::services::ServeDir;
use serde::{Deserialize, Serialize};
use serde_json::json;
use obws::Client;
use futures_util::{SinkExt, StreamExt};
use anyhow::{anyhow, Result};
use base64::Engine;


static MESSAGE_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChatMessage {
    pub role: String,
    pub text: String,
}

// OBS structures
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ObsStatus {
    pub streaming: bool,
    pub recording: bool,
    pub virtual_cam: bool,
    pub current_scene: Option<String>,
    pub scenes: Vec<String>,
    pub audio_inputs: Vec<AudioInputInfo>,
    pub base_width: u32,
    pub base_height: u32,
    pub output_width: u32,
    pub output_height: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AudioInputInfo {
    pub name: String,
    pub muted: bool,
    pub volume_mul: f32,
    pub volume_db: Option<f32>,
}

// Client messages
#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "chat")]
    Chat {
        text: String,
        model: Option<String>,
    },
    #[serde(rename = "command")]
    Command {
        action: String,
        #[serde(default)]
        params: serde_json::Value,
    },
}

// Agent tool representation
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "name", content = "input")]
pub enum ObsTool {
    #[serde(rename = "list_scenes")]
    ListScenes {},
    #[serde(rename = "switch_scene")]
    SwitchScene { scene_name: String },
    #[serde(rename = "get_audio_inputs")]
    GetAudioInputs {},
    #[serde(rename = "set_mute")]
    SetMute { input_name: String, muted: bool },
    #[serde(rename = "set_volume")]
    SetVolume { input_name: String, volume: f32 },
    #[serde(rename = "get_stream_recording_status")]
    GetStreamRecordingStatus {},
    #[serde(rename = "control_stream")]
    ControlStream { action: String },
    #[serde(rename = "control_record")]
    ControlRecord { action: String },
    #[serde(rename = "control_virtualcam")]
    ControlVirtualCam { action: String },
    #[serde(rename = "set_source_visibility")]
    SetSourceVisibility {
        scene_name: String,
        item_name: String,
        enabled: bool,
    },
    #[serde(rename = "create_scene")]
    CreateScene { scene_name: String },
    #[serde(rename = "remove_scene")]
    RemoveScene { scene_name: String },
    #[serde(rename = "create_input")]
    CreateInput {
        scene_name: String,
        input_name: String,
        input_kind: String,
        #[serde(default)]
        enabled: Option<bool>,
    },
    #[serde(rename = "remove_input")]
    RemoveInput { input_name: String },
    #[serde(rename = "list_system_windows")]
    ListSystemWindows {},
    #[serde(rename = "resize_and_focus_window")]
    ResizeAndFocusWindow {
        window_title: String,
        width: u32,
        height: u32,
        x: Option<i32>,
        y: Option<i32>,
    },
    #[serde(rename = "set_obs_window_capture")]
    SetObsWindowCapture {
        scene_name: String,
        source_name: String,
        window_identifier: String,
    },
    #[serde(rename = "list_transitions")]
    ListTransitions {},
    #[serde(rename = "set_transition")]
    SetTransition { transition_name: String },
    #[serde(rename = "set_transition_duration")]
    SetTransitionDuration { duration_ms: u32 },
}

// Claude API representation
#[derive(Serialize)]
struct ClaudeMessageRequest {
    model: String,
    max_tokens: u32,
    system: String,
    messages: Vec<ClaudeMessage>,
    tools: Vec<ClaudeTool>,
}

#[derive(Serialize, Deserialize, Clone)]
struct ClaudeMessage {
    role: String,
    content: ClaudeContent,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum ClaudeContent {
    Text(String),
    Blocks(Vec<ClaudeBlock>),
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
enum ClaudeBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ClaudeTool {
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

#[derive(Deserialize)]
struct ClaudeMessageResponse {
    content: Vec<ClaudeBlock>,
}

// App State
struct AppState {
    obs_client: Arc<RwLock<Option<Client>>>,
    status_tx: broadcast::Sender<ObsStatus>,
    reconnect_tx: broadcast::Sender<()>,
    chat_history: Arc<Mutex<Vec<ChatMessage>>>,
    hyperia_tools: Arc<RwLock<Vec<ClaudeTool>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let obs_client = Arc::new(RwLock::new(None));
    let (status_tx, _) = broadcast::channel(16);
    let (reconnect_tx, _) = broadcast::channel(16);
    let chat_history = Arc::new(Mutex::new(Vec::<ChatMessage>::new()));
    let hyperia_tools = Arc::new(RwLock::new(Vec::<ClaudeTool>::new()));

    let state = Arc::new(AppState {
        obs_client: obs_client.clone(),
        status_tx: status_tx.clone(),
        reconnect_tx: reconnect_tx.clone(),
        chat_history,
        hyperia_tools: hyperia_tools.clone(),
    });

    // Spawn tokio task to fetch hyperia tools periodically
    let hyperia_tools_clone = hyperia_tools.clone();
    tokio::spawn(async move {
        loop {
            fetch_hyperia_tools(hyperia_tools_clone.clone()).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });

    // Spawn OBS connection & status loop
    let obs_client_clone = obs_client.clone();
    let status_tx_clone = status_tx.clone();
    let mut reconnect_rx = reconnect_tx.subscribe();
    tokio::spawn(async move {
        loop {
            let mut reconnect_needed = false;
            
            // Scope for read guard to verify connection
            {
                let read_guard = obs_client_clone.read().await;
                if let Some(client) = &*read_guard {
                    if client.general().version().await.is_err() {
                        reconnect_needed = true;
                    }
                } else {
                    reconnect_needed = true;
                }
            }

            if reconnect_needed {
                let host = std::env::var("OBS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
                let port = std::env::var("OBS_PORT")
                    .ok()
                    .and_then(|p| p.parse::<u16>().ok())
                    .unwrap_or(4455);

                println!("[OBS] Attempting connection to {}:{}...", host, port);
                let password = std::env::var("OBS_PASSWORD").ok();
                
                // Clear any broken connection
                {
                    let mut write_guard = obs_client_clone.write().await;
                    *write_guard = None;
                }

                match Client::connect(&host, port, password.as_deref()).await {
                    Ok(client) => {
                        println!("[OBS] Connected successfully!");
                        let mut write_guard = obs_client_clone.write().await;
                        *write_guard = Some(client);
                    }
                    Err(e) => {
                        eprintln!("[OBS] Connection failed: {}. Retrying...", e);
                    }
                }
            }

            // If connected, fetch and broadcast status
            {
                let read_guard = obs_client_clone.read().await;
                if let Some(client) = &*read_guard {
                    if let Ok(status) = get_obs_status(client).await {
                        let _ = status_tx_clone.send(status);
                    }
                }
            }

            // Sleep, but abort sleep early if a manual reconnect signal is received
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(3)) => {}
                _ = reconnect_rx.recv() => {
                    println!("[OBS] Manual reconnect signal received.");
                    // Force reconnect on next iteration
                    let mut write_guard = obs_client_clone.write().await;
                    *write_guard = None;
                }
            }
        }
    });

    // Axum router
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/api/transcribe", axum::routing::post(transcribe_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(state);

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Server starting on http://localhost:{}", port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn get_obs_status(client: &Client) -> Result<ObsStatus> {
    // 1. Streaming status
    let streaming = client.streaming().status().await
        .map(|s| s.active)
        .unwrap_or(false);

    // 2. Recording status
    let recording = client.recording().status().await
        .map(|s| s.active)
        .unwrap_or(false);

    // 3. Virtual Cam status
    let virtual_cam = client.virtual_cam().status().await
        .unwrap_or(false);

    // 4. Scenes list
    let scenes_list = client.scenes().list().await?;
    let current_scene = scenes_list.current_program_scene.as_ref().map(|s| s.name.clone());
    let scenes = scenes_list.scenes.into_iter().map(|s| s.id.name).collect();

    // 5. Audio inputs
    let audio_inputs = get_audio_inputs(client).await.unwrap_or_default();

    // 6. Video settings (resolutions)
    let (base_width, base_height, output_width, output_height) = if let Ok(vs) = client.config().video_settings().await {
        (vs.base_width, vs.base_height, vs.output_width, vs.output_height)
    } else {
        (0, 0, 0, 0)
    };

    Ok(ObsStatus {
        streaming,
        recording,
        virtual_cam,
        current_scene,
        scenes,
        audio_inputs,
        base_width,
        base_height,
        output_width,
        output_height,
    })
}

async fn get_audio_inputs(client: &Client) -> Result<Vec<AudioInputInfo>> {
    let inputs = client.inputs().list(None).await?;
    let mut audio_inputs = Vec::new();
    for input in inputs {
        if let Ok(muted) = client.inputs().muted(obws::requests::inputs::InputId::Name(&input.id.name)).await {
            if let Ok(volume) = client.inputs().volume(obws::requests::inputs::InputId::Name(&input.id.name)).await {
                audio_inputs.push(AudioInputInfo {
                    name: input.id.name.clone(),
                    muted,
                    volume_mul: volume.mul,
                    volume_db: Some(volume.db),
                });
            }
        }
    }
    Ok(audio_inputs)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    // Task to write messages to the WebSocket
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Send initial status if OBS is connected
    {
        let read_guard = state.obs_client.read().await;
        if let Some(client) = &*read_guard {
            let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                "type": "obs_connected"
            })).unwrap().into())).await;

            if let Ok(status) = get_obs_status(client).await {
                let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                    "type": "obs_status",
                    "data": status
                })).unwrap().into())).await;
            }
        } else {
            let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                "type": "obs_disconnected"
            })).unwrap().into())).await;
        }
    }

    // Send existing chat history
    {
        let history = state.chat_history.lock().await;
        if !history.is_empty() {
            let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                "type": "chat_history",
                "messages": *history
            })).unwrap().into())).await;
        }
    }

    // Subscribe to periodic OBS status changes
    let mut status_rx = state.status_tx.subscribe();
    let tx_clone = tx.clone();
    let obs_client_for_status = state.obs_client.clone();
    tokio::spawn(async move {
        let mut last_connected = false;
        loop {
            tokio::select! {
                status_res = status_rx.recv() => {
                    match status_res {
                        Ok(status) => {
                            if !last_connected {
                                last_connected = true;
                                let _ = tx_clone.send(Message::Text(serde_json::to_string(&json!({
                                    "type": "obs_connected"
                                })).unwrap().into())).await;
                            }
                            let _ = tx_clone.send(Message::Text(serde_json::to_string(&json!({
                                "type": "obs_status",
                                "data": status
                            })).unwrap().into())).await;
                        }
                        Err(_) => break,
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    let connected = obs_client_for_status.read().await.is_some();
                    if connected != last_connected {
                        last_connected = connected;
                        let type_str = if connected { "obs_connected" } else { "obs_disconnected" };
                        let _ = tx_clone.send(Message::Text(serde_json::to_string(&json!({
                            "type": type_str
                        })).unwrap().into())).await;
                    }
                }
            }
        }
    });

    // Listen for client WebSocket inputs
    while let Some(Ok(Message::Text(text))) = receiver.next().await {
        if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
            match client_msg {
                ClientMessage::Chat { text, model } => {
                    let chosen_model = model.unwrap_or_else(|| {
                        if std::env::var("ANTHROPIC_API_KEY").is_ok() {
                            "claude".to_string()
                        } else if std::env::var("OPENAI_API_KEY").is_ok() {
                            "openai".to_string()
                        } else {
                            "claude".to_string()
                        }
                    });

                    let obs_client = state.obs_client.clone();
                    let tx_for_agent = tx.clone();
                    let chat_history_clone = state.chat_history.clone();
                    let hyperia_tools_clone = state.hyperia_tools.clone();

                    tokio::spawn(async move {
                        run_agent_with_fallback(
                            text,
                            obs_client,
                            chosen_model,
                            tx_for_agent,
                            chat_history_clone,
                            hyperia_tools_clone,
                        ).await;
                    });
                }
                ClientMessage::Command { action, params } => {
                    let obs_client = state.obs_client.clone();
                    let tx_for_cmd = tx.clone();
                    let reconnect_tx = state.reconnect_tx.clone();
                    let chat_history_clone = state.chat_history.clone();
                    
                    tokio::spawn(async move {
                        if action == "reconnect_obs" {
                            let _ = reconnect_tx.send(());
                            return;
                        }

                        if action == "clear_history" {
                            let mut history = chat_history_clone.lock().await;
                            history.clear();
                            let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                "type": "system_notification",
                                "text": "Chat history cleared."
                            })).unwrap().into())).await;
                            return;
                        }

                        if action == "list_system_windows" {
                            match list_system_windows() {
                                Ok(list) => {
                                    let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                        "type": "system_windows_list",
                                        "windows": list
                                    })).unwrap().into())).await;
                                }
                                Err(e) => {
                                    let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                        "type": "system_notification",
                                        "text": format!("Failed to list system windows: {}", e)
                                    })).unwrap().into())).await;
                                }
                            }
                            return;
                        }

                        if action == "resize_window" {
                            let title = params.get("window_title").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                            let width = params.get("width").and_then(|v| v.as_u64()).unwrap_or(1280) as u32;
                            let height = params.get("height").and_then(|v| v.as_u64()).unwrap_or(720) as u32;
                            let x = params.get("x").and_then(|v| v.as_i64()).map(|v| v as i32);
                            let y = params.get("y").and_then(|v| v.as_i64()).map(|v| v as i32);
                            
                            match resize_and_locate_window(&title, width, height, x, y) {
                                Ok(details) => {
                                    let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                        "type": "window_resized_details",
                                        "details": details
                                    })).unwrap().into())).await;
                                }
                                Err(e) => {
                                    let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                        "type": "system_notification",
                                        "text": format!("Failed to resize window: {}", e)
                                    })).unwrap().into())).await;
                                }
                            }
                            return;
                        }

                        if action == "set_obs_window_capture" {
                            let scene_name = params.get("scene_name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                            let source_name = params.get("source_name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                            let window_identifier = params.get("window_identifier").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                            
                            let read_guard = obs_client.read().await;
                            if let Some(client) = &*read_guard {
                                match execute_tool(client, ObsTool::SetObsWindowCapture { scene_name, source_name, window_identifier }).await {
                                    Ok(res) => {
                                        let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                            "type": "system_notification",
                                            "text": res
                                        })).unwrap().into())).await;
                                        // Instantly fetch status and broadcast
                                        if let Ok(status) = get_obs_status(client).await {
                                            let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                                "type": "obs_status",
                                                "data": status
                                            })).unwrap().into())).await;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                            "type": "system_notification",
                                            "text": format!("Failed to set OBS window capture: {}", e)
                                        })).unwrap().into())).await;
                                    }
                                };
                            } else {
                                let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                    "type": "system_notification",
                                    "text": "OBS is disconnected"
                                })).unwrap().into())).await;
                            }
                            return;
                        }

                        let read_guard = obs_client.read().await;
                        if let Some(client) = &*read_guard {
                            let result = match action.as_str() {
                                "switch_scene" => {
                                    if let Some(scene_name) = params.get("scene_name").and_then(|v| v.as_str()) {
                                        execute_tool(client, ObsTool::SwitchScene { scene_name: scene_name.to_string() }).await
                                    } else {
                                        Err(anyhow!("Missing scene_name"))
                                    }
                                }
                                "set_mute" => {
                                    if let (Some(input_name), Some(muted)) = (
                                        params.get("input_name").and_then(|v| v.as_str()),
                                        params.get("muted").and_then(|v| v.as_bool()),
                                    ) {
                                        execute_tool(client, ObsTool::SetMute { input_name: input_name.to_string(), muted }).await
                                    } else {
                                        Err(anyhow!("Missing input_name or muted"))
                                    }
                                }
                                "set_volume" => {
                                    if let (Some(input_name), Some(volume)) = (
                                        params.get("input_name").and_then(|v| v.as_str()),
                                        params.get("volume").and_then(|v| v.as_f64()),
                                    ) {
                                        execute_tool(client, ObsTool::SetVolume { input_name: input_name.to_string(), volume: volume as f32 }).await
                                    } else {
                                        Err(anyhow!("Missing input_name or volume"))
                                    }
                                }
                                _ => Err(anyhow!("Unknown action: {}", action)),
                            };

                            if let Err(e) = result {
                                let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                    "type": "system_notification",
                                    "text": format!("Command failed: {}", e)
                                })).unwrap().into())).await;
                            } else {
                                // Instantly fetch status and broadcast
                                if let Ok(status) = get_obs_status(client).await {
                                    let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                        "type": "obs_status",
                                        "data": status
                                    })).unwrap().into())).await;
                                }
                            }
                        } else {
                            let _ = tx_for_cmd.send(Message::Text(serde_json::to_string(&json!({
                                "type": "system_notification",
                                "text": "Command failed: OBS is not connected."
                            })).unwrap().into())).await;
                        }
                    });
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SystemWindowInfo {
    pub pid: u32,
    pub process_name: String,
    pub title: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WindowDetails {
    pub title: String,
    pub class_name: String,
    pub process_name: String,
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
    pub obs_identifier: String,
}

pub fn list_system_windows() -> Result<Vec<SystemWindowInfo>> {
    #[cfg(target_os = "windows")]
    {
        let script = r#"
            Get-Process | Where-Object { $_.MainWindowTitle } | ForEach-Object {
                [PSCustomObject]@{
                    pid = $_.Id
                    process_name = $_.ProcessName
                    title = $_.MainWindowTitle
                }
            } | ConvertTo-Json -Compress
        "#;
        
        let output = std::process::Command::new("powershell")
            .args(&["-NoProfile", "-Command", script])
            .output()?;
            
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if stdout.is_empty() {
                return Ok(Vec::new());
            }
            if stdout.starts_with('[') {
                let list: Vec<SystemWindowInfo> = serde_json::from_str(&stdout)?;
                Ok(list)
            } else if stdout.starts_with('{') {
                let item: SystemWindowInfo = serde_json::from_str(&stdout)?;
                Ok(vec![item])
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(anyhow::anyhow!("PowerShell failed: {}", String::from_utf8_lossy(&output.stderr)))
        }
    }
    
    #[cfg(not(target_os = "windows"))]
    {
        let output_res = std::process::Command::new("wmctrl")
            .arg("-lp")
            .output();
            
        match output_res {
            Ok(output) if output.status.success() => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let mut list = Vec::new();
                for line in stdout.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 5 {
                        let pid_parsed = parts[2].parse::<u32>().unwrap_or(0);
                        let title = parts[4..].join(" ");
                        list.push(SystemWindowInfo {
                            pid: pid_parsed,
                            process_name: "X11Window".to_string(),
                            title,
                        });
                    }
                }
                Ok(list)
            }
            _ => {
                Ok(vec![
                    SystemWindowInfo {
                        pid: 1234,
                        process_name: "chrome".to_string(),
                        title: "Google - OBS Frontend Control - Chrome".to_string(),
                    },
                    SystemWindowInfo {
                        pid: 5678,
                        process_name: "cmd".to_string(),
                        title: "Command Prompt - cargo run".to_string(),
                    }
                ])
            }
        }
    }
}

pub fn resize_and_locate_window(
    title_query: &str,
    width: u32,
    height: u32,
    x: Option<i32>,
    y: Option<i32>,
) -> Result<WindowDetails> {
    let target_x = x.unwrap_or(0);
    let target_y = y.unwrap_or(0);

    #[cfg(target_os = "windows")]
    {
        let script = format!(
            r#"
            Add-Type @"
            using System;
            using System.Text;
            using System.Runtime.InteropServices;
            public class Win32 {{
                [DllImport("user32.dll")]
                [return: MarshalAs(UnmanagedType.Bool)]
                public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
                
                [DllImport("user32.dll")]
                [return: MarshalAs(UnmanagedType.Bool)]
                public static extern bool SetWindowPos(IntPtr hWnd, IntPtr hWndInsertAfter, int X, int Y, int cx, int cy, uint uFlags);
                
                [DllImport("user32.dll")]
                [return: MarshalAs(UnmanagedType.Bool)]
                public static extern bool SetForegroundWindow(IntPtr hWnd);

                [DllImport("user32.dll", CharSet = CharSet.Auto)]
                public static extern int GetClassName(IntPtr hWnd, StringBuilder lpClassName, int nMaxCount);
            }}
"@
            $query = $env:HOTWIRE_WINDOW_QUERY
            $proc = Get-Process | Where-Object {{ $_.MainWindowTitle -and ($_.MainWindowTitle.ToLower().Contains($query.ToLower()) -or $_.ProcessName.ToLower() -eq $query.ToLower()) }} | Select-Object -First 1
            if ($proc) {{
                $hwnd = $proc.MainWindowHandle
                [Win32]::ShowWindow($hwnd, 9) | Out-Null
                [Win32]::SetForegroundWindow($hwnd) | Out-Null
                [Win32]::SetWindowPos($hwnd, [IntPtr]::Zero, {}, {}, {}, {}, 0) | Out-Null
                
                $className = New-Object System.Text.StringBuilder 256
                [Win32]::GetClassName($hwnd, $className, $className.MaxCapacity) | Out-Null
                
                $res = @{{
                    title = $proc.MainWindowTitle
                    class_name = $className.ToString()
                    process_name = $proc.ProcessName + ".exe"
                    x = {}
                    y = {}
                    width = {}
                    height = {}
                    obs_identifier = ($proc.MainWindowTitle + ":" + $className.ToString() + ":" + $proc.ProcessName + ".exe")
                }}
                $res | ConvertTo-Json -Compress
            }} else {{
                throw "Window not found matching query: $query"
            }}
            "#,
            target_x, target_y, width, height, target_x, target_y, width, height
        );

        let output = std::process::Command::new("powershell")
            .args(&["-NoProfile", "-Command", &script])
            .env("HOTWIRE_WINDOW_QUERY", title_query)
            .output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let details: WindowDetails = serde_json::from_str(&stdout)?;
            Ok(details)
        } else {
            Err(anyhow::anyhow!("PowerShell failed: {}", String::from_utf8_lossy(&output.stderr)))
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = std::process::Command::new("xdotool")
            .args(&["search", "--name", title_query, "windowactivate", "windowsize", &width.to_string(), &height.to_string(), "windowmove", &target_x.to_string(), &target_y.to_string()])
            .output();
            
        Ok(WindowDetails {
            title: title_query.to_string(),
            class_name: "MockClassName".to_string(),
            process_name: format!("{}.exe", title_query.to_lowercase().replace(' ', "_")),
            x: target_x,
            y: target_y,
            width,
            height,
            obs_identifier: format!("{}:MockClassName:{}.exe", title_query, title_query.to_lowercase().replace(' ', "_")),
        })
    }
}

async fn execute_tool(client: &Client, tool: ObsTool) -> Result<String> {
    match tool {
        ObsTool::ListScenes {} => {
            let res = client.scenes().list().await?;
            let current = res.current_program_scene.as_ref().map(|s| s.name.clone()).unwrap_or_default();
            let scenes: Vec<String> = res.scenes.into_iter().map(|s| s.id.name).collect();
            Ok(format!("Current scene: '{}'. Available scenes: {:?}", current, scenes))
        }
        ObsTool::SwitchScene { scene_name } => {
            client.scenes().set_current_program_scene(&*scene_name).await?;
            Ok(format!("Switched to scene '{}'", scene_name))
        }
        ObsTool::GetAudioInputs {} => {
            let inputs = get_audio_inputs(client).await?;
            let formatted: Vec<String> = inputs.into_iter().map(|i| {
                format!(
                    "Name: '{}', Muted: {}, VolMul: {:.2}, VolDb: {:?}",
                    i.name, i.muted, i.volume_mul, i.volume_db
                )
            }).collect();
            Ok(format!("Audio inputs: {:?}", formatted))
        }
        ObsTool::SetMute { input_name, muted } => {
            client.inputs().set_muted(obws::requests::inputs::InputId::Name(&input_name), muted).await?;
            Ok(format!("Set mute for '{}' to {}", input_name, muted))
        }
        ObsTool::SetVolume { input_name, volume } => {
            client.inputs().set_volume(obws::requests::inputs::InputId::Name(&input_name), obws::requests::inputs::Volume::Mul(volume)).await?;
            Ok(format!("Set volume for '{}' to {:.2} multiplier", input_name, volume))
        }
        ObsTool::GetStreamRecordingStatus {} => {
            let streaming = client.streaming().status().await?.active;
            let recording = client.recording().status().await?.active;
            let virtual_cam = client.virtual_cam().status().await?;
            Ok(format!(
                "Streaming: {}, Recording: {}, Virtual Camera: {}",
                streaming, recording, virtual_cam
            ))
        }
        ObsTool::ControlStream { action } => {
            match action.as_str() {
                "start" => {
                    client.streaming().start().await?;
                    Ok("Started streaming".to_string())
                }
                "stop" => {
                    client.streaming().stop().await?;
                    Ok("Stopped streaming".to_string())
                }
                "toggle" | _ => {
                    let active = client.streaming().toggle().await?;
                    Ok(format!("Toggled streaming. Active: {}", active))
                }
            }
        }
        ObsTool::ControlRecord { action } => {
            match action.as_str() {
                "start" => {
                    client.recording().start().await?;
                    Ok("Started recording".to_string())
                }
                "stop" => {
                    client.recording().stop().await?;
                    Ok("Stopped recording".to_string())
                }
                "toggle" | _ => {
                    let active = client.recording().toggle().await?;
                    Ok(format!("Toggled recording. Active: {}", active))
                }
            }
        }
        ObsTool::ControlVirtualCam { action } => {
            match action.as_str() {
                "start" => {
                    client.virtual_cam().start().await?;
                    Ok("Started virtual camera".to_string())
                }
                "stop" => {
                    client.virtual_cam().stop().await?;
                    Ok("Stopped virtual camera".to_string())
                }
                "toggle" | _ => {
                    let active = client.virtual_cam().toggle().await?;
                    Ok(format!("Toggled virtual camera. Active: {}", active))
                }
            }
        }
        ObsTool::SetSourceVisibility { scene_name, item_name, enabled } => {
            let items = client.scene_items().list(obws::requests::canvases::SceneId::Name(&scene_name)).await?;
            if let Some(item) = items.into_iter().find(|i| i.source_name == item_name) {
                client.scene_items().set_enabled(obws::requests::scene_items::SetEnabled {
                    scene: obws::requests::canvases::SceneId::Name(&scene_name),
                    item_id: item.id,
                    enabled,
                }).await?;
                Ok(format!("Set visibility of '{}' in scene '{}' to {}", item_name, scene_name, enabled))
            } else {
                Err(anyhow!("Source '{}' not found in scene '{}'", item_name, scene_name))
            }
        }
        ObsTool::CreateScene { scene_name } => {
            client.scenes().create(&scene_name).await?;
            Ok(format!("Created scene '{}'", scene_name))
        }
        ObsTool::RemoveScene { scene_name } => {
            client.scenes().remove(obws::requests::canvases::SceneId::Name(&scene_name)).await?;
            Ok(format!("Removed scene '{}'", scene_name))
        }
        ObsTool::CreateInput { scene_name, input_name, input_kind, enabled } => {
            client.inputs().create(obws::requests::inputs::Create {
                scene: obws::requests::canvases::SceneId::Name(&scene_name),
                input: &input_name,
                kind: &input_kind,
                settings: None::<()>,
                enabled,
            }).await?;
            Ok(format!("Created input '{}' of kind '{}' in scene '{}'", input_name, input_kind, scene_name))
        }
        ObsTool::RemoveInput { input_name } => {
            client.inputs().remove(obws::requests::inputs::InputId::Name(&input_name)).await?;
            Ok(format!("Removed input '{}'", input_name))
        }
        ObsTool::ListSystemWindows {} => {
            let list = list_system_windows()?;
            let text = serde_json::to_string_pretty(&list)?;
            Ok(text)
        }
        ObsTool::ResizeAndFocusWindow { window_title, width, height, x, y } => {
            let details = resize_and_locate_window(&window_title, width, height, x, y)?;
            let text = serde_json::to_string_pretty(&details)?;
            Ok(text)
        }
        ObsTool::SetObsWindowCapture { scene_name, source_name, window_identifier } => {
            let exists = {
                if let Ok(inputs) = client.inputs().list(None).await {
                    inputs.into_iter().any(|i| i.id.name == source_name)
                } else {
                    false
                }
            };
            
            // 1. If it doesn't exist, create it with dummy settings first
            if !exists {
                let temp_settings = json!({
                    "window": "",
                    "method": 2,
                    "window_capture_method": 2
                });
                client.inputs().create(obws::requests::inputs::Create {
                    scene: obws::requests::canvases::SceneId::Name(&scene_name),
                    input: &source_name,
                    kind: "window_capture",
                    settings: Some(&temp_settings),
                    enabled: Some(true),
                }).await?;
            }

            // 2. Query available windows from OBS to resolve the identifier to the exact string in OBS's list
            let mut resolved_identifier = window_identifier.clone();
            if let Ok(items) = client.inputs().properties_list_property_items(
                obws::requests::inputs::InputId::Name(&source_name),
                "window"
            ).await {
                // Parse the query identifier, e.g. "Title:Class:Process.exe"
                let parts: Vec<&str> = window_identifier.split(':').collect();
                if parts.len() >= 3 {
                    let title_part = parts[0].to_lowercase();
                    let class_part = parts[1].to_lowercase();
                    let process_part = parts[2].to_lowercase();
                    
                    let mut best_match = None;
                    let mut best_score = 0;
                    
                    for item in &items {
                        if let Some(val_str) = item.value.as_str() {
                            let val_lower = val_str.to_lowercase();
                            let mut score = 0;
                            
                            // Match process name (strong weight)
                            if val_lower.contains(&process_part) {
                                score += 10;
                            }
                            // Match class name
                            if val_lower.contains(&class_part) {
                                score += 5;
                            }
                            // Match title
                            if val_lower.contains(&title_part) {
                                score += 3;
                            }
                            
                            if score > best_score {
                                best_score = score;
                                best_match = Some(val_str.to_string());
                            }
                        }
                    }
                    
                    if let Some(matched) = best_match {
                        resolved_identifier = matched;
                    }
                } else {
                    // Fallback to simple matching if the format is not "Title:Class:Process.exe"
                    let query = window_identifier.to_lowercase();
                    for item in &items {
                        if let Some(val_str) = item.value.as_str() {
                            if val_str.to_lowercase().contains(&query) {
                                resolved_identifier = val_str.to_string();
                                break;
                            }
                        }
                    }
                }
            }

            // 3. Set the resolved identifier and capture method settings (using both "method" and "window_capture_method" for WGC/Windows 10)
            let settings = json!({
                "window": resolved_identifier,
                "method": 2,
                "window_capture_method": 2
            });

            client.inputs().set_settings(obws::requests::inputs::SetSettings {
                input: obws::requests::inputs::InputId::Name(&source_name),
                settings: &settings,
                overlay: Some(true),
            }).await?;

            Ok(format!("Window capture source '{}' bound to window '{}' in scene '{}'", source_name, resolved_identifier, scene_name))
        }
        ObsTool::ListTransitions {} => {
            let list = client.transitions().list().await?;
            let current = client.transitions().current().await?;
            let current_duration = current.duration.map(|d| d.whole_milliseconds()).unwrap_or(0);
            
            let transitions: Vec<serde_json::Value> = list.transitions.into_iter().map(|t| {
                json!({
                    "name": t.id.name,
                    "kind": t.kind,
                    "fixed": t.fixed,
                    "configurable": t.configurable,
                })
            }).collect();
            
            let res = json!({
                "transitions": transitions,
                "current_transition": current.id.name,
                "current_kind": current.kind,
                "current_duration_ms": current_duration,
            });
            Ok(serde_json::to_string_pretty(&res)?)
        }
        ObsTool::SetTransition { transition_name } => {
            client.transitions().set_current(&transition_name).await?;
            Ok(format!("Successfully set scene transition to '{}'", transition_name))
        }
        ObsTool::SetTransitionDuration { duration_ms } => {
            let dur = time::Duration::milliseconds(duration_ms as i64);
            client.transitions().set_current_duration(dur).await?;
            Ok(format!("Successfully set current scene transition duration to {}ms", duration_ms))
        }
    }
}

async fn run_agent_loop(
    user_message: String,
    obs_client: Arc<RwLock<Option<Client>>>,
    anthropic_key: String,
    websocket_sender: mpsc::Sender<Message>,
    chat_history: Arc<Mutex<Vec<ChatMessage>>>,
    hyperia_tools: Arc<RwLock<Vec<ClaudeTool>>>,
) -> Result<()> {
    let client = reqwest::Client::new();
    
    // Push user message to history
    {
        let mut history_guard = chat_history.lock().await;
        history_guard.push(ChatMessage {
            role: "user".to_string(),
            text: user_message.clone(),
        });
    }

    let mut local_history = Vec::<ClaudeMessage>::new();
    {
        let history_guard = chat_history.lock().await;
        for msg in history_guard.iter() {
            if msg.role == "user" {
                local_history.push(ClaudeMessage {
                    role: "user".to_string(),
                    content: ClaudeContent::Text(msg.text.clone()),
                });
            } else {
                local_history.push(ClaudeMessage {
                    role: "assistant".to_string(),
                    content: ClaudeContent::Text(msg.text.clone()),
                });
            }
        }
    }

    let count = MESSAGE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let message_id = format!("agent-{}", count);
    
    // Notify frontend that agent message started
    let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
        "type": "agent_message_start",
        "message_id": message_id
    }))?.into())).await;

    let mut loop_count = 0;
    const MAX_LOOPS: usize = 10;
    let mut accumulated_response = String::new();

    while loop_count < MAX_LOOPS {
        loop_count += 1;

        // Get current OBS state to feed to the system prompt
        let current_state_str = {
            let read_guard = obs_client.read().await;
            if let Some(client) = &*read_guard {
                match get_obs_status(client).await {
                    Ok(status) => serde_json::to_string_pretty(&status).unwrap_or_default(),
                    Err(e) => format!("Error getting OBS status: {}", e),
                }
            } else {
                "OBS is currently disconnected.".to_string()
            }
        };

        let system_prompt = format!(
            "You are OBSy, an agentic controller for OBS Studio. \
             You have access to tools to control OBS in real-time. \
             Current OBS Studio Status:\n```json\n{}\n```\n\n\
             CRITICAL RULE: You MUST always conclude your response with a voice summary wrapped in `<voice_summary>...</voice_summary>` tags. This text will be spoken to the user. E.g. `<voice_summary>I've switched to the BRB scene and muted the microphone.</voice_summary>`. Do not forget this tag under any circumstances.\n\n\
             Your task is to fulfill the user's request by calling the appropriate tools sequentially. \
             Follow these rules:\n\
             1. Analyze the request and check the current OBS status.\n\
             2. Call tools to modify OBS state if needed (e.g., switch scene, mute, adjust volume, start/stop recording, list/set transitions).\n\
             3. **Hotwiring & Window Capture (Docker/Terminals/etc.)**:\n\
                - If the user asks to capture, show, resize, or bind a window (such as a Docker terminal, browser, or any other app),\n\
                  you must first call `list_system_windows` to find its title/process name.\n\
                  - Once found, call `resize_and_focus_window` with the target title and dimensions (e.g., matching the canvas resolution like 2560x1440) to focus and resize it.\n\
                  - This returns an `obs_identifier` (e.g. 'Title:ClassName:ProcessName.exe').\n\
                  - Then, call `set_obs_window_capture` to create or bind that window to a Window Capture source in the desired scene.\n\
             4. **Mandatory Voice Summary (ElevenLabs)**:\n\
                - At the end of your response, you MUST include a short, conversational summary (1-2 sentences) of the actions you took or the answers you provided, wrapped in `<voice_summary>...</voice_summary>` tags. This is mandatory for every response.\n\
                - E.g., `<voice_summary>I have switched OBS to the Be Right Back scene and muted the microphone.</voice_summary>`.\n\
             5. You can enable or disable voice listening mode dynamically by calling the `set_listening_state` tool (e.g. if the user says 'listen to me' or 'stop listening').\n\
             6. If a tool fails, report it and try another way or explain the failure.\n\
             7. Once you have achieved the user's goal, or if you need to ask a question, output your final response to the user.\n\
             8. Keep your final response concise and directly answer the user.",
            current_state_str
        );

        // Notify trace: thinking
        let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
            "type": "agent_trace_step",
            "message_id": message_id,
            "step_type": "thinking",
            "content": "Analyzing current OBS state and determining next steps..."
        }))?.into())).await;

        let messages_payload = local_history.clone();

        let mut tools = get_claude_tools();
        {
            let h_tools = hyperia_tools.read().await;
            tools.extend(h_tools.clone());
        }

        let request = ClaudeMessageRequest {
            model: "claude-3-5-sonnet-20241022".to_string(),
            max_tokens: 1024,
            system: system_prompt,
            messages: messages_payload,
            tools,
        };

        // Call Claude API
        let response = client.post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &anthropic_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let err_text = response.text().await?;
            let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                "type": "agent_trace_step",
                "message_id": message_id,
                "step_type": "error",
                "content": format!("Claude API error: {}", err_text)
            }))?.into())).await;
            break;
        }

        let claude_resp: ClaudeMessageResponse = response.json().await?;

        // Process Claude response content
        let mut text_response = String::new();
        let mut tool_calls = Vec::new();

        for block in &claude_resp.content {
            match block {
                ClaudeBlock::Text { text } => {
                    text_response.push_str(text);
                    accumulated_response.push_str(text);
                    // Stream text chunk to user
                    let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                        "type": "agent_message_chunk",
                        "message_id": message_id,
                        "text": text
                    }))?.into())).await;
                }
                ClaudeBlock::ToolUse { id, name, input } => {
                    tool_calls.push((id.clone(), name.clone(), input.clone()));
                }
                _ => {}
            }
        }

        // Append assistant response to local history
        local_history.push(ClaudeMessage {
            role: "assistant".to_string(),
            content: ClaudeContent::Blocks(claude_resp.content.clone()),
        });

        if tool_calls.is_empty() {
            break;
        }

        // Execute tools and collect results
        let mut tool_results = Vec::new();
        for (tool_use_id, tool_name, tool_input) in tool_calls {
            let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                "type": "agent_trace_step",
                "message_id": message_id,
                "step_type": "tool-call",
                "content": format!("Calling tool `{}` with input: {}", tool_name, tool_input)
            }))?.into())).await;

            let is_listening_tool = tool_name == "set_listening_state";
            let is_hyperia = {
                let h_tools = hyperia_tools.read().await;
                h_tools.iter().any(|t| t.name == tool_name)
            };

            let result_str = if is_listening_tool {
                let enabled = tool_input.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                let msg = json!({
                    "type": "set_listening",
                    "enabled": enabled
                });
                let _ = websocket_sender.send(Message::Text(serde_json::to_string(&msg).unwrap().into())).await;
                format!("Success: Voice transcription listening state set to {}", enabled)
            } else if is_hyperia {
                match execute_hyperia_tool(&tool_name, &tool_input).await {
                    Ok(res) => format!("Success: {}", res),
                    Err(e) => format!("Error executing Hyperia tool: {}", e),
                }
            } else {
                match serde_json::from_value::<ObsTool>(json!({
                    "name": tool_name,
                    "input": tool_input
                })) {
                    Ok(tool) => {
                        let read_guard = obs_client.read().await;
                        if let Some(ref client) = *read_guard {
                            match execute_tool(client, tool).await {
                                Ok(res) => format!("Success: {}", res),
                                Err(e) => format!("Error executing tool: {}", e),
                            }
                        } else {
                            "Error: OBS is disconnected".to_string()
                        }
                    }
                    Err(e) => format!("Error parsing tool call: {}", e),
                }
            };

            let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                "type": "agent_trace_step",
                "message_id": message_id,
                "step_type": "tool-result",
                "content": format!("Tool result: {}", result_str)
            }))?.into())).await;

            tool_results.push(ClaudeBlock::ToolResult {
                tool_use_id,
                content: result_str,
            });
        }

        // Append tool results to local history for next turn
        local_history.push(ClaudeMessage {
            role: "user".to_string(),
            content: ClaudeContent::Blocks(tool_results),
        });
    }

    // Trigger ElevenLabs Voice Summary
    handle_voice_summary(&accumulated_response, &websocket_sender).await;

    // Push final accumulated response to global history
    if !accumulated_response.is_empty() {
        let mut history_guard = chat_history.lock().await;
        history_guard.push(ChatMessage {
            role: "assistant".to_string(),
            text: accumulated_response,
        });
    }

    // Finalize message
    let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
        "type": "agent_message_end",
        "message_id": message_id
    }))?.into())).await;

    Ok(())
}

fn get_claude_tools() -> Vec<ClaudeTool> {
    vec![
        ClaudeTool {
            name: "list_scenes".to_string(),
            description: "List all scenes available in OBS and find which scene is currently active.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ClaudeTool {
            name: "switch_scene".to_string(),
            description: "Switch the active program scene to the specified scene name.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scene_name": {
                        "type": "string",
                        "description": "The name of the scene to switch to."
                    }
                },
                "required": ["scene_name"]
            }),
        },
        ClaudeTool {
            name: "get_audio_inputs".to_string(),
            description: "Get a list of all audio inputs (sources) in OBS along with their volume levels and mute status.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ClaudeTool {
            name: "set_mute".to_string(),
            description: "Mute or unmute a specific audio input (source).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "input_name": {
                        "type": "string",
                        "description": "The name of the audio input to mute/unmute."
                    },
                    "muted": {
                        "type": "boolean",
                        "description": "True to mute, False to unmute."
                    }
                },
                "required": ["input_name", "muted"]
            }),
        },
        ClaudeTool {
            name: "set_volume".to_string(),
            description: "Set the volume level of a specific audio input (source) using a multiplier between 0.0 (silent) and 1.0 (100% volume).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "input_name": {
                        "type": "string",
                        "description": "The name of the audio input to adjust."
                    },
                    "volume": {
                        "type": "number",
                        "description": "The volume multiplier (from 0.0 to 1.0)."
                    }
                },
                "required": ["input_name", "volume"]
            }),
        },
        ClaudeTool {
            name: "get_stream_recording_status".to_string(),
            description: "Get the current streaming, recording, and virtual camera active status.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ClaudeTool {
            name: "control_stream".to_string(),
            description: "Start, stop, or toggle the stream output.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["start", "stop", "toggle"],
                        "description": "The action to perform."
                    }
                },
                "required": ["action"]
            }),
        },
        ClaudeTool {
            name: "control_record".to_string(),
            description: "Start, stop, or toggle local video recording.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["start", "stop", "toggle"],
                        "description": "The action to perform."
                    }
                },
                "required": ["action"]
            }),
        },
        ClaudeTool {
            name: "control_virtualcam".to_string(),
            description: "Start, stop, or toggle the virtual camera output.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["start", "stop", "toggle"],
                        "description": "The action to perform."
                    }
                },
                "required": ["action"]
            }),
        },
        ClaudeTool {
            name: "set_source_visibility".to_string(),
            description: "Enable (show) or disable (hide) a source item inside a specific scene.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scene_name": {
                        "type": "string",
                        "description": "The name of the scene containing the source."
                    },
                    "item_name": {
                        "type": "string",
                        "description": "The name of the source (scene item) to show or hide."
                    },
                    "enabled": {
                        "type": "boolean",
                        "description": "True to show (enable), False to hide (disable)."
                    }
                },
                "required": ["scene_name", "item_name", "enabled"]
            }),
        },
        ClaudeTool {
            name: "create_scene".to_string(),
            description: "Create a new scene in OBS with the specified name.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scene_name": {
                        "type": "string",
                        "description": "The name of the new scene to create."
                    }
                },
                "required": ["scene_name"]
            }),
        },
        ClaudeTool {
            name: "remove_scene".to_string(),
            description: "Delete/remove an existing scene from OBS by its name.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scene_name": {
                        "type": "string",
                        "description": "The name of the scene to remove."
                    }
                },
                "required": ["scene_name"]
            }),
        },
        ClaudeTool {
            name: "create_input".to_string(),
            description: "Create a new input source (e.g. image_source, ffmpeg_source, text_ft2_source_v2) inside a specified scene.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scene_name": {
                        "type": "string",
                        "description": "The name of the scene to create the input in."
                    },
                    "input_name": {
                        "type": "string",
                        "description": "The name of the new input source."
                    },
                    "input_kind": {
                        "type": "string",
                        "description": "The type of input (e.g., 'image_source', 'ffmpeg_source', 'text_ft2_source_v2', 'wasapi_input_capture')."
                    },
                    "enabled": {
                        "type": "boolean",
                        "description": "Whether the source should be initially enabled/visible. Defaults to true."
                    }
                },
                "required": ["scene_name", "input_name", "input_kind"]
            }),
        },
        ClaudeTool {
            name: "remove_input".to_string(),
            description: "Remove/delete an existing input source from OBS.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "input_name": {
                        "type": "string",
                        "description": "The name of the input source to remove."
                    }
                },
                "required": ["input_name"]
            }),
        },
        ClaudeTool {
            name: "list_system_windows".to_string(),
            description: "List all open system windows on the host OS. Use this to find window titles and processes that are currently running.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ClaudeTool {
            name: "resize_and_focus_window".to_string(),
            description: "Locate an open system window by title, unminimize/restore it, bring it to focus, and resize/reposition it. Returns its details including the OBS window capture identifier string.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "window_title": {
                        "type": "string",
                        "description": "A substring matching the title of the window to find."
                    },
                    "width": {
                        "type": "integer",
                        "description": "Target width in pixels."
                    },
                    "height": {
                        "type": "integer",
                        "description": "Target height in pixels."
                    },
                    "x": {
                        "type": "integer",
                        "description": "Optional X coordinate for window position."
                    },
                    "y": {
                        "type": "integer",
                        "description": "Optional Y coordinate for window position."
                    }
                },
                "required": ["window_title", "width", "height"]
            }),
        },
        ClaudeTool {
            name: "set_obs_window_capture".to_string(),
            description: "Create or update a Window Capture input source in a specified scene in OBS to target a specific window identifier.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scene_name": {
                        "type": "string",
                        "description": "The name of the scene."
                    },
                    "source_name": {
                        "type": "string",
                        "description": "The name of the Window Capture source."
                    },
                    "window_identifier": {
                        "type": "string",
                        "description": "The OBS window capture identifier string (e.g. 'Title:ClassName:Process.exe')."
                    }
                },
                "required": ["scene_name", "source_name", "window_identifier"]
            }),
        },
        ClaudeTool {
            name: "list_transitions".to_string(),
            description: "List all scene transitions available in OBS, including the current active transition name, kind, and duration.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ClaudeTool {
            name: "set_transition".to_string(),
            description: "Set the active scene transition to the specified transition name (e.g., 'Fade', 'Cut').".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "transition_name": {
                        "type": "string",
                        "description": "The name of the transition to activate."
                    }
                },
                "required": ["transition_name"]
            }),
        },
        ClaudeTool {
            name: "set_transition_duration".to_string(),
            description: "Set the duration of the current scene transition in milliseconds (e.g. 300).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "duration_ms": {
                        "type": "integer",
                        "description": "Transition duration in milliseconds."
                    }
                },
                "required": ["duration_ms"]
            }),
        },
        ClaudeTool {
            name: "set_listening_state".to_string(),
            description: "Start or stop always-listening microphone mode for realtime voice control.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "enabled": {
                        "type": "boolean",
                        "description": "Whether voice listening/mic transcription should be active."
                    }
                },
                "required": ["enabled"]
            }),
        },
    ]
}

// ==========================================
// OpenAI Structures and Agent Loop
// ==========================================

#[derive(Serialize)]
struct OpenAiChatRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OpenAiTool>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct OpenAiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OpenAiToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct OpenAiToolCall {
    id: String,
    #[serde(rename = "type")]
    r#type: String,
    function: OpenAiFunctionCall,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct OpenAiFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct OpenAiTool {
    #[serde(rename = "type")]
    r#type: String,
    function: OpenAiFunctionDefinition,
}

#[derive(Serialize)]
struct OpenAiFunctionDefinition {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Deserialize, Debug)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize, Debug)]
struct OpenAiChoice {
    message: OpenAiResponseMessage,
}

#[derive(Deserialize, Debug)]
struct OpenAiResponseMessage {
    role: String,
    content: Option<String>,
    tool_calls: Option<Vec<OpenAiToolCall>>,
}



async fn run_agent_loop_openai(
    user_message: String,
    obs_client: Arc<RwLock<Option<Client>>>,
    openai_key: String,
    websocket_sender: mpsc::Sender<Message>,
    chat_history: Arc<Mutex<Vec<ChatMessage>>>,
    hyperia_tools: Arc<RwLock<Vec<ClaudeTool>>>,
) -> Result<()> {
    let client = reqwest::Client::new();
    
    // Push user message to history
    {
        let mut history_guard = chat_history.lock().await;
        history_guard.push(ChatMessage {
            role: "user".to_string(),
            text: user_message.clone(),
        });
    }

    let mut local_history = Vec::<OpenAiMessage>::new();
    {
        let history_guard = chat_history.lock().await;
        for msg in history_guard.iter() {
            local_history.push(OpenAiMessage {
                role: msg.role.clone(),
                content: Some(msg.text.clone()),
                tool_calls: None,
                tool_call_id: None,
            });
        }
    }

    let count = MESSAGE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let message_id = format!("agent-{}", count);
    
    // Notify frontend that agent message started
    let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
        "type": "agent_message_start",
        "message_id": message_id
    }))?.into())).await;

    let mut loop_count = 0;
    const MAX_LOOPS: usize = 10;
    let mut accumulated_response = String::new();

    while loop_count < MAX_LOOPS {
        loop_count += 1;

        // Get current OBS state to feed to the system prompt
        let current_state_str = {
            let read_guard = obs_client.read().await;
            if let Some(client) = &*read_guard {
                match get_obs_status(client).await {
                    Ok(status) => serde_json::to_string_pretty(&status).unwrap_or_default(),
                    Err(e) => format!("Error getting OBS status: {}", e),
                }
            } else {
                "OBS is currently disconnected.".to_string()
            }
        };

        let system_prompt = format!(
            "You are OBSy, an agentic controller for OBS Studio. \
             You have access to tools to control OBS in real-time. \
             Current OBS Studio Status:\n```json\n{}\n```\n\n\
             CRITICAL RULE: You MUST always conclude your response with a voice summary wrapped in `<voice_summary>...</voice_summary>` tags. This text will be spoken to the user. E.g. `<voice_summary>I've switched to the BRB scene and muted the microphone.</voice_summary>`. Do not forget this tag under any circumstances.\n\n\
             Your task is to fulfill the user's request by calling the appropriate tools sequentially. \
             Follow these rules:\n\
             1. Analyze the request and check the current OBS status.\n\
             2. Call tools to modify OBS state if needed (e.g., switch scene, mute, adjust volume, start/stop recording, list/set transitions).\n\
             3. **Hotwiring & Window Capture (Docker/Terminals/etc.)**:\n\
                - If the user asks to capture, show, resize, or bind a window (such as a Docker terminal, browser, or any other app),\n\
                  you must first call `list_system_windows` to find its title/process name.\n\
                  - Once found, call `resize_and_focus_window` with the target title and dimensions (e.g., matching the canvas resolution like 2560x1440) to focus and resize it.\n\
                  - This returns an `obs_identifier` (e.g. 'Title:ClassName:ProcessName.exe').\n\
                  - Then, call `set_obs_window_capture` to create or bind that window to a Window Capture source in the desired scene.\n\
             4. **Mandatory Voice Summary (ElevenLabs)**:\n\
                - At the end of your response, you MUST include a short, conversational summary (1-2 sentences) of the actions you took or the answers you provided, wrapped in `<voice_summary>...</voice_summary>` tags. This is mandatory for every response.\n\
                - E.g., `<voice_summary>I have switched OBS to the Be Right Back scene and muted the microphone.</voice_summary>`.\n\
             5. You can enable or disable voice listening mode dynamically by calling the `set_listening_state` tool (e.g. if the user says 'listen to me' or 'stop listening').\n\
             6. If a tool fails, report it and try another way or explain the failure.\n\
             7. Once you have achieved the user's goal, or if you need to ask a question, output your final response to the user.\n\
             8. Keep your final response concise and directly answer the user.",
            current_state_str
        );

        // Notify trace: thinking
        let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
            "type": "agent_trace_step",
            "message_id": message_id,
            "step_type": "thinking",
            "content": "Analyzing current OBS state and determining next steps..."
        }))?.into())).await;

        // Combine system prompt and user messages
        let mut messages = vec![
            OpenAiMessage {
                role: "system".to_string(),
                content: Some(system_prompt),
                tool_calls: None,
                tool_call_id: None,
            }
        ];
        messages.extend(local_history.clone());

        let mut tools = get_claude_tools();
        {
            let h_tools = hyperia_tools.read().await;
            tools.extend(h_tools.clone());
        }
        let openai_tools: Vec<OpenAiTool> = tools.into_iter().map(|t| OpenAiTool {
            r#type: "function".to_string(),
            function: OpenAiFunctionDefinition {
                name: t.name,
                description: t.description,
                parameters: t.input_schema,
            },
        }).collect();

        let request = OpenAiChatRequest {
            model: "gpt-4o".to_string(),
            messages,
            tools: Some(openai_tools),
        };

        // Call OpenAI API
        let response = client.post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", openai_key))
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let err_text = response.text().await?;
            let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                "type": "agent_trace_step",
                "message_id": message_id,
                "step_type": "error",
                "content": format!("OpenAI API error: {}", err_text)
            }))?.into())).await;
            break;
        }

        let openai_resp: OpenAiChatResponse = response.json().await?;
        let choice = openai_resp.choices.get(0).ok_or_else(|| anyhow!("No choices returned from OpenAI"))?;
        let resp_msg = &choice.message;

        if let Some(ref text) = resp_msg.content {
            accumulated_response.push_str(text);
            // Stream text chunk to user
            let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                "type": "agent_message_chunk",
                "message_id": message_id,
                "text": text
            }))?.into())).await;
        }

        // Add assistant message to local history
        local_history.push(OpenAiMessage {
            role: "assistant".to_string(),
            content: resp_msg.content.clone(),
            tool_calls: resp_msg.tool_calls.clone(),
            tool_call_id: None,
        });

        let tool_calls = match &resp_msg.tool_calls {
            Some(calls) if !calls.is_empty() => calls,
            _ => {
                break;
            }
        };

        // Execute tools and collect results
        for tool_call in tool_calls {
            let tool_name = &tool_call.function.name;
            let tool_args_str = &tool_call.function.arguments;

            let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                "type": "agent_trace_step",
                "message_id": message_id,
                "step_type": "tool-call",
                "content": format!("Calling tool `{}` with input: {}", tool_name, tool_args_str)
            }))?.into())).await;

            let args_value: serde_json::Value = serde_json::from_str(tool_args_str).unwrap_or(json!({}));

            let is_listening_tool = tool_name == "set_listening_state";
            let is_hyperia = {
                let h_tools = hyperia_tools.read().await;
                h_tools.iter().any(|t| t.name == *tool_name)
            };

            let result_str = if is_listening_tool {
                let enabled = args_value.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                let msg = json!({
                    "type": "set_listening",
                    "enabled": enabled
                });
                let _ = websocket_sender.send(Message::Text(serde_json::to_string(&msg).unwrap().into())).await;
                format!("Success: Voice transcription listening state set to {}", enabled)
            } else if is_hyperia {
                match execute_hyperia_tool(tool_name, &args_value).await {
                    Ok(res) => format!("Success: {}", res),
                    Err(e) => format!("Error executing Hyperia tool: {}", e),
                }
            } else {
                match serde_json::from_value::<ObsTool>(json!({
                    "name": tool_name,
                    "input": args_value
                })) {
                    Ok(tool) => {
                        let read_guard = obs_client.read().await;
                        if let Some(ref client) = *read_guard {
                            match execute_tool(client, tool).await {
                                Ok(res) => format!("Success: {}", res),
                                Err(e) => format!("Error executing tool: {}", e),
                            }
                        } else {
                            "Error: OBS is disconnected".to_string()
                        }
                    }
                    Err(e) => format!("Error parsing tool call: {}", e),
                }
            };

            let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                "type": "agent_trace_step",
                "message_id": message_id,
                "step_type": "tool-result",
                "content": format!("Tool result: {}", result_str)
            }))?.into())).await;

            local_history.push(OpenAiMessage {
                role: "tool".to_string(),
                content: Some(result_str),
                tool_calls: None,
                tool_call_id: Some(tool_call.id.clone()),
            });
        }
    }

    // Trigger ElevenLabs Voice Summary
    handle_voice_summary(&accumulated_response, &websocket_sender).await;

    // Push final accumulated response to global history
    if !accumulated_response.is_empty() {
        let mut history_guard = chat_history.lock().await;
        history_guard.push(ChatMessage {
            role: "assistant".to_string(),
            text: accumulated_response,
        });
    }

    // Finalize message
    let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
        "type": "agent_message_end",
        "message_id": message_id
    }))?.into())).await;

    Ok(())
}

async fn fetch_hyperia_tools(hyperia_tools: Arc<RwLock<Vec<ClaudeTool>>>) {
    println!("[Hyperia] Fetching tools list...");
    match tokio::process::Command::new("python3")
        .args(&["hyperia_helper.py", "list"])
        .output()
        .await
    {
        Ok(output) => {
            if output.status.success() {
                let stdout_str = String::from_utf8_lossy(&output.stdout);
                match serde_json::from_str::<Vec<ClaudeTool>>(&stdout_str) {
                    Ok(tools) => {
                        println!("[Hyperia] Successfully loaded {} tools.", tools.len());
                        let mut guard = hyperia_tools.write().await;
                        *guard = tools;
                    }
                    Err(e) => {
                        eprintln!("[Hyperia] Error deserializing tools list: {}", e);
                    }
                }
            } else {
                let stderr_str = String::from_utf8_lossy(&output.stderr);
                eprintln!("[Hyperia] Helper script exited with error: {}", stderr_str);
            }
        }
        Err(e) => {
            eprintln!("[Hyperia] Failed to run hyperia_helper.py: {}", e);
        }
    }
}

async fn execute_hyperia_tool(name: &str, args: &serde_json::Value) -> Result<String> {
    let args_str = serde_json::to_string(args)?;
    let output = tokio::process::Command::new("python3")
        .args(&["hyperia_helper.py", "call", name, &args_str])
        .output()
        .await?;
    
    if output.status.success() {
        let stdout_str = String::from_utf8_lossy(&output.stdout).into_owned();
        Ok(stdout_str)
    } else {
        let stderr_str = String::from_utf8_lossy(&output.stderr).into_owned();
        let stdout_str = String::from_utf8_lossy(&output.stdout).into_owned();
        anyhow::bail!("Hyperia tool call failed.\nStderr: {}\nStdout: {}", stderr_str, stdout_str)
    }
}

async fn transcribe_handler(
    mut multipart: axum::extract::Multipart,
) -> impl axum::response::IntoResponse {
    let openai_key = match std::env::var("OPENAI_API_KEY") {
        Ok(k) => k,
        Err(_) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "OPENAI_API_KEY is not set on the server.").into_response(),
    };

    let mut audio_bytes = None;
    let mut file_name = "audio.webm".to_string();

    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or_default().to_string();
        if name == "file" {
            if let Some(f_name) = field.file_name() {
                file_name = f_name.to_string();
            }
            if let Ok(bytes) = field.bytes().await {
                audio_bytes = Some(bytes);
            }
            break;
        }
    }

    let bytes = match audio_bytes {
        Some(b) => b,
        None => return (axum::http::StatusCode::BAD_REQUEST, "No audio file uploaded.").into_response(),
    };

    let client = reqwest::Client::new();
    let part = reqwest::multipart::Part::bytes(bytes.to_vec())
        .file_name(file_name)
        .mime_str("audio/webm").unwrap();

    let form = reqwest::multipart::Form::new()
        .text("model", "whisper-1")
        .part("file", part);

    match client.post("https://api.openai.com/v1/audio/transcriptions")
        .header("Authorization", format!("Bearer {}", openai_key))
        .multipart(form)
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                #[derive(serde::Deserialize)]
                struct WhisperResponse {
                    text: String,
                }
                if let Ok(whisper_res) = response.json::<WhisperResponse>().await {
                    (axum::http::StatusCode::OK, axum::Json(json!({ "text": whisper_res.text }))).into_response()
                } else {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Failed to parse Whisper response.").into_response()
                }
            } else {
                let err_text = response.text().await.unwrap_or_default();
                (axum::http::StatusCode::BAD_GATEWAY, format!("OpenAI Whisper error: {}", err_text)).into_response()
            }
        }
        Err(e) => {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to connect to OpenAI: {}", e)).into_response()
        }
    }
}

async fn run_agent_with_fallback(
    text: String,
    obs_client: Arc<RwLock<Option<Client>>>,
    model_preference: String,
    tx: mpsc::Sender<Message>,
    chat_history: Arc<Mutex<Vec<ChatMessage>>>,
    hyperia_tools: Arc<RwLock<Vec<ClaudeTool>>>,
) {
    let anthropic_key = std::env::var("ANTHROPIC_API_KEY").ok();
    let openai_key = std::env::var("OPENAI_API_KEY").ok();

    if anthropic_key.is_none() && openai_key.is_none() {
        let _ = tx.send(Message::Text(serde_json::to_string(&json!({
            "type": "system_notification",
            "text": "Error: Neither ANTHROPIC_API_KEY nor OPENAI_API_KEY is set on the server."
        })).unwrap().into())).await;
        return;
    }

    let (primary_model, secondary_model) = if model_preference == "openai" {
        ("openai", "claude")
    } else {
        ("claude", "openai")
    };

    let try_model = |model: &str| {
        match model {
            "claude" => anthropic_key.is_some(),
            "openai" => openai_key.is_some(),
            _ => false,
        }
    };

    let mut current_model = if try_model(primary_model) {
        primary_model
    } else {
        secondary_model
    };

    if current_model != primary_model {
        let _ = tx.send(Message::Text(serde_json::to_string(&json!({
            "type": "model_switched",
            "model": current_model
        })).unwrap().into())).await;
    }

    let mut attempts = 0;
    while attempts < 2 {
        attempts += 1;
        
        let result = match current_model {
            "claude" => {
                if let Some(key) = &anthropic_key {
                    let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                        "type": "system_notification",
                        "text": "Running agent with Claude..."
                    })).unwrap().into())).await;
                    run_agent_loop(text.clone(), obs_client.clone(), key.clone(), tx.clone(), chat_history.clone(), hyperia_tools.clone()).await
                } else {
                    Err(anyhow::anyhow!("Anthropic key is not available"))
                }
            }
            "openai" => {
                if let Some(key) = &openai_key {
                    let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                        "type": "system_notification",
                        "text": "Running agent with OpenAI..."
                    })).unwrap().into())).await;
                    run_agent_loop_openai(text.clone(), obs_client.clone(), key.clone(), tx.clone(), chat_history.clone(), hyperia_tools.clone()).await
                } else {
                    Err(anyhow::anyhow!("OpenAI key is not available"))
                }
            }
            _ => Err(anyhow::anyhow!("Unknown model")),
        };

        match result {
            Ok(_) => break,
            Err(e) => {
                eprintln!("Error running model {}: {}", current_model, e);
                if attempts == 1 {
                    let next_model = if current_model == "claude" { "openai" } else { "claude" };
                    if try_model(next_model) {
                        // Remove the duplicate user message that was pushed in the first failed attempt
                        {
                            let mut history = chat_history.lock().await;
                            if let Some(last) = history.last() {
                                if last.role == "user" && last.text == text {
                                    history.pop();
                                }
                            }
                        }
                        let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                            "type": "model_switched",
                            "model": next_model
                        })).unwrap().into())).await;
                        let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                            "type": "system_notification",
                            "text": format!("Model {} execution failed ({}). Automatically switching/falling back...", current_model, e)
                        })).unwrap().into())).await;
                        current_model = next_model;
                        continue;
                    }
                }
                
                let _ = tx.send(Message::Text(serde_json::to_string(&json!({
                    "type": "system_notification",
                    "text": format!("Agent execution failed: {}", e)
                })).unwrap().into())).await;
                break;
            }
        }
    }
}

async fn synthesize_speech_elevenlabs(text: &str, api_key: &str) -> Option<String> {
    let client = reqwest::Client::new();
    let url = "https://api.elevenlabs.io/v1/text-to-speech/r1KmysJdVYZjJCm4mL3b";
    
    let payload = json!({
        "text": text,
        "model_id": "eleven_monolingual_v1",
        "voice_settings": {
            "stability": 0.5,
            "similarity_boost": 0.75
        }
    });

    match client.post(url)
        .header("xi-api-key", api_key)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(res) => {
            if res.status().is_success() {
                if let Ok(bytes) = res.bytes().await {
                    let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                    return Some(b64);
                }
            } else {
                let err_text = res.text().await.unwrap_or_default();
                eprintln!("ElevenLabs API error: {}", err_text);
            }
        }
        Err(e) => {
            eprintln!("Failed to contact ElevenLabs: {}", e);
        }
    }
    None
}

async fn handle_voice_summary(
    accumulated_response: &str,
    websocket_sender: &mpsc::Sender<Message>,
) {
    let eleven_key = match std::env::var("ELEVENLABS_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            println!("[ElevenLabs] Warning: ELEVENLABS_API_KEY is not set in the environment. Voice summary skipped.");
            return;
        }
    };

    match (accumulated_response.find("<voice_summary>"), accumulated_response.find("</voice_summary>")) {
        (Some(start_idx), Some(end_idx)) => {
            let voice_text = &accumulated_response[start_idx + "<voice_summary>".len()..end_idx];
            println!("[ElevenLabs] Found voice summary text: {}", voice_text);
            println!("[ElevenLabs] Synthesizing speech for: {}", voice_text);
            
            if let Some(b64_audio) = synthesize_speech_elevenlabs(voice_text, &eleven_key).await {
                println!("[ElevenLabs] Synthesis successful! Sending base64 audio to client.");
                let _ = websocket_sender.send(Message::Text(serde_json::to_string(&json!({
                    "type": "play_audio",
                    "audio_base64": b64_audio
                })).unwrap().into())).await;
            } else {
                eprintln!("[ElevenLabs] Synthesis failed (API or connection error).");
            }
        }
        _ => {
            println!("[ElevenLabs] Info: No <voice_summary> tags found in the response.");
        }
    }
}






