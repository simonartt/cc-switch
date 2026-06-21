//! 局域网广播命令 — 启动/停止/状态

use crate::services::lan_broadcast::LanBroadcast;
use crate::store::AppState;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tauri::State;
use tokio::sync::Mutex;

/// 广播状态，在 AppState 之外单独管理
pub struct BroadcastState {
    pub running: bool,
}

/// 全局广播管理器，在 setup 中初始化
pub struct BroadcastManager {
    pub stop_flag: Arc<AtomicBool>,
    pub state: Arc<Mutex<BroadcastState>>,
}

impl BroadcastManager {
    pub fn new() -> Self {
        Self {
            stop_flag: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(BroadcastState { running: false })),
        }
    }
}

/// 启动局域网广播
#[tauri::command]
pub async fn start_lan_broadcast(
    app_state: State<'_, AppState>,
    bm: State<'_, BroadcastManager>,
) -> Result<String, String> {
    let mut state = bm.state.lock().await;
    if state.running {
        return Err("LAN broadcast is already running".to_string());
    }

    // Reset stop flag
    bm.stop_flag.store(false, Ordering::Relaxed);

    let db = app_state.db.clone();
    let stop_flag = bm.stop_flag.clone();
    let _state_clone = bm.state.clone();

    tokio::spawn(async move {
        if let Err(e) = LanBroadcast::start(db, stop_flag).await {
            log::error!("LAN broadcast failed: {}", e);
        }
    });

    state.running = true;
    Ok("LAN broadcast started".to_string())
}

/// 停止局域网广播
#[tauri::command]
pub async fn stop_lan_broadcast(
    bm: State<'_, BroadcastManager>,
) -> Result<String, String> {
    let mut state = bm.state.lock().await;
    if !state.running {
        return Err("LAN broadcast is not running".to_string());
    }

    bm.stop_flag.store(true, Ordering::Relaxed);
    state.running = false;
    Ok("LAN broadcast stopped".to_string())
}

/// 获取局域网广播状态
#[tauri::command]
pub async fn get_lan_broadcast_status(
    bm: State<'_, BroadcastManager>,
) -> Result<bool, String> {
    let state = bm.state.lock().await;
    Ok(state.running)
}
