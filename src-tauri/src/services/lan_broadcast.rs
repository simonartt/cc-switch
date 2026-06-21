//! 局域网广播服务
//!
//! 在本地局域网启动 HTTP 服务 + UDP 广播，让 Cardputer 等设备自动发现。
//! HTTP 端点与远程服务器 (111.231.44.136:3344) 保持兼容，
//! 数据源为本地的 SQLite 数据库。

use crate::database::Database;
use crate::services::usage_stats::LogFilters;
use axum::{
    extract::{Query, State as AxumState},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// 局域网广播端口（UDP 发现用）
const BROADCAST_PORT: u16 = 3445;
/// HTTP 服务端口
const HTTP_PORT: u16 = 3345;

/// UDP 广播内容
#[derive(Serialize)]
struct BroadcastAnnounce {
    v: u8,
    name: String,
    port: u16,
}

/// 服务端响应格式：/api/v1/summary
#[derive(Serialize)]
struct SummaryResponse {
    summary: SummaryData,
}

#[derive(Serialize)]
struct SummaryData {
    total_requests: u64,
    total_input_tokens: u64,
    total_output_tokens: u64,
    total_cache_read_tokens: u64,
    total_cache_creation_tokens: u64,
    total_cost_usd: f64,
}

/// 服务端响应格式：/api/v1/logs
#[derive(Serialize)]
struct LogsResponse {
    logs: Vec<LogEntry>,
}

/// 匹配远程服务器的日志条目
#[derive(Serialize)]
struct LogEntry {
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    total_cost_usd: f64,
    latency_ms: u64,
}

/// 服务端响应格式：/api/v1/trends
#[derive(Serialize)]
struct TrendsResponse {
    trends: Vec<TrendEntry>,
}

#[derive(Serialize)]
struct TrendEntry {
    date: String,
    requests: u64,
}

/// LAN 广播服务句柄
pub struct LanBroadcast;

impl LanBroadcast {
    /// 启动 HTTP 服务 + UDP 广播
    pub async fn start(
        db: Arc<Database>,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<(), String> {
        let http_handle = Self::start_http(db.clone(), stop_flag.clone());
        let udp_handle = Self::start_udp(stop_flag.clone());

        // 等待任一任务结束（如果有错误）
        tokio::select! {
            r = http_handle => {
                if let Err(e) = r {
                    return Err(format!("HTTP server error: {}", e));
                }
            }
            r = udp_handle => {
                if let Err(e) = r {
                    return Err(format!("UDP broadcast error: {}", e));
                }
            }
        }

        Ok(())
    }

    fn start_http(
        db: Arc<Database>,
        stop_flag: Arc<AtomicBool>,
    ) -> JoinHandle<Result<(), String>> {
        tokio::spawn(async move {
            let app_state = HttpAppState { db };

            let app = Router::new()
                .route("/api/v1/summary", get(Self::handle_summary))
                .route("/api/v1/logs", get(Self::handle_logs))
                .route("/api/v1/trends", get(Self::handle_trends))
                .with_state(app_state);

            let addr = format!("0.0.0.0:{}", HTTP_PORT);
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .map_err(|e| format!("Failed to bind HTTP server on {}: {}", addr, e))?;

            log::info!("LAN broadcast HTTP server started on {}", addr);

            // 用 axum::serve 同时监听停止信号
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    while !stop_flag.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                })
                .await
                .map_err(|e| format!("HTTP server error: {}", e))?;

            log::info!("LAN broadcast HTTP server stopped");
            Ok(())
        })
    }

    fn start_udp(stop_flag: Arc<AtomicBool>) -> JoinHandle<Result<(), String>> {
        tokio::spawn(async move {
            let socket = UdpSocket::bind("0.0.0.0:0")
                .map_err(|e| format!("Failed to create UDP socket: {}", e))?;
            socket
                .set_broadcast(true)
                .map_err(|e| format!("Failed to set broadcast: {}", e))?;

            let hostname = hostname();
            let announce = serde_json::to_string(&BroadcastAnnounce {
                v: 1,
                name: hostname,
                port: HTTP_PORT,
            })
            .unwrap_or_default();

            log::info!(
                "LAN broadcast started on UDP port {}",
                BROADCAST_PORT
            );

            let broadcast_addr = format!("255.255.255.255:{}", BROADCAST_PORT);

            while !stop_flag.load(Ordering::Relaxed) {
                if let Err(e) = socket.send_to(announce.as_bytes(), &broadcast_addr) {
                    log::warn!("UDP broadcast send error: {}", e);
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }

            log::info!("LAN broadcast UDP sender stopped");
            Ok(())
        })
    }

    // ─── HTTP handlers ───

    async fn handle_summary(
        AxumState(state): AxumState<HttpAppState>,
    ) -> Result<Json<SummaryResponse>, StatusCode> {
        let db = &state.db;
        let summary = db
            .get_usage_summary(None, None, None, None, None)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let cost: f64 = summary
            .total_cost
            .parse()
            .unwrap_or(0.0);

        Ok(Json(SummaryResponse {
            summary: SummaryData {
                total_requests: summary.total_requests,
                total_input_tokens: summary.total_input_tokens,
                total_output_tokens: summary.total_output_tokens,
                total_cache_read_tokens: summary.total_cache_read_tokens,
                total_cache_creation_tokens: summary.total_cache_creation_tokens,
                total_cost_usd: cost,
            },
        }))
    }

    async fn handle_logs(
        AxumState(state): AxumState<HttpAppState>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Json<LogsResponse>, StatusCode> {
        let limit: usize = params
            .get("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(6);

        let db = &state.db;
        let paginated = db
            .get_request_logs(&LogFilters::default(), 0, limit as u32)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let logs: Vec<LogEntry> = paginated
            .data
            .into_iter()
            .map(|l| {
                let cost: f64 = l.total_cost_usd.parse().unwrap_or(0.0);
                LogEntry {
                    model: l.model,
                    input_tokens: l.input_tokens as u64,
                    output_tokens: l.output_tokens as u64,
                    total_cost_usd: cost,
                    latency_ms: l.latency_ms,
                }
            })
            .collect();

        Ok(Json(LogsResponse { logs }))
    }

    async fn handle_trends(
        AxumState(state): AxumState<HttpAppState>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Json<TrendsResponse>, StatusCode> {
        let _days: i64 = params
            .get("days")
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);

        let db = &state.db;
        let daily_stats = db
            .get_daily_trends(None, None, None, None, None)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let trends: Vec<TrendEntry> = daily_stats
            .into_iter()
            .map(|d| TrendEntry {
                date: d.date.clone(),
                requests: d.request_count,
            })
            .collect();

        Ok(Json(TrendsResponse { trends }))
    }
}

fn hostname() -> String {
    // 跨平台主机名获取
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("scutil")
            .arg("--get")
            .arg("ComputerName")
            .output()
            .ok()
            .and_then(|o| {
                String::from_utf8(o.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            })
            .unwrap_or_else(|| "CC-Switch".to_string())
    }
    #[cfg(not(target_os = "macos"))]
    {
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "CC-Switch".to_string())
    }
}

/// Axum 应用状态
#[derive(Clone)]
struct HttpAppState {
    db: Arc<Database>,
}
