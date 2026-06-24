//! 局域网广播服务
//!
//! 在本地局域网启动 HTTP 服务 + UDP 广播，让 Cardputer 等设备自动发现。
//! HTTP 端点与远程服务器 (111.231.44.136:3344) 保持兼容，
//! 数据源为本地的 SQLite 数据库。

use crate::database::Database;
use crate::services::usage_stats::LogFilters;
use crate::services::usage_stats::find_model_pricing;
use crate::proxy::usage::calculator::CostCalculator;
use crate::proxy::usage::TokenUsage;
use rust_decimal::Decimal;
use axum::{
    extract::{Query, State as AxumState},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// 广播状态
pub struct BroadcastState {
    pub running: bool,
    pub local_ip: String,
}

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

/// 可选的日期范围查询参数（Unix 秒级时间戳）
#[derive(Deserialize, Default)]
struct DateRangeQuery {
    start_date: Option<i64>,
    end_date: Option<i64>,
}

impl DateRangeQuery {
    /// 默认返回「今天」范围（匹配桌面端默认 preset="today"）
    /// 使用本地时区的 00:00:00，与前端 resolveUsageRange("today") 保持完全一致
    fn today() -> Self {
        let local = chrono::Local::now();
        let now_ts = local.timestamp();
        let midnight = local
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(chrono::Local)
            .unwrap()
            .timestamp();
        Self {
            start_date: Some(midnight),
            end_date: Some(now_ts),
        }
    }

    fn resolve(&self) -> (Option<i64>, Option<i64>) {
        if self.start_date.is_some() || self.end_date.is_some() {
            (self.start_date, self.end_date)
        } else {
            let d = Self::today();
            (d.start_date, d.end_date)
        }
    }
}

/// LAN 广播服务句柄
pub struct LanBroadcast;

impl LanBroadcast {
    /// 启动 HTTP 服务 + UDP 广播（IP 已在命令层检测完成）
    pub async fn start_services(
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

    fn start_http(db: Arc<Database>, stop_flag: Arc<AtomicBool>) -> JoinHandle<Result<(), String>> {
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
            let local_ip = detect_local_ip();
            let announce = serde_json::to_string(&BroadcastAnnounce {
                v: 1,
                name: hostname,
                port: HTTP_PORT,
            })
            .unwrap_or_default();

            log::info!("LAN broadcast started on UDP port {}", BROADCAST_PORT);

            // 同时向 255.255.255.255 和子网广播地址发送
            let mut addrs = vec![format!("255.255.255.255:{}", BROADCAST_PORT)];
            if let Some(subnet_bcast) = subnet_broadcast(&local_ip) {
                addrs.push(format!("{}:{}", subnet_bcast, BROADCAST_PORT));
            }

            log::info!("LAN broadcast targets: {:?}", addrs);

            while !stop_flag.load(Ordering::Relaxed) {
                for addr in &addrs {
                    if let Err(e) = socket.send_to(announce.as_bytes(), addr) {
                        log::warn!("UDP broadcast send error to {}: {}", addr, e);
                    }
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
        Query(params): Query<DateRangeQuery>,
    ) -> Result<Json<SummaryResponse>, StatusCode> {
        let (start_date, end_date) = params.resolve();
        let db = &state.db;
        let summary = db
            .get_usage_summary(start_date, end_date, None, None, None)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut cost: f64 = summary.total_cost.parse().unwrap_or(0.0);

        // 对 DB 中 total_cost_usd="0" 但有 token 消耗的记录，从定价表实时重算成本
        let conn = db.conn.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let mut query_sql = String::from(
            "SELECT request_id, app_type, model, request_model, input_tokens, output_tokens,
                    cache_read_tokens, cache_creation_tokens
             FROM proxy_request_logs
             WHERE total_cost_usd IN ('0', '0.0')
               AND (input_tokens > 0 OR output_tokens > 0)"
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(start) = start_date {
            query_sql.push_str(" AND created_at >= ?");
            params.push(Box::new(start));
        }
        if let Some(end) = end_date {
            query_sql.push_str(" AND created_at <= ?");
            params.push(Box::new(end));
        }
        query_sql.push_str(" ORDER BY rowid");

        if let Ok(mut stmt) = conn.prepare(&query_sql) {
            let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
            let rows_result = stmt.query_map(param_refs.as_slice(), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            });
            if let Ok(rows) = rows_result {
                for row_result in rows {
                    if let Ok((_, ref app_type, ref model, ref request_model, input_t, output_t, cache_read, cache_create)) = row_result {
                        let model_id = request_model.as_deref().unwrap_or(model);
                        if let Some(pricing) = find_model_pricing(&conn, model_id) {
                            let usage = TokenUsage {
                                input_tokens: input_t as u32,
                                output_tokens: output_t as u32,
                                cache_read_tokens: cache_read as u32,
                                cache_creation_tokens: cache_create as u32,
                                model: Some(model.clone()),
                                message_id: None,
                            };
                            let calc = CostCalculator::calculate_for_app(
                                app_type,
                                &usage,
                                &pricing,
                                Decimal::ONE,
                            );
                            if let Ok(extra) = calc.total_cost.to_string().parse::<f64>() {
                                cost += extra;
                            }
                        }
                    }
                }
            }
        }
        drop(conn);

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
        let date_params: DateRangeQuery = DateRangeQuery {
            start_date: params.get("start_date").and_then(|v| v.parse().ok()),
            end_date: params.get("end_date").and_then(|v| v.parse().ok()),
        };
        let (start_date, end_date) = date_params.resolve();

        let limit: usize = params
            .get("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(6);

        let db = &state.db;
        let conn = db.conn.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let filters = LogFilters {
            app_type: None,
            provider_name: None,
            model: None,
            status_code: None,
            start_date,
            end_date,
        };
        let paginated = db
            .get_request_logs(&filters, 0, limit as u32)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let logs: Vec<LogEntry> = paginated
            .data
            .into_iter()
            .map(|l| {
                let mut cost: f64 = l.total_cost_usd.parse().unwrap_or(0.0);
                // 对 cost="0" 但有 token 的记录实时重算
                if cost == 0.0 && (l.input_tokens > 0 || l.output_tokens > 0) {
                    if let Some(pricing) = find_model_pricing(&conn, &l.model) {
                        let usage = TokenUsage {
                            input_tokens: l.input_tokens as u32,
                            output_tokens: l.output_tokens as u32,
                            cache_read_tokens: l.cache_read_tokens as u32,
                            cache_creation_tokens: l.cache_creation_tokens as u32,
                            model: Some(l.model.clone()),
                            message_id: None,
                        };
                        let calc = CostCalculator::calculate_for_app(
                            &l.app_type,
                            &usage,
                            &pricing,
                            Decimal::ONE,
                        );
                        if let Ok(c) = calc.total_cost.to_string().parse::<f64>() {
                            cost = c;
                        }
                    }
                }
                LogEntry {
                    model: l.model,
                    input_tokens: l.input_tokens as u64,
                    output_tokens: l.output_tokens as u64,
                    total_cost_usd: cost,
                    latency_ms: l.latency_ms,
                }
            })
            .collect();
        drop(conn);

        Ok(Json(LogsResponse { logs }))
    }

    async fn handle_trends(
        AxumState(state): AxumState<HttpAppState>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Json<TrendsResponse>, StatusCode> {
        let date_params: DateRangeQuery = DateRangeQuery {
            start_date: params.get("start_date").and_then(|v| v.parse().ok()),
            end_date: params.get("end_date").and_then(|v| v.parse().ok()),
        };
        let (start_date, end_date) = date_params.resolve();

        let _days: i64 = params.get("days").and_then(|v| v.parse().ok()).unwrap_or(8);

        let db = &state.db;
        let daily_stats = db
            .get_daily_trends(start_date, end_date, None, None, None)
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

/// 检测本机主局域网 IP 地址
pub fn detect_local_ip() -> String {
    // Windows: 解析 ipconfig，兼容中英文系统，隐藏命令窗口
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;

        if let Ok(out) = std::process::Command::new("ipconfig")
            .creation_flags(CREATE_NO_WINDOW)
            .output()
        {
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines() {
                let trimmed = line.trim();
                if !trimmed.contains("IPv4") {
                    continue;
                }
                // 提取冒号后的内容，兼容 : 和 ：
                let after_colon = trimmed
                    .split(':').nth(1)
                    .or_else(|| trimmed.split('：').nth(1))
                    .unwrap_or("")
                    .trim();
                // 提取 IP 地址 (xxx.xxx.xxx.xxx)，去掉尾部 (首选)/(Preferred) 等后缀
                if let Some(raw) = after_colon
                    .split_whitespace()
                    .find(|s| s.chars().filter(|&c| c == '.').count() == 3)
                {
                    let clean_ip: String = raw
                        .chars()
                        .take_while(|c| c.is_ascii_digit() || *c == '.')
                        .collect();
                    if let Ok(ip) = clean_ip.parse::<std::net::Ipv4Addr>() {
                        if !ip.is_loopback() && !ip.is_link_local() {
                            return ip.to_string();
                        }
                    }
                }
            }
        }

        // Fallback: UDP socket 获取本机 IP
        if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
            socket.set_read_timeout(Some(std::time::Duration::from_millis(100))).ok();
            if socket.connect("192.168.6.255:1").is_ok()
                || socket.connect("10.255.255.255:1").is_ok()
            {
                if let Ok(local) = socket.local_addr() {
                    let ip = local.ip();
                    if !ip.is_loopback() {
                        return ip.to_string();
                    }
                }
            }
        }
    }

    // macOS: 用 ifconfig 解析
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("ifconfig").args(["-l"]).output() {
            let ifaces_str = String::from_utf8_lossy(&out.stdout);
            for iface in ifaces_str.split_whitespace() {
                if iface == "lo0" || iface == "lo" {
                    continue;
                }
                if let Ok(addr_out) = std::process::Command::new("ifconfig")
                    .args([iface])
                    .output()
                {
                    let info = String::from_utf8_lossy(&addr_out.stdout);
                    for line in info.lines() {
                        let line = line.trim();
                        if let Some(rest) = line.strip_prefix("inet ") {
                            if let Some(ip_str) = rest.split_whitespace().next() {
                                if let Ok(ip) = ip_str.parse::<std::net::Ipv4Addr>() {
                                    if !ip.is_loopback() && !ip.is_link_local() {
                                        return ip.to_string();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Linux: 解析 ip addr
    #[cfg(target_os = "linux")]
    {
        if let Ok(out) = std::process::Command::new("ip")
            .args(["-o", "-4", "addr", "show"])
            .output()
        {
            let info = String::from_utf8_lossy(&out.stdout);
            for line in info.lines() {
                // Format: "2: eth0    inet 192.168.1.5/24 brd ..."
                if let Some(inet_part) = line.split_whitespace()
                    .skip_while(|&w| w != "inet")
                    .nth(1)
                {
                    if let Some(ip_str) = inet_part.split('/').next() {
                        if let Ok(ip) = ip_str.parse::<std::net::Ipv4Addr>() {
                            if !ip.is_loopback() && !ip.is_link_local() {
                                return ip.to_string();
                            }
                        }
                    }
                }
            }
        }
    }

    // 最终 fallback
    "127.0.0.1".to_string()
}

/// 根据 IP 计算子网广播地址（假设 /24 子网）
fn subnet_broadcast(ip: &str) -> Option<String> {
    if let Ok(v4) = ip.parse::<std::net::Ipv4Addr>() {
        let octets = v4.octets();
        if octets[0] != 127 && octets[0] != 169 {
            let bcast = std::net::Ipv4Addr::new(octets[0], octets[1], octets[2], 255);
            return Some(bcast.to_string());
        }
    }
    None
}

/// Axum 应用状态
#[derive(Clone)]
struct HttpAppState {
    db: Arc<Database>,
}
