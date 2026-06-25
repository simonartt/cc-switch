//! 使用统计远程推送服务
//!
//! 当用户在 Settings 中启用"使用统计推送"并配置了服务器 URL 后，
//! 本模块会定期将 `proxy_request_logs` 表中的新增记录推送到远程服务器。
//!
//! 设计要点：
//! - 启动时检查设置，若不启用或未配置 URL 则跳过
//! - 每隔 30 秒检查一次状态变化和新的日志记录
//! - 记录已推送的最大 ID，避免重复推送
//! - 使用 reqwest 进行 HTTP POST，失败时记录日志并重试
//! - 推送前用定价表重新计算 cost（DB 中未配定价模型 cost 为 "0"）

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::database::Database;
use reqwest::Client as HttpClient;
use rust_decimal::Decimal;
use serde::Serialize;

/// 推送到远程服务器的单条日志
#[derive(Debug, Clone, Serialize)]
struct PushLogEntry {
    request_id: String,
    app_type: String,
    provider_id: String,
    model: String,
    request_model: Option<String>,
    pricing_model: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    cache_creation_tokens: i64,
    input_cost_usd: String,
    output_cost_usd: String,
    cache_read_cost_usd: String,
    cache_creation_cost_usd: String,
    total_cost_usd: String,
    latency_ms: i64,
    first_token_ms: Option<i64>,
    duration_ms: Option<i64>,
    status_code: i64,
    error_message: Option<String>,
    is_streaming: bool,
    data_source: String,
    created_at: i64,
}

/// 推送请求体
#[derive(Debug, Clone, Serialize)]
struct PushRequestBody {
    logs: Vec<PushLogEntry>,
    device_id: String,
    device_name: String,
    timestamp: i64,
}

/// 推送状态标记（模块级单例）
static PUSH_ENABLED: AtomicBool = AtomicBool::new(false);

/// 获取设备名称（带 OS 前缀，格式: [MACOS:xxx] 或 [PC:xxx]）
fn get_device_name() -> String {
    crate::utils::get_os_device_name()
}

/// 获取设备 ID（用 hostname 作为设备 ID）
fn get_device_id() -> String {
    crate::utils::get_os_device_name()
}

/// 启动推送服务
///
/// 在应用 setup 阶段调用。从数据库读取设置，根据开关和 URL 决定是否启动。
pub fn start_worker(db: Arc<Database>) {
    // 读取当前设置
    let settings = crate::settings::get_settings();
    if !settings.usage_push_enabled {
        log::info!("[usage-push] 推送服务未启用，跳过启动");
        return;
    }

    let server_url = match &settings.usage_push_server_url {
        Some(url) if !url.is_empty() => url.clone(),
        _ => {
            log::info!("[usage-push] 未配置服务器 URL，跳过启动");
            return;
        }
    };

    log::info!("[usage-push] 启动推送服务，目标: {}", server_url);

    PUSH_ENABLED.store(true, Ordering::Release);

    let device_id = get_device_id();
    let device_name = get_device_name();

    tauri::async_runtime::spawn(async move {
        run_push_loop(db, device_id, device_name).await;
    });
}

/// 如果设置变更，重新启动推送服务
pub fn restart_if_needed() {
    let settings = crate::settings::get_settings();
    let should_be_enabled = settings.usage_push_enabled
        && settings
            .usage_push_server_url
            .as_deref()
            .map(|u| !u.is_empty())
            .unwrap_or(false);

    let was_enabled = PUSH_ENABLED.swap(should_be_enabled, Ordering::AcqRel);

    if was_enabled == should_be_enabled {
        return; // 状态未变，无需操作
    }

    if should_be_enabled {
        log::info!("[usage-push] 设置已变更，将自动生效（下次循环）");
        // 不需要显式启动，因为当前可能在跑的旧 worker 循环会退出的
        // （PUSH_ENABLED 被设为 false 时旧 worker 检测到后退出），
        // 但新的 should_be_enabled=true 意味着旧 worker 也不存在。
        // 解决方式：这里重新读一次，新 worker 的 start_worker 被调用
        let settings2 = crate::settings::get_settings();
        if settings2.usage_push_enabled {
            let db = match crate::database::Database::new_readonly() {
                Some(d) => d,
                None => {
                    log::warn!("[usage-push] 获取数据库失败");
                    return;
                }
            };
            start_worker(db);
        }
    } else {
        // 标记已设为 false，运行中的 worker 会在下次循环检测到并退出
        log::info!("[usage-push] 标记已清除，等待运行中的推送线程退出");
    }
}

/// 后台推送循环
async fn run_push_loop(db: Arc<Database>, device_id: String, device_name: String) {
    let client = match HttpClient::builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            log::error!("[usage-push] 创建 HTTP 客户端失败: {e}");
            return;
        }
    };

    // 记录上次推送的最大 ID
    let mut last_id: i64 = 0;

    // 先推送所有历史数据，不再跳过已有记录
    log::debug!("[usage-push] 初始化 last_id = {} (从最早记录开始推送)", last_id);

    // 主循环: 从设置读取间隔（默认 30 秒）
    loop {
        // 每次迭代重新读取设置，支持热更新 URL 和间隔
        let settings = crate::settings::get_settings();
        if !settings.usage_push_enabled {
            log::debug!("[usage-push] 推送已禁用，停止循环");
            PUSH_ENABLED.store(false, Ordering::Release);
            break;
        }

        let interval = settings.usage_push_interval_secs.max(5); // 最小 5 秒
        tokio::time::sleep(Duration::from_secs(interval)).await;

        let server_url = match &settings.usage_push_server_url {
            Some(url) if !url.is_empty() => url.trim_end_matches('/').to_string(),
            _ => {
                log::debug!("[usage-push] URL 已清空，停止循环");
                PUSH_ENABLED.store(false, Ordering::Release);
                break;
            }
        };
        let push_url = format!("{}/api/v1/push", server_url);

        // 查询从 last_id 之后的新记录
        let logs = match db.get_usage_logs_since(last_id, 200) {
            Ok(rows) => rows,
            Err(e) => {
                log::warn!("[usage-push] 查询新日志失败: {e}");
                continue;
            }
        };

        if logs.is_empty() {
            continue;
        }

        // 构造推送数据，对 cost 为 "0" 但有 token 的记录重算成本
        let push_logs: Vec<PushLogEntry> = {
            let conn = db.conn.lock().unwrap_or_else(|e| {
                log::error!("[usage-push] Mutex lock failed: {}", e);
                panic!("Mutex poisoned, cannot proceed")
            });
            let result: Vec<PushLogEntry> = logs
                .iter()
                .map(|row| {
                    let model_id = row.request_model.as_deref().unwrap_or(&row.model);
                    let total_cost = if row.total_cost_usd == "0"
                        && (row.input_tokens > 0 || row.output_tokens > 0)
                    {
                        crate::services::usage_stats::find_model_pricing(&conn, model_id)
                            .map(|pricing| {
                                let usage = crate::proxy::usage::TokenUsage {
                                    input_tokens: row.input_tokens as u32,
                                    output_tokens: row.output_tokens as u32,
                                    cache_read_tokens: row.cache_read_tokens as u32,
                                    cache_creation_tokens: row.cache_creation_tokens as u32,
                                    model: Some(row.model.clone()),
                                    message_id: None,
                                };
                                crate::proxy::usage::calculator::CostCalculator::calculate_for_app(
                                    &row.app_type,
                                    &usage,
                                    &pricing,
                                    Decimal::ONE,
                                )
                                .total_cost
                                .to_string()
                            })
                            .unwrap_or_else(|| row.total_cost_usd.clone())
                    } else {
                        row.total_cost_usd.clone()
                    };

                    PushLogEntry {
                        request_id: row.request_id.clone(),
                        app_type: row.app_type.clone(),
                        provider_id: row.provider_id.clone(),
                        model: row.model.clone(),
                        request_model: row.request_model.clone(),
                        pricing_model: row.pricing_model.clone(),
                        input_tokens: row.input_tokens,
                        output_tokens: row.output_tokens,
                        cache_read_tokens: row.cache_read_tokens,
                        cache_creation_tokens: row.cache_creation_tokens,
                        input_cost_usd: row.input_cost_usd.clone(),
                        output_cost_usd: row.output_cost_usd.clone(),
                        cache_read_cost_usd: row.cache_read_cost_usd.clone(),
                        cache_creation_cost_usd: row.cache_creation_cost_usd.clone(),
                        total_cost_usd: total_cost,
                        latency_ms: row.latency_ms,
                        first_token_ms: row.first_token_ms,
                        duration_ms: row.duration_ms,
                        status_code: row.status_code,
                        error_message: row.error_message.clone(),
                        is_streaming: row.is_streaming,
                        data_source: row.data_source.clone(),
                        created_at: row.created_at,
                    }
                })
                .collect();
            drop(conn);
            result
        };

        let body = PushRequestBody {
            logs: push_logs,
            device_id: device_id.clone(),
            device_name: device_name.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        // 发送
        match client.post(&push_url).json(&body).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    // 更新 last_id 为本批次最大 ID
                    if let Some(max_id) = logs.iter().map(|r| r.id).max() {
                        last_id = max_id;
                    }
                    log::debug!(
                        "[usage-push] 成功推送 {} 条记录，last_id 更新为 {}",
                        logs.len(),
                        last_id
                    );
                } else {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    log::warn!("[usage-push] 服务器返回错误 ({}): {}", status, text);
                    // 400 错误不重试（数据格式问题）
                    if status.is_client_error() {
                        log::warn!("[usage-push] 客户端错误，跳过这批记录");
                        if let Some(max_id) = logs.iter().map(|r| r.id).max() {
                            last_id = max_id;
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("[usage-push] 推送请求失败: {e}");
                // 网络错误等，下次循环重试
            }
        }
    }
}
