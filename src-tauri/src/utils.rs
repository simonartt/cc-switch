//! 通用工具函数

/// 获取带 OS 前缀的设备名称
///
/// 格式: `[MACOS:电脑名称]` 或 `[PC:电脑名称]`
/// 用于推送数据和 LAN Broadcast，客户端据此切换显示哪个设备的数据。
pub fn get_os_device_name() -> String {
    let hostname = get_hostname();
    let prefix = if cfg!(target_os = "macos") {
        "MACOS"
    } else if cfg!(target_os = "windows") {
        "PC"
    } else {
        "PC" // Linux 等也归为 PC
    };
    format!("[{}:{}]", prefix, hostname)
}

fn get_hostname() -> String {
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
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                // fallback: 用 whoami 或 hostname
                std::process::Command::new("hostname")
                    .output()
                    .ok()
                    .and_then(|o| {
                        String::from_utf8(o.stdout)
                            .ok()
                            .map(|s| s.trim().to_string())
                    })
                    .unwrap_or_else(|| "Mac".to_string())
            })
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "PC".to_string())
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        std::env::var("HOSTNAME")
            .unwrap_or_else(|_| "Linux".to_string())
    }
}
