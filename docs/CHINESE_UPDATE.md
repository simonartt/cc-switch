# CC Switch — 局域网广播功能更新

> 版本: v3.16.3-lan-broadcast
> 更新内容: 新增局域网广播 (LAN Broadcast) 功能

---

## 🚀 新功能：局域网广播

现在 CC Switch 可以作为**局域网数据源**，让你的 Cardputer 等设备在同一个局域网内直接获取用量数据，无需经过公网服务器。

### 工作原理

```
┌─────────────┐       UDP 广播 (3445)        ┌──────────────┐
│  CC Switch  │ ──────────────────────────→  │  Cardputer   │
│  (桌面端)   │     每 3 秒广播一次           │  (M5Stack)   │
│             │                               │              │
│  HTTP :3345 │ ←────── HTTP 请求 ────────    │  显示屏      │
│  /summary   │                               │  用量概览    │
│  /logs      │                               │  请求日志    │
│  /trends    │                               │  趋势图      │
└─────────────┘                               └──────────────┘
```

### 如何使用

1. **桌面端**：打开 CC Switch → 设置 → 高级 → **LAN Broadcast** → 打开开关
2. **Cardputer**：开机选择 `[2] 局域网发现`，自动扫描并连接桌面端

### 技术细节

| 项目 | 说明 |
|------|------|
| UDP 广播端口 | 3445 |
| HTTP 服务端口 | 3345 |
| 广播频率 | 每 3 秒 |
| 广播内容 | `{"v":1,"name":"hostname","port":3345}` |
| API 端点 | `/api/v1/summary`、`/api/v1/logs`、`/api/v1/trends` |
| 数据源 | 本地 SQLite 数据库（与远程服务器格式兼容） |

### API 响应格式

**GET /api/v1/summary**
```json
{
  "summary": {
    "total_requests": 1234,
    "total_input_tokens": 567890,
    "total_output_tokens": 123456,
    "total_cache_read_tokens": 100000,
    "total_cache_creation_tokens": 50000,
    "total_cost_usd": 12.34
  }
}
```

**GET /api/v1/logs?limit=6**
```json
{
  "logs": [
    {
      "model": "claude-sonnet-4",
      "input_tokens": 1500,
      "output_tokens": 800,
      "total_cost_usd": 0.015,
      "latency_ms": 3200
    }
  ]
}
```

**GET /api/v1/trends?days=8**
```json
{
  "trends": [
    {"date": "2025-06-15", "requests": 120},
    {"date": "2025-06-16", "requests": 95}
  ]
}
```

---

## 📦 下载

### Windows (x86_64)

- **MSI 安装包**: 从 GitHub Actions 构建产物中下载
- **便携版 (Portable)**: 解压即用，无需安装

### macOS

- 等待 release 正式发布，或自行 `pnpm tauri build`

### 固件 (Cardputer)

- 固件代码位于独立仓库 `cardputer-monitor`，支持远程服务器 + 局域网发现两种模式
- 支持颜色主题切换（绿/蓝/橙）
- 无 emoji、无填充色、大字号显示

---

## ⚠️ 注意事项

1. **局域网要求**：桌面端和 Cardputer 必须在同一个网段
2. **防火墙**：确保 3345 (TCP) 和 3445 (UDP) 端口未被防火墙拦截
3. **广播范围**：UDP 广播仅限当前子网，跨网段需使用远程服务器模式
4. **数据一致性**：局域网模式读取的是本地数据库，与远程服务器数据可能不同步
