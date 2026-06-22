# CC Switch 交付记录

## 平台构建方式

### Windows (.exe)
- **构建方式**：GitHub Actions 自动编译
- **GitHub 仓库**：https://github.com/simonartt/cc-switch
- **触发方式**：推送到 `main` 分支自动触发
- **权限**：用户个人 token（已在 gh CLI 中配置）
- **产物下载**：Actions 运行完成后在 workflow run 页面下载

### macOS (.dmg)
- **构建方式**：本地 Tauri build
- **命令**：
  ```bash
  cd /Users/simon/cc-switch
  cargo tauri build
  ```
- **产物路径**：`src-tauri/target/release/bundle/dmg/cc-switch_*.dmg`
- **编译检查**：先检查 `target/release/bundle/` 下是否有已有产物再决定是否重编

### 依赖环境
- Rust 工具链（rustup）
- Node.js（前端构建）
- Tauri CLI

## 架构

- **前端**：React + TypeScript（src/ 目录）
- **后端**：Rust + Tauri 2（src-tauri/）
- **数据库**：SQLite（本地 cc-switch 数据）
- **LAN 广播**：Axum HTTP 服务 (3345) + UDP 广播 (3445)
- **远程推送**：推送使用统计到 usage-monitor-server（111.231.44.136:3344）
