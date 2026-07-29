# mercury2api

把 [chat.inceptionlabs.ai](https://chat.inceptionlabs.ai) 的未登录聊天 API 包装成标准 **OpenAI Chat Completions** 兼容端点 `/v1/chat/completions`，任意 OpenAI 客户端可直接调用。

逆向研究见 `../inceptionlabs-api-research/`（API 文档、复现脚本、抓包样本）。

## 原理

Inception 上游有两个外部门槛：

1. **Vercel Bot Challenge (WAF)**：按 TLS 指纹拦截非浏览器请求，`curl`/`httpx` 直接调 `/api/chat` 全部 `429`。
2. **AI SDK v5 `parts` 格式**：body 必须用 `messages[].parts[]{type:text,text}`，旧版 `content` 字段会被拒（500）。

本服务用 **chromedp** 驱动本地 Chrome 自动通过 challenge，在页面内用同源 `fetch()` 调 Inception API（同源请求携带 challenge cookie），再把返回的 SSE 翻译成标准 OpenAI 格式。

整体栈：
- **CLIProxyAPI v7 SDK**（`github.com/router-for-me/CLIProxyAPI/v7`）提供 HTTP 服务 / 路由 / 翻译框架，嵌入一个自定义 `ProviderExecutor`。
- **chromedp** 绕过 Vercel challenge，页面内 `fetch` 调 Inception。
- **translator**：executor 内主动调用 `sdk/translator` 完成双向翻译（SDK 不会自动翻译 custom executor 的响应，这是非选项约束）。

## 数据流

```
OpenAI 客户端 POST /v1/chat/completions {messages, stream, model, reasoning_effort}
   │ CLIProxyAPI handler 透传原始 OpenAI body 到 executor
   ▼
InceptionExecutor.Execute:
   1. TranslateRequestByFormatName(OpenAI → Inception)   // reqT：content→parts，reasoning_effort→thinking
   2. browser.Manager.FetchChat(inceptionBody)           // chromedp 页面内 fetch /api/chat，全量读回 SSE
   3. TranslateNonStreamByFormatName(Inception → OpenAI)  // respT：拼接 text-delta → chat.completion JSON
   4. return Response{Payload: openaiJSON}
   ▼ handler 原样写回 → 客户端收到标准 OpenAI 响应
```

流式 `{stream:true}` 同上，步骤 3 改 `TranslateStreamByFormatName` 切成多个 `chat.completion.chunk`；`[DONE]` 由 SDK handler 自动追加。v1 为**全量模式**（页面内 drain SSE 后返回），架构预留真流式（CDP Network/Fetch 域实时转发）。

## 运行

### 前置

- Go ≥ 1.26（CLIProxyAPI 声明 `go 1.26.0`；`GOTOOLCHAIN=auto` 会自动拉取）
- 本机 Chrome（chromedp 自动发现 `/Applications/Google Chrome.app`）

### 构建与启动

```bash
go build -o inception-proxy ./cmd/inception-proxy
./inception-proxy            # 读 config.yaml，监听 127.0.0.1:8317
```

环境变量：
- `INCEPTION_PROXY_CONFIG`：配置文件路径（默认 `config.yaml`）
- `INCEPTION_PROXY_HEADLESS`：`0` = 有头模式（调试），其余 = headless（默认）

### 配置

`config.yaml`：

```yaml
host: 127.0.0.1
port: 8317
auth-dir: ./auths
```

`auths/inception.json`（占位，无真实凭证；`type` 字段决定 executor 路由）：

```json
{"type":"inception"}
```

### 验证

```bash
# 非流式
curl -s http://127.0.0.1:8317/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"inception-standard","messages":[{"role":"user","content":"What is 2+2? one word"}]}'
# → {"object":"chat.completion","choices":[{"message":{"content":"4",...}}]}

# 流式
curl -N http://127.0.0.1:8317/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"inception-standard","stream":true,"messages":[{"role":"user","content":"Say pong"}]}'

# 模型列表
curl -s http://127.0.0.1:8317/v1/models

# reasoning 深度透传
curl -s http://127.0.0.1:8317/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"model":"inception-standard","reasoning_effort":"high","messages":[{"role":"user","content":"..."}]}'
```

服务**无鉴权**（本地/内网定位），客户端无需 API Key。

## 文件结构

```
inception-proxy/
├── config.yaml
├── auths/inception.json          # {"type":"inception"}
├── cmd/inception-proxy/main.go  # 装配：browser + core manager + cliproxy Builder + 守卫
└── internal/
    ├── browser/manager.go       # chromedp 单例：启动、过 challenge、页面内 fetch /api/chat
    └── inception/
        ├── executor.go          # InceptionExecutor（6 方法），编排 fetch + translator
        ├── transform.go         # init() 注册 OpenAI↔Inception 双向翻译器
        └── sse.go               # SSE 解析：提取 text-delta
```

## SDK 适配的两个关键点

CLIProxyAPI SDK 设计围绕内置 provider，自定义 provider 需两处守卫处理「未知 provider」路径，见 `main.go` 注释：

1. **模型注册被清空**：`service_models.registerModelsForAuth` 对非内置 provider 走 `default` 分支，因无 plugin 模型而 `UnregisterClient` 清掉 `OnAfterStart` 的注册。`OnAfterStart` 后的 file watcher 初始枚举也会清一次。
   → 启动周期守卫（2s ticker）检测 `GlobalModelRegistry().ClientSupportsModel` 丢失即补注册。

2. **executor 被覆盖**：config reload 路径 `applyWatcherConfigUpdate` 用 `forceReplaceAuths:true`，对 inception auth 的 `default` 分支无条件注册内置 `OpenAICompatExecutor` 覆盖 chromedp executor（报 `missing provider baseURL`）。
   → 同一守卫检测 `coreManager.Executor("inception")` 非本实例时 `RegisterExecutor` 换回。

这两个守卫是对 SDK「无自定义 provider 一等支持」的必要适配，仅在检测到被清/被覆盖时才动作（reconcile 仅在 auth/config 变更触发，不持续）。

## 约束（用户确认）

- 服务**无鉴权**（本地/内网）；
- 流式 **v1 全量模式**（页面内 drain SSE 后返回），架构预留真流式；
- 浏览器**单实例复用**（`UserDataDir` 持久化 challenge 状态）。
