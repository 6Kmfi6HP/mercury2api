# mercury2api

把 [chat.inceptionlabs.ai](https://chat.inceptionlabs.ai) 的未登录聊天 API 包装成标准 **OpenAI Chat Completions** 兼容端点 `/v1/chat/completions`，任意 OpenAI 客户端可直接调用。

逆向研究见 `../inceptionlabs-api-research/`（API 文档、复现脚本、抓包样本）。

## 原理

Inception 上游有两个门槛：

1. **`/api/chat` 需要 `x-session-token`**：调用前必须先 GET `/api/session` 取回 token，作为 `x-session-token` 头带上。
2. **AI SDK v5 `parts` 格式**：body 必须用 `messages[].parts[]{type:text,text}`，旧版 `content` 字段会被拒（500）。

经实测，`/api/session` 与 `/api/chat` 现阶段**不对浏览器级 TLS/HTTP2 指纹强制 WAF 挑战**：只需用 [bogdanfinn/tls-client](https://github.com/bogdanfinn/tls-client) 伪装 Chrome 指纹（`Chrome_146` profile）即可放行。因此本服务**不再驱动任何浏览器进程**，改为纯 Go 指纹直连：

- **tls-client** 单例 `HttpClient`（`Chrome_146` profile）直接 POST `/api/chat`，对外暴露全量 `FetchChat` 与流式 `FetchChatStream`。
- **token 缓存**：首次（或 401 失效）GET `/api/session` 取 token 并复用，收到 401 自动重取 token 再重试一次。
- **关键头**：`setBrowserHeaders` 补齐 `Sec-Fetch-*` 头族（`Dest:empty` / `Mode:cors` / `Site:same-origin` + `Referer`），模拟浏览器内 JS `fetch` 的真实样貌。实测缺这族头，即便 TLS 指纹已伪装仍被判非浏览器返回 `429`。
- **translator**：executor 内主动调用 `sdk/translator` 完成双向翻译（SDK 不会自动翻译 custom executor 的响应，这是非选项约束）。

整体栈：
- **CLIProxyAPI v7 SDK**（`github.com/router-for-me/CLIProxyAPI/v7`）提供 HTTP 服务 / 路由 / 翻译框架，嵌入一个自定义 `ProviderExecutor`。
- **tls-client** 取代早期 chromedp 浏览器方案做指纹直连（见下文「演进」）。

## 演进

v0 用 **chromedp** 驱动本地 Chrome 自动通过 Vercel Bot Challenge，在页面内用同源 `fetch()` 调 Inception API（同源请求携带 challenge cookie），再把返回的 SSE 翻译成标准 OpenAI 格式。

实测发现上游已不对浏览器级指纹强制挑战，于是**彻底移除浏览器进程**，改为纯 Go tls-client 指纹直连（`internal/browser/manager.go` 内部重构，对外 `NewManager/Start/WaitReady/FetchChat/Close` 与 `FetchResult` 签名不变，文件名保留为历史命名）。`userDataDir/headless` 参数仅留作兼容签名占位，已不使用。

流式也从「伪流式」改为**真流式**：原先整段读完上游 SSE 后一次性下发，现由 `FetchChatStream` 返回未读 body，executor 用 `bufio.Scanner` 逐行读，每个 `text-delta` 立刻封装成 OpenAI chunk 推入 channel，handler 逐帧 flush，达成打字机效果。

## 数据流

```
OpenAI 客户端 POST /v1/chat/completions {messages, stream, model, reasoning_effort}
   │ CLIProxyAPI handler 透传原始 OpenAI body 到 executor
   ▼
InceptionExecutor.Execute / ExecuteStream:
   1. TranslateRequestByFormatName(OpenAI → Inception)   // reqT：content→parts，reasoning_effort→thinking
   2. 全量: manager.FetchChat(inceptionBody)            // tls-client POST /api/chat，全量读回 SSE
      流式: manager.FetchChatStream(inceptionBody)      // 返回未读 *fhttp.Response
   3. 全量: TranslateNonStreamByFormatName → chat.completion JSON
      流式: bufio.Scanner 逐行 → text-delta 即刻 openAIStreamChunker → chat.completion.chunk channel
   4. return Response{Payload} / 流式 StreamChunk
   ▼ handler 原样写回（流式逐帧 flush）→ 客户端收到标准 OpenAI 响应
```

中线 `[DONE]` 由 SDK handler 自动追加，executor 不手动加（对齐 SDK `handlers_stream.go` / `openai_handlers.go` 契约）。

## 运行

### 前置

- Go ≥ 1.26（CLIProxyAPI 声明 `go 1.26.0`；`GOTOOLCHAIN=auto` 会自动拉取）

无需本机 Chrome——已改为纯 Go 指纹直连。

### 构建与启动

```bash
go build -o inception-proxy ./cmd/inception-proxy
./inception-proxy            # 读 config.yaml，监听 127.0.0.1:8317
```

环境变量：
- `INCEPTION_PROXY_CONFIG`：配置文件路径（默认 `config.yaml`）
- `INCEPTION_PROXY_HEADLESS`：历史兼容占位，`0` = 有头（仅对早期 chromedp 方案有意义；直连方案不再使用）

### 配置

`config.yaml`：

```yaml
host: 127.0.0.1
port: 8317
auth-dir: ./auths
```

`auths/inception.json`（占位，无真实凭证；`type` 字段决定 executor 路由）：

```json
{"disabled": false, "type": "inception"}
```

### 验证

```bash
# 非流式
curl -s http://127.0.0.1:8317/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"inception-standard","messages":[{"role":"user","content":"What is 2+2? one word"}]}'
# → {"object":"chat.completion","choices":[{"message":{"content":"4",...}}]}

# 流式（真流式，逐帧到达）
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
├── go.mod / go.sum             # module inception-proxy；直连 bogdanfinn/tls-client v1.15.1 + fhttp v0.6.8；移除 chromedp/cdproto
├── config.yaml
├── auths/inception.json        # {"disabled":false,"type":"inception"}
├── cmd/inception-proxy/main.go # 装配：tls-client manager + core manager + cliproxy Builder + 守卫
└── internal/
    ├── browser/manager.go      # tls-client 单例直连（文件名历史遗留）：Chrome_146 指纹、
    │                            # /api/session 取 token、POST /api/chat（全量 FetchChat / 流式 FetchChatStream）
    └── inception/
        ├── executor.go         # InceptionExecutor，编排 fetch + translator；流式逐帧推 StreamChunk
        ├── transform.go        # init() 注册 OpenAI↔Inception 双向翻译器；openAIStreamChunker 生成流式 chunk
        └── sse.go              # SSE 行级解析：parseDeltaFromLine + isDoneLine，全量与流式共用同一规则
```

## SDK 适配的两个关键点

CLIProxyAPI SDK 设计围绕内置 provider，自定义 provider 需两处守卫处理「未知 provider」路径，见 `main.go` 注释：

1. **模型注册被清空**：`service_models.registerModelsForAuth` 对非内置 provider 走 `default` 分支，因无 plugin 模型而 `UnregisterClient` 清掉 `OnAfterStart` 的注册。`OnAfterStart` 后的 file watcher 初始枚举也会清一次。
   → 启动周期守卫（2s ticker）检测 `GlobalModelRegistry().ClientSupportsModel` 丢失即补注册。

2. **executor 被覆盖**：config reload 路径 `applyWatcherConfigUpdate` 用 `forceReplaceAuths:true`，对 inception auth 的 `default` 分支无条件注册内置 `OpenAICompatExecutor` 覆盖直连 executor（报 `missing provider baseURL`）。
   → 同一守卫检测 `coreManager.Executor("inception")` 非本实例时 `RegisterExecutor` 换回。

这两个守卫是对 SDK「无自定义 provider 一等支持」的必要适配，仅在检测到被清/被覆盖时才动作（reconcile 仅在 auth/config 变更触发，不持续）。

## 约束（用户确认）

- 服务**无鉴权**（本地/内网）；
- 流式**真流式**（上游 SSE 边收边推，逐帧 flush，打字机效果），`[DONE]` 由 SDK handler 自动追加；
- tls-client **单实例复用** + token 缓存（401 自动重取再重试一次）。
