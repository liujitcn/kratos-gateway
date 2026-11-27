# kratos-gateway

面向 kratos 体系（Go + kratos v3 + kratos-kit）的长期自研 API 网关：统一南北向入口，按路由分发到注册中心（Consul）中的后端服务，并提供负载均衡、灰度、限流等流量治理能力。

> 当前状态：**v1.0 已实现**（全部代码位于本仓库，测试覆盖单测 + 集成 + 竞态检测）。设计方案见 [docs/设计方案.md](docs/设计方案.md)。

## 为什么自研

| 候选 | 弃选原因 |
|---|---|
| go-kratos/gateway | kratos v2 血统、从未发版；中间件（cors/auth/ratelimit）与 kratos-kit 现有体系硬冲突 |
| Spring Cloud Gateway | JVM 运行时，与 Go 技术栈不符；仅借鉴 Route/Predicate/Filter 思想 |
| nginx / APISIX | 生产过渡备选；路由与发现配置手工维护，无扩展模型 |
| **自研** | 与 kit registry 工厂、config 体系一体；网关过滤器与业务中间件同源设计，冲突天然消解 |

## 核心设计（速览）

- **分层**：`Config → Router(Predicate) → Filter 链 → Selector(负载均衡) → Upstream(反向代理)`
- **服务发现**使用 Consul 健康查询订阅实例，复用 `kratos-kit` 的服务名规范和注册能力，配置与业务服务 registry.yaml 同构。
- **负载均衡**：复用 kratos v3 `selector` 独立包接口与 wrr 实现（平滑加权；round_robin 自建），不依赖 kratos transport（规避 balancer init 缺陷）
- **边界**：网关只做传输与路由治理；鉴权、审计、脱敏等业务语义留在服务侧

## 快速开始

```bash
export CONSUL_HTTP_ADDR=127.0.0.1:8500
# Consul 开启 ACL 时设置 CONSUL_HTTP_TOKEN。
make run CONF=configs APP_ENV=dev
# 等价命令：go run ./internal/cmd/server -c configs -e dev

curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
curl -i http://127.0.0.1:8080/api/v1/base/captcha
```

示例 `registry.yaml` 启用 Consul 注册与发现，启动前需有可达的 Consul。仅使用静态 URL 的部署可移除该文件，并保留静态路由。配置文件按基础文件、`*.{env}.yaml` 覆盖文件的顺序加载。`server.yaml` 配置共享 HTTP 服务和中间件，`registry.yaml` 配置注册发现，`gateway.yaml` 的 `gateway` 块配置目录聚合、连接排空和路由。

`gateway.routes` 支持本地热更新，路由列表在环境覆盖文件中按整表替换。路由解析或装配失败时继续使用上一个有效路由表。接入远程配置 Provider 后，启动时采用合并后的配置，后续路由变更通过重启加载。`gateway.server.stats: true` 可在重启后开放 `/gw-stats`。

网关默认不设置 `server.http.timeout`，以便任意路由透明转发 SSE 和 WebSocket。普通请求可通过路由级 `timeout` 过滤器设置超时。

`/readyz` 在目录首个快照发布且已发布的发现路由都有可用节点时返回成功。容器和负载均衡器可以用该端点检查接流状态。Consul 注册和 `health_check` 与 Admin 共用 kit 注册器，启用后使用标准 TCP 健康检查。收到停机信号时，Kratos 注销实例，随后网关停止接流并排空连接。

```bash
docker build -t kratos-gateway .
docker run -p 8080:8080 -e CONSUL_HTTP_ADDR=host.docker.internal:8500 kratos-gateway
```

bootstrap 默认在配置目录生成 `root.key`。容器使用只读配置挂载时，需要同时提供可读的根密钥或在 `key.yaml` 配置密钥 Provider。

## 代码结构

```text
api/proto/                   网关扩展配置契约
api/gen/go/                  由 Buf 生成的 Go 配置类型
internal/
├── biz/gateway/             路由装配、热更新、匹配与代理转发
├── cmd/server/              main.go、wire.go 和生成的 wire_gen.go
├── config/                  config.go 定义配置，init.go 注册扩展配置
├── data/                    Consul 服务目录、健康订阅和上游实例管理
├── module/                  module.go 装配网关和管理后台任务生命周期
└── server/                  init.go 创建 HTTP 服务，register.go 挂载网关
    └── middleware/          log.go 记录访问日志，metrics.go 采集指标
```

`make gen` 生成配置 Proto 和 Wire，`make check` 执行 Go 静态检查和测试，`make build` 构建网关二进制。

## 演进路线（当前全部已实现）

| 版本 | 范围 |
|---|---|
| v0.1 | 单服务转发（Consul 发现 + 路由 + 轮询 + SSE 透传 + 可观测） |
| v0.2 | 多服务路由（Method/Header 谓词 + 静态目标 + 路由级 Filter 链） |
| v0.3 | 服务自描述路由 + Weight 灰度 |
| v0.4 | 限流（令牌桶）/ 熔断（节点级被动）/ 重试（幂等安全） |
| v1.0 | 配置热更新 + Prometheus + 优雅停机 + 容器化 |

## 相关仓库

- [kratos-admin](https://github.com/liujitcn/kratos-admin) — 宿主业务（首个接入验证方）
- [kratos-kit](https://github.com/liujitcn/kratos-kit) — 基础组件（registry 工厂、config 结构复用方）

## License

[MIT](LICENSE)
