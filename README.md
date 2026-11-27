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
# 本地运行（配置含 ${REGISTRY_TOKEN} 占位符，从环境变量展开；未设置时启动报错快速暴露）
export REGISTRY_TOKEN=xxx
go run ./cmd/gateway -conf configs/config.yaml

# 验证
curl http://127.0.0.1:8080/healthz     # 存活探针
curl http://127.0.0.1:8080/readyz      # 接流探针
curl http://127.0.0.1:8080/metrics     # Prometheus 指标
curl -i http://127.0.0.1:8080/api/v1/base/captcha   # 转发（响应头 X-GW-Upstream 标注实际命中节点）

# 容器化
docker build -t kratos-gateway . && docker run -p 8080:8080 -e REGISTRY_TOKEN=xxx kratos-gateway
```

滚动部署使用 `/readyz` 检查接流状态。启用服务目录时，网关等待首个目录快照发布。每条已发布的发现路由都须至少有一个匹配节点。静态 URL 不做连通性探测。容器健康检查默认使用 `/readyz`。自注册并通过 Consul 分发流量时，设置 `registry.consul.health_check: true`，负载均衡只选健康实例。停机时先摘流，再由网关注销并排空在途请求。

`/gw-stats` 默认关闭。设置 `server.stats: true` 并重启后，可以通过该端点查看路由表和上游命中统计。

## 代码结构

```
cmd/gateway/          入口：装配、热更新、优雅停机
internal/config/      YAML 配置加载、${VAR} 字符串展开、路由快照热更新
internal/route/       Route/Predicate/Filter 模型、Router（段前缀最长优先）、内置路由过滤器
internal/upstream/    Consul 健康订阅、UpstreamManager、负载均衡、节点级被动熔断
internal/forward/     ReverseProxy 转发（SSE 透传、X-GW-Upstream、幂等重试）
internal/catalog/     服务自描述路由聚合（Consul 元数据 gateway_routes）
internal/builder/     Config → 运行时路由表装配（含灰度权重区间分配）
internal/filter/      全局过滤器：Recovery / AccessLog / Metrics
internal/server/      HTTP 服务组装、/gw-stats、/metrics、/healthz、/readyz、优雅停机
```

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
