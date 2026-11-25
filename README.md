# kratos-gateway

面向 kratos 体系（Go + kratos v3 + kratos-kit）的长期自研 API 网关：统一南北向入口，按路由分发到注册中心（Consul）中的后端服务，并提供负载均衡、灰度、限流等流量治理能力。

> 当前状态：**设计阶段**（v0.1 未开工）。设计方案见 [docs/设计方案.md](docs/设计方案.md)。

## 为什么自研

| 候选 | 弃选原因 |
|---|---|
| go-kratos/gateway | kratos v2 血统、从未发版；中间件（cors/auth/ratelimit）与 kratos-kit 现有体系硬冲突 |
| Spring Cloud Gateway | JVM 运行时，与 Go 技术栈不符；仅借鉴 Route/Predicate/Filter 思想 |
| nginx / APISIX | 生产过渡备选；路由与发现配置手工维护，无扩展模型 |
| **自研** | 与 kit registry 工厂、config 体系一体；网关过滤器与业务中间件同源设计，冲突天然消解 |

## 核心设计（速览）

- **分层**：`Config → Router(Predicate) → Filter 链 → Selector(负载均衡) → Upstream(反向代理)`
- **服务发现**：复用 `kratos-kit/registry` 工厂（首版 Consul），配置与业务服务 registry.yaml 同构
- **负载均衡**：复用 kratos v3 `selector` 独立包接口与 wrr 实现（平滑加权；round_robin 自建），不依赖 kratos transport（规避 balancer init 缺陷）
- **边界**：网关只做传输与路由治理；鉴权、审计、脱敏等业务语义留在服务侧

## 演进路线

| 版本 | 范围 |
|---|---|
| v0.1 | 单服务转发（Consul 发现 + 路由 + 轮询 + SSE 透传 + 可观测） |
| v0.2 | 多服务路由（Method/Header 谓词 + 静态目标） |
| v0.3 | 服务自描述路由 + Weight 灰度 |
| v0.4 | 限流 / 熔断 / 重试 |
| v1.0 | 配置热更新 + Prometheus + 生产化 |

详细架构、接口设计、验收标准见 [docs/设计方案.md](docs/设计方案.md)。

## 相关仓库

- [kratos-admin](https://github.com/liujitcn/kratos-admin) — 宿主业务（首个接入验证方）
- [kratos-kit](https://github.com/liujitcn/kratos-kit) — 基础组件（registry 工厂、config 结构复用方）

## License

[MIT](LICENSE)
