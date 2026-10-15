# 分布式短链平台

Spring Boot 3.4 + Java 21 + MySQL + Redis + Kafka + Docker

## 功能

| 功能 | 实现 |
|------|------|
| 短链生成 | 雪花 ID + Base62（11 位短码，无碰撞、免查重） |
| 短链跳转 | Redis 缓存 + 302 重定向，空值缓存防穿透 |
| 点击统计 | Redis 实时计数/HyperLogLog UV + Kafka 异步消费聚合落库日统计 |
| 限流 | Redis + Lua 固定窗口，创建 5 次/分、跳转 120 次/分（按 IP） |
| 过期控制 | 创建时可指定 expireAt，过期后跳转返回 410 |

## 快速启动（Docker 一键拉起全部组件）

```bash
cd shortlink
cp .env.example .env   # 填入本地密码（.env 不入库）
docker compose up -d --build
```

应用: http://localhost:8082   MySQL: 13306   Redis: 16379   Kafka: 19092

> 架构图、Redis / Kafka 设计详解与核心考点：见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## API

```bash
# 1. 创建短链
curl -X POST http://localhost:8082/api/v1/links \
  -H "Content-Type: application/json" \
  -d '{"longUrl":"https://www.example.com/some/very/long/path?query=1"}'
# => {"code":"...","shortUrl":"http://localhost:8082/...",...}

# 2. 跳转（302）
curl -i http://localhost:8082/{code}

# 3. 查询统计（总点击/今日点击/UV/近7天明细）
curl http://localhost:8082/api/v1/stats/{code}
```

## 监控指标

Prometheus 抓取端点 `GET /actuator/prometheus`（含 JVM / Tomcat / HikariCP / HTTP 指标）：

```bash
curl -s http://localhost:8082/actuator/prometheus | grep '^shortlink_'
# shortlink_redirect_total{outcome="hit"}            跳转结果（hit/miss/null_placeholder/not_found/expired，互斥）
# shortlink_ratelimit_rejected_total{scene="redirect"} 被限流拒绝数
# shortlink_consume_lag{partition="0"}               消费滞后，基于 DB 位点而非 Kafka LAG
```

指标清单与接入方式见 [docs/ARCHITECTURE.md §9.4](docs/ARCHITECTURE.md#94-可观测性指标出口)；AIOps 平台侧的接入与实施记录见 [docs/AIOPS_INTEGRATION.md](docs/AIOPS_INTEGRATION.md)。

## 架构要点

- 发号器：Snowflake（41bit 时间戳 + 10bit 机器号 + 12bit 序列），多实例部署时用 SHORTLINK_WORKER_ID 区分机器号
- 读多写少：跳转只读 Redis（miss 回源 MySQL 并回填），缓存空值 5 分钟防穿透
- 统计削峰：点击事件投 Kafka（以 code 为 key 保证同短链有序），消费端批量拉取、内存聚合后按 (code, date) ON DUPLICATE KEY UPDATE 原子累加，一批点击只产生一次 UPSERT
- 302 而非 301：301 被浏览器永久缓存会导致后续点击不到达服务端，统计严重偏低

## 本地开发（不用 Docker）

本地启动 MySQL/Redis/Kafka 后（数据源密码通过 `DB_PASSWORD` 环境变量传入）：

```bash
export DB_PASSWORD=你的密码
mvn spring-boot:run
```

默认连接 localhost:3306 / 6379 / 9092。
