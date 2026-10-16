# AIOps 自愈演进 — shortlink 侧运维接口 TODO

> 这是 `docs/AIOPS_INTEGRATION.md` 的**续篇**，聚焦一个具体问题：
> 当 AIOps 平台要把自愈从「docker 重启容器」（物理层）演进到「调用应用内置开关」（应用层）时，
> shortlink 侧需要新增哪些运维管理接口、设计上有哪些约束。

## 背景

阶段 5（commit `1e8d8dd`）已经实现真实 docker 自愈闭环——`docker stop shortlink-redis` → 告警 → `Root cause: Redis 不可达 (confidence 0.81)` → 真实 `docker restart shortlink-redis` → 验证恢复 → 业务 302，从告警到验证恢复 6 秒。

但 `SHORTLINK_PLAYBOOKS` 里有相当一批动作被标 L2 人工，且 `argv_template=None`——这是阶段 5 的**自我清单**，不是临时占位：

| 动作 | 标 L2 的原因 |
|---|---|
| `rate_limit` | 短链的限流是 Redis + Lua 固定窗口，阈值写死在配置里，无运行时开关 |
| `block_ip` | 短链应用没有实现 IP 黑名单机制 |
| `circuit_breaker` | 短链应用没有实现熔断机制（读 Redis 失败会直接抛异常） |
| `tune_threads` | Tomcat 线程池参数需改配置并重启 |
| `scale_app` | 水平扩容必须人工：雪花 ID 的 `SHORTLINK_WORKER_ID` 需每实例唯一 |
| `heap_dump` | jmap 会 STW 且产出大文件，需人工评估时机 |
| `rollback` / `rollback_config` | 需人工确认版本与影响 |

详见 `AIOPS_INTEGRATION.md` §5.3。

## 系统现状（已查证）

通过对 shortlink 仓库代码的探查（`RateLimitService` / `LinkService` / `application.yml` / 消费位点 / 雪花 ID），确认全部对应缺失：

- **限流**：`RateLimitService.check(scene, identity, limit, windowSeconds)` 通过 `@Value` 注入，构造期后无 setter；无 `@RefreshScope` / Nacos 监听
- **Redis 降级**：`LinkService.resolve:91` `redis.opsForValue().get(key)` 裸调用，Redis 挂即 500
- **Actuator**：`application.yml:62-72` 只暴露 `health,info,prometheus,metrics` 四个端点；无 `shutdown`、无独立管理端口
- **自定义管理接口**：`controller/` 目录只有 `LinkController` / `RedirectController` / `StatsController` 三个，全是业务接口
- **消费位点**：`@KafkaListener` 自动启动，`ClickStatsConsumer` 没有任何 pause / reset 钩子
- **缓存清理**：`LinkService` 无 `evict(code)` / `warmUp(code)`；只能 `redis-cli DEL sl:link:xxx`
- **Worker ID**：`SnowflakeIdGenerator` `@Value("${shortlink.worker-id:1}")`，必须手工改环境变量

## 演进方向：从「物理层」到「应用层」自愈

按优先级排，**全部以「解锁当前 L2 人工动作降为 L0 自动」为收益标准**。

### P0 — 立即可做，改动量小

#### 1. `POST /admin/ratelimit` — 运行时调整限流阈值

- **当前阻塞**：`rate_limit` 动作（`SHORTLINK_PLAYBOOKS:266`）永远是 L2 人工
- **端点契约**：
  ```
  POST /admin/ratelimit
  Body: { "scene": "redirect"|"create", "limit": int, "windowSeconds": int }
  Auth:  X-Admin-Token
  Returns: 200 { "old": ..., "new": ... } | 400 验证失败
  ```
- **实现要点**：`RateLimitService` 改用 Redis 键读取（`sl:rl:config:redirect:limit`），启动时 fallback 到配置；AIOps 写入时更新 Redis 键
- **收益**：限流告警（`ratelimit_reject_burst`）触发后可自动收口（临时调高 / 调低）
- **改动量**：小（service 内改 ~20 行 + 1 个 Controller）

#### 2. `DELETE /admin/cache/{code}` — 按短码清缓存

- **当前阻塞**：数据库回填出错导致脏缓存时，唯一恢复手段是重启整容器（爆炸半径 0.35）
- **端点契约**：
  ```
  DELETE /admin/cache/{code}
  Auth:  X-Admin-Token
  Returns: 204 | 404 短码不存在
  ```
- **实现要点**：`LinkService` 加 `evict(code)`，删 `sl:link:{code}`；可选同时清空值占位
- **收益**：把"脏缓存"类故障的爆炸半径从 0.35（重启容器）降到 ~0
- **改动量**：极小（service 加 ~5 行 + 1 个 Controller）

### P1 — 解锁更细粒度的自愈

#### 3. `POST /admin/consumer/{pause,resume,reset-offset}`

- **当前阻塞**：`restart_consumer` 是 L1 半自动（要审批通过也不自动执行），而积压恢复往往只要重置位点
- **端点契约**：
  ```
  POST /admin/consumer/pause         → 暂停 @KafkaListener
  POST /admin/consumer/resume        → 恢复
  POST /admin/consumer/reset-offset  → Body: { groupId, topic, partitionNo, offset }
                                       调 ConsumerOffsetRepository.upsertOffset
  ```
- **实现要点**：`KafkaListenerEndpointRegistry.stop()` / `start()` + 注入 `ConsumerOffsetRepository`
- **收益**：消费积压类故障可定位到 partition 级别重置，不必重启整应用
- **改动量**：中（要管理 Kafka 生命周期 + 同步位点表）

#### 4. `POST /admin/cache/fallback` — Redis 降级开关

- **当前阻塞**：Redis 挂时 `redirect_5xx_rate` 告警靠"重启 Redis 容器"恢复（爆炸半径 0.25）；如果应用层能切到 DB 读，告警可自动消失
- **端点契约**：
  ```
  POST /admin/cache/fallback
  Body: { "enabled": bool }
  ```
- **实现要点**：`LinkService.resolve` 加 `if (fallbackEnabled) ... else ...` 分支；`StatsService` 同理
- **收益**：Redis 类告警的恢复时间从 ~15s（重启容器）降到 ~0s（开关切换），且不中断在途请求
- **改动量**：中（LinkService read 路径 try Redis else DB；空指针与一致性要重新设计）

### P2 — 收尾

#### 5. `POST /actuator/shutdown` + Dockerfile `exec` 形式

- **当前阻塞**：`exposure.include` 列表里没 `shutdown`；Dockerfile `ENTRYPOINT` 是裸 `java -jar`，没接 SIGTERM
- **端点契约**：开箱即有（`POST /actuator/shutdown`），仅需在 `application.yml` 暴露
- **Dockerfile 改动**：
  ```dockerfile
  ENTRYPOINT ["java", "-jar", "app.jar"]   # exec 形式 → 接 SIGTERM
  ```
- **收益**：K8s 滚动升级 / AIOps 主动重启时不丢在途请求
- **改动量**：极小（2 处）

## 部署架构（不属应用代码改动）

#### `scale_app` — K8s StatefulSet + `pod.ordinal`

- **当前阻塞**：多实例部署必须手工改 `SHORTLINK_WORKER_ID` 环境变量
- **正解**（不属于应用代码改动）：K8s StatefulSet 用 `pod.ordinal` 自动注入
  ```yaml
  env:
    - name: SHORTLINK_WORKER_ID
      value: "{{ pod.ordinal }}"
  ```
- **收益**：`scale_app` 从 L2 人工降为 L0 自动（前提是部署在 K8s 上）
- **前提**：短链需迁移到 K8s（当前是 docker compose）

## 设计约束（如果决定做）

三件必须：

1. **独立管理端口**：`management.server.port: 8081`，与业务 8080 隔离
   - 运维接口被压垮或被误用时不污染业务路径
   - 监控/告警系统对 8080 扫描时不会触碰到管理接口
2. **鉴权**：至少要有下面之一
   - mTLS（最严，但运维成本高）
   - 共享密钥：`X-Admin-Token` 头，值从环境变量 `ADMIN_TOKEN` 读，启动时校验非空
   - 兜底：IP 白名单（仅内网网段）
3. **审计 + 幂等**：
   - 每个动作写一条审计日志（`who/when/argv/result`）→ 写 `t_admin_audit` 表或日志框架
   - POST 类接口必须幂等：请求带 `Idempotency-Key` 头，24 小时内同 key 不重复执行
   - 避免 AIOps 重试或网络抖动导致重复执行（重启 Redis 一次是好的，重启 5 次就是事故）

可选但推荐：

4. **同步 vs 异步**：P0/P2 类操作应同步返回（< 200ms），P1 的 reset-offset 涉及 Kafka 重平衡，应异步返回任务 ID，状态可查
5. **回滚**：每次变更类操作要返回「原值」，便于 AIOps 后续回退（例：调高限流后告警消失了，10 分钟后 AIOps 调回原值）

## 决策点

需要回答的：

- [ ] 是否要做这一整套？如果目的是降低 MTTR，P0 两个就够（限流调阈 + 清缓存）
- [ ] 优先做哪些？建议从 P0 起（`rate_limit` 触发频次最高，收益最直接）
- [ ] 接口用同步还是异步？默认建议：变更类同步（< 1s），重置类异步
- [ ] 鉴权选 mTLS 还是共享密钥？内网部署共享密钥足够，公网必须 mTLS
- [ ] 是否同时上 K8s？如果是，优先把 `scale_app` 解锁（走 StatefulSet 注入 worker-id）

短期不改也行——**目前阶段 5 的边界「该自动的自动、该人工的让 AIOps 老老实实说『本系统不执行』」就是负责任的实现**。

## 关联

- `AIOPS_INTEGRATION.md` §5.3 阶段 5 分级原则
- `AIOPS_INTEGRATION.md` §5.6 审批闸门修复
- `AIOPS_INTEGRATION.md` §5.7 README 逐条核对（爆炸半径硬编码、L2 示例动作不存在两个待办）
- AIOps 侧 `HealProposeAgent` / `HealExecuteAgent` 见 `python/agents/heal_agent.py`
