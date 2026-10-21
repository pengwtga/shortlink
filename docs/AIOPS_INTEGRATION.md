# AIOps 集成实施记录

本文档记录本项目（shortlink）作为**被监测对象**，与 AIOps 平台（fork 自 `bcefghj/multi-agent-aiops`）的二次开发协作记录。短链侧改造（阶段 1，Actuator + 业务指标）详见 [ARCHITECTURE.md §9.4](ARCHITECTURE.md#94-可观测性指标出口)，本文聚焦 **AIOps 侧**改动。

> **贯穿全文的关键决策**：二次开发的改动**主要在 AIOps 侧**完成；短链侧只需暴露 `/actuator/prometheus`、遵循 Prometheus 指标命名规范即可，对短链业务代码零侵入。

---

## 阶段总览

| 阶段 | 范围 | AIOps 侧 commit | 关键产出 |
|------|------|-----------------|----------|
| 1（短链侧）| Actuator + 业务指标 | — | `shortlink_redirect_total`、`shortlink_consume_lag` 等 |
| 2 | Prometheus scrape job | `9f35d31` | AIOps Prometheus 开始抓 `host.docker.internal:8082` |
| 3 | 真实采集 + 双层告警 | `6f7d6a4` | `collectors/` + `MonitorAgent` 双层路径 |
| 4 | RCA 换短链拓扑与故障模式库 | `6052b9f` `cde5327` | 根因判定由证据驱动（PromQL 探针） |
| 5 | Heal 换真实 docker 执行器 | `1e8d8dd` | 真实自愈闭环（告警 → 重启 Redis → 验证恢复） |
| 5 修 | 自愈拆提议/执行两段 | `dd79d05` | 审批成为真正的闸门（详见 §5.6） |
| fix | `IncidentState.metadata` 字段 | `293c516` | 修复上游 metadata 通道不可用的硬 bug |
| chore | `.gitignore` | `dcc08a0` | 排除构建产物 / IDE / OS 杂项 |

八个 commit 已推送到 fork：`git@github.com:pengwtga/multi-agent-aiops.git`；`upstream` 仍指向 `bcefghj/multi-agent-aiops`，便于后续同步上游。

---

## 阶段 2：Prometheus 接入（commit `9f35d31`）

在 `python/config/prometheus.yml` 新增 `shortlink-app` 抓取作业：

```yaml
  - job_name: 'shortlink-app'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets: ['host.docker.internal:8082']
        labels:
          service: 'shortlink'
          env: 'local'
```

`host.docker.internal` 是 Docker Desktop 提供的宿主机回环别名（与既有 `aiops-agents` 作业同源机制），无需修改 `docker-compose.yml` 的网络配置。

**已验证**：抓取目标 `up`；PromQL 可查 `shortlink_redirect_total{outcome="hit"}`、`shortlink_consume_lag{partition}` 等，完整 label 链路（`application` / `service` / `env` / `instance` / `job`）。

---

## 阶段 3：MonitorAgent 接真数据（commit `6f7d6a4`）

这是工作量最大的一阶段。核心动机：原 MonitorAgent 只能处理「手动喂入的单个指标样本」，未与任何真实数据源对接——既不读 Prometheus，且在缺 `metric_data` 时直接 fallback 到伪造的 `order-service` CPU 告警。

### 3.1 双层告警设计

| 层 | 判定方式 | 触发条件 | 冷启动 | 覆盖故障 |
|----|----------|----------|--------|----------|
| **静态业务规则** | 采集器按短链业务阈值预判 | `up==0` / 5xx rate > 0.5/s / consume_lag > 1 万 / 限流拒绝 > 1/s | 立即 | 已知故障模式（`consume_lag` 假 lag 语义、5xx 业务含义） |
| **统计异常检测** | 复用原 3-sigma / EWMA / Isolation Forest 多算法投票 | ≥ 2/3 算法判异常 | 需 ≥ 5\~20 历史样本（5\~10 min） | 未知突变（压测触发的 `tomcat_busy_threads` 异常即被此层捕获） |

**为什么必须两层**：

- 统计检测需要历史样本（3-sigma ≥ 10、Isolation Forest ≥ 20），冷启动窗口内不可用；静态规则立即生效
- 静态规则编码的是运维知识（如「`consume_lag` 必须用 DB 位点算，用 broker LAG 会误报」），这是统计模型推断不出的
- 两层天然互补：`consume_lag` 常态是常量 0，标准差为 0，统计层对「0→30000」跳变**故意不报**（`std==0` 保护），正好由静态规则兜住；而压测把线程池顶上去这类渐变，静态阈值未覆盖，由统计层抓

两层共用 `MonitorAgent.AlertFingerprint`（5 分钟窗口）去重——同一规则持续故障时每 5 分钟重通知一次，符合通用告警行为。两条路径的告警都写入自观测列表并带 `source` 字段区分（初版遗漏统计层记账，导致状态端点对统计告警「失明」，已补齐）。

### 3.2 新增 / 修改模块

**`python/collectors/prometheus_collector.py`**（核心新增）：

- `PrometheusClient`：std-lib `urllib` + `asyncio.to_thread` 实现 instant query，**零新增依赖**——venv 只有 `fastapi / uvicorn / pydantic / numpy / scikit-learn` 等，没有 `httpx` / `requests`
- `AlertRule`（dataclass）：`name / promql / comparator / threshold / severity / target_service / metric_name / description`
- `build_shortlink_rules(job)`：返回 4 条规则
- `BASELINE_QUERIES`：3 条基线指标（`consume_lag` / `app_cpu_usage` / `tomcat_busy_threads`）持续投喂统计层积累历史
- `MonitorLoop`：`run_forever()` 主循环，**单周期异常只记日志不退出**（保活优先）；`status()` 与 `get_metrics_text()` 供 FastAPI 端点使用
- `_record_alert(state, source)`：规则层与统计层共用的告警记账入口

**`python/agents/monitor_agent.py`**（`process()` 改双层）：

```python
rule_violation = state.metadata.get("rule_violation")
if rule_violation:
    alert = self._build_rule_alert(...)              # 静态规则路径
else:
    is_anomaly, score = self._detect_anomaly(...)    # 统计检测路径
    if not is_anomaly:
        return state
    alert = self._build_statistical_alert(...)

# 两条路径共用指纹去重
fingerprint = self._fingerprint.compute(alert.alert_name, target_service, labels)
if self._fingerprint.is_duplicate(fingerprint):
    return state
```

**`python/api/main.py`**：

- `lifespan` 启停 `MonitorLoop`（`asyncio.create_task` + 优雅 `cancel`）
- 新增 `GET /api/v1/monitor/status`：规则集、周期数、已触发告警（最近 20 条，含 `source`）
- 新增 `GET /metrics`：手写 Prometheus exposition 文本（`aiops_monitor_cycles_total` / `_alerts_total` / `_rules`），让 `prometheus.yml` 的 `aiops-agents` 抓取作业 up

**`python/config/settings.py`**：新增 `monitor_enabled: bool = True`（默认开，可临时关停采集）。

### 3.3 验证证据

故障注入：`docker stop shortlink-redis` + 600 次压流量 → 跳转从 302 变 500。

| 验证项 | 期望 | 实际 |
|--------|------|------|
| 规则层 | `redirect_5xx_rate` critical 触发 | value=0.511 > 阈值 0.5 ✓ |
| 规则层 | `ratelimit_reject_burst` medium | value=1.97 > 1.0 ✓ |
| 统计层 | `anomaly_tomcat_busy_threads` high | 高并发压测时真实触发 ✓ |
| 统计层 | `anomaly_app_cpu_usage` medium | CPU 抬升时触发 ✓ |
| 全链路 | Monitor → RCA → Heal → Change | 四 Agent 全部执行，状态 `resolved` ✓ |
| 告警收敛 | 5 min 内同规则重复触发被抑制 | 日志确认 `Alert suppressed (duplicate)` ✓ |
| 自愈验证 | `docker start shortlink-redis` 后恢复 | 跳转 302 / `actuator/health` UP ✓ |
| Prometheus 目标 | 三个全 up | `aiops-agents` / `prometheus` / `shortlink-app` 全 up ✓ |

完整流水线一次实测（`incident=97659c85-...`，cycle 26）：

```
MonitorAgent  Alert fired: redirect_5xx_rate [critical]
RCAAgent      Root cause: 上游服务请求量激增导致 CPU 过载 (confidence: 0.42)
HealAgent     DRY-RUN: kubectl annotate svc/shortlink-app rate-limit=100 -n production --overwrite
              Action: rate_limit [L0] blast_radius=0.01
ChangeAgent   APPROVED: rate_limit risk=0.113 approver=auto-system
Orchestrator  Workflow completed: status=resolved
```

---

## 阶段 4：RCA 换短链拓扑与故障模式库（commit `6052b9f`）

### 4.1 动机

阶段 3 的验证暴露了一处**逻辑错误**：注入 Redis 故障后，RCA 报的根因是「上游服务请求量激增导致 CPU 过载」，与事实（Redis 不可达）完全不符。根因有两层：

- `MOCK_SERVICE_TOPOLOGY` 里根本没有 `shortlink-app`，服务不存在 → `affected_services` 退化成单节点
- `redirect_5xx_rate` 不匹配任何 `FAULT_PATTERNS` 的键 → 回落到默认的 `high_cpu_usage` 候选

但**换掉常量表只解决了一半问题**。原实现里，似然 `P(Evidence|Cause)` 是由 `evidence_pattern` 静态拍定的，`base_probability` 先验因此直接决定结论——也就是说它只会回答「哪类故障最常见」，而不是「这次到底是谁」。

### 4.2 改动

| 项 | 改动 |
|----|------|
| 拓扑 | `MOCK_SERVICE_TOPOLOGY` → `SHORTLINK_TOPOLOGY`：shortlink-app / click-consumer / mysql / redis / kafka。`runs_on` 用 docker 容器名（非 K8s Pod），供阶段 5 执行器直接消费。消费端独立建模：只依赖 kafka + mysql，与 app 的依赖链区分开 |
| 故障模式库 | 7 类告警的短链专属候选；未命中时用短链语义的兜底候选，**不再回落原项目的通用 CPU 候选** |
| 证据探针 | 候选根因带 `verify`（PromQL）。实测成立 → 似然 0.9；不成立 → 0.05；探针无数据 → 0.5 |
| 推理 | `_bayesian_inference` 改 async（`verify` 需回查 Prometheus）；`Orchestrator` / `api/main.py` 注入 `PrometheusClient` |

### 4.3 关键设计：先验决定「谁更常见」，证据决定「这次是谁」

以 `redirect_5xx_rate` 为例，四个候选靠探针互相证伪：

| 候选根因 | 先验 | 探针（判据） | Redis 真挂时 |
|----------|------|--------------|--------------|
| Redis 不可达 | 0.45 | `rate(shortlink_redirect_total[2m]) < 0.05`（成功跳转吞吐归零） | 成立 → 0.81 ← 胜出 |
| MySQL 不可达 | 0.25 | `rate(hikaricp_connections_timeout_total[2m]) > 0` | 不成立 → 0.025 |
| 流量激增 | 0.20 | `rate(http_server_requests_seconds_count[2m]) > 20` | 不成立 → 0.02 |
| 近期部署缺陷 | 0.10 | 无探针（无法用指标证伪，只能靠变更记录） | 0.10 |

「成功跳转吞吐归零」这个判据之所以有效，是因为告警本身（5xx rate > 0.5/s）已经保证了**有流量在打**，此时成功数为 0 就只能是依赖挂了，而不是没人访问。

### 4.4 过程中修掉的一个探针设计缺陷

MySQL 候选最初用「命中请求仍成功」作判据。**这个判据在健康状态下同样成立**，等于无法证伪——会导致系统在正常状态下被误判为 MySQL 故障。已改用 HikariCP 连接获取超时这个一手信号。

### 4.5 验证

`python/tests/test_rca_shortlink.py` 用桩证据提供者注入探针实测值，**不依赖 Docker 与 Prometheus**，可离线运行：

```bash
cd python && PYTHONPATH=. python tests/test_rca_shortlink.py
```

| 场景 | 注入的探针值 | 期望 | 结果 |
|------|--------------|------|------|
| Redis 不可达 | 成功吞吐 0、DB 超时 0、总请求 8 | 判 Redis | 置信度 0.81 ✓ |
| MySQL 不可达 | 成功吞吐 4、DB 超时 2 | 判 MySQL | 0.45 ✓ |
| 流量激增 | 成功吞吐 30、总请求 60 | 判流量 | 0.36 ✓ |
| 健康状态误报 | 成功吞吐 5、DB 超时 0、总请求 3 | 低置信度、不硬编错因 | 0.10 ✓ |
| Tomcat 线程池打满 | 线程占用率 0.85 | 判线程池（`anomaly_` 前缀剥离生效） | ✓ |
| 依赖链 | — | 取自短链拓扑 | ✓ |

其中「健康状态误报」是特地设计的负例：当证据把所有已知根因都证伪时，系统应保持**低置信度**而不是硬编一个错因——这是证据驱动相对先验驱动的关键收益。

### 4.6 端到端验证（真实 Prometheus + 真实故障）

故障注入：`docker stop shortlink-redis` → 重启应用使连接快速失败（原因见 §4.7）→ 压流量制造持续 5xx。

```
Alert fired: redirect_5xx_rate [critical]          5xx 速率 13.46/s
探针 成立:   Redis 不可达   实测=0.0000 (lt 0.05)   ← 成功跳转吞吐归零
探针 不成立: MySQL 不可达   实测=0.0000 (gt 0.0)
探针 不成立: 流量激增       实测=6.7896 (gt 20.0)
Root cause: Redis 不可达：读路径 sl:link:{code} 直接抛异常，无降级逻辑
            (confidence 0.81) → [restart_redis, check_dependency]
HealAgent:  No playbook for action: restart_redis   ← 阶段 5 缺口，优雅降级未崩溃
```

与阶段 3 同一故障的判定对比：

| | 阶段 3（改前） | 阶段 4（改后） |
|---|---|---|
| 根因 | 上游服务请求量激增导致 CPU 过载 ❌ | Redis 不可达 ✅ |
| 置信度 | 0.42 | **0.81**（与桩测试预测值一致） |
| 建议动作 | `rate_limit`（且是假 kubectl 命令） | `restart_redis` / `check_dependency` |

恢复 Redis 后：跳转 302、`actuator/health` UP、三个 Prometheus 抓取目标全 up。

### 4.7 验证中发现的新缺口：依赖「挂起」不产生 5xx

验证时先按阶段 3 的方式直接 `docker stop shortlink-redis`（应用已运行一段时间、连接池里有存量连接），结果请求**不是返回 500 而是挂住**：

```
单次请求:     http=000 耗时=40.0s（超时无响应）
5xx 速率:     0.0095/s    ← 几乎为零
redirect 吞吐: 0
tomcat 忙线程: 2
```

原理：连接已建立但 Redis 不再响应，请求卡在读写等待上，**走不到「返回 500」那一步**；而 Micrometer 只在请求完成时记录 status，挂起的请求不计入任何状态码。

后果：**`redirect_5xx_rate`（5xx > 0.5/s）对这类故障完全失效**。而挂起比快速失败更危险——线程会被慢慢占满，却不产生任何 5xx 信号。本轮是靠「Redis 已停时重启应用」（连接被拒 → 立即 500）才制造出 5xx 的。

> **✅ 已在阶段 5 修复**（详见 §5.8）：新增 `dependency_hang` 告警规则，
> 改用「Tomcat 连接堆积 + 成功跳转吞吐归零」这两个**占用类**信号捕获挂起。
> 实测该故障下 RCA 置信度 **0.99**，审批 → 执行完成 **5.5 秒**。

这说明规则集缺一类**基于延迟 / 吞吐**的判据，例如：

- 成功吞吐骤降：`rate(shortlink_redirect_total[2m])` 长期为 0，而总请求速率不为 0
- p99 延迟：基于 `http_server_requests_seconds` histogram 的 quantile
- 线程池占用率持续高位：`tomcat_threads_busy_threads / tomcat_threads_config_max_threads`

此项留待后续阶段（不在阶段 4 范围内）。

---

## 阶段 5：Heal 换真实 docker 执行器（commit `1e8d8dd`）

### 5.1 动机

阶段 4 把根因判对了，但自愈仍是假的：HealAgent 输出 `kubectl annotate svc/shortlink-app rate-limit=100 -n production`——短链是 docker compose 部署，没有 K8s；且动作本身也不对（限流对 Redis 不可达无效）。更根本的是 `_execute_remediation` 直接 `return SUCCESS`，压根没执行。

### 5.2 改动

| 项 | 改动 |
|----|------|
| 修复方案库 | `REMEDIATION_PLAYBOOKS`（K8s）→ `SHORTLINK_PLAYBOOKS`（docker）：`restart_redis` / `restart_mysql` / `restart_container` / `restart_consumer` + 三个只读诊断动作 |
| 执行器 | 新增 `DockerExecutor`：真实执行。只认 `docker`、子命令走白名单（`restart`/`start`/`exec`/`inspect`/`ps`/`logs`/`compose`）、永远用 argv 列表不用 `shell=True`、超时强制 kill |
| 执行后验证 | 重启类动作轮询 `docker inspect` 确认容器回到 healthy |
| 动作选择 | `_select_best_action` 改为尊重 RCA 排序（原实现用固定全局优先级，曾把 Redis 故障选成 `rate_limit`） |
| dry_run 语义 | 修正为「True 时只校验不执行」（原实现反而在 `dry_run=True` 时执行） |
| 审批风险模型 | `ChangeAgent` 的 `OPERATION_RISK` / `CRITICAL_SERVICES` 换成短链动作与服务，否则新动作落到默认 0.5 高风险会被拦 |
| 爆炸半径上限 | `blast_radius_max_percent` 0.2 → 0.5：容器重启的爆炸半径天然大于 K8s 重启单个 Pod，0.2 会把所有重启动作降级成人工 |

### 5.3 关键设计：宁可只提建议，也不伪造执行

只有「**幂等 + 可验证 + 影响可控**」的动作才允许 L0 自动执行：

| 级别 | 动作 | 理由 |
|------|------|------|
| **L0 自动** | `restart_redis`、`check_dependency`、`check_mysql`、`check_t_consumer_offset` | 幂等（重启可重复）、可验证（`docker inspect` 查健康）、只读诊断无副作用 |
| **L1 半自动** | `restart_container`、`restart_consumer`、`restart_mysql` | 会中断在途请求 / 影响写入，需人确认 |
| **L2 人工** | `scale_app`、`rate_limit`、`block_ip`、`circuit_breaker`、`tune_threads`、`heap_dump`、`rollback` 系列 | 见下 |

L2 里有一类是**短链应用根本没有对应机制**的：`rate_limit`（阈值写死在配置里，无运行时开关）、`block_ip`（无 IP 黑名单）、`circuit_breaker`（无熔断，读 Redis 失败直接抛异常）。对这些动作，`argv_template` 设为 `None` 并附上 `manual_reason`——**明确告诉人「本系统不执行，需要你来做」，而不是伪造一个执行结果**。

`scale_app` 标 L2 是刻意的：水平扩容时雪花 ID 的 `SHORTLINK_WORKER_ID` 必须每实例唯一，自动扩容会导致发号碰撞（重复短码）。

### 5.4 端到端自愈闭环验证

`AIOPS_HEAL_DRY_RUN=false` + 真实 `docker stop shortlink-redis`：

```
14:45:33 Alert fired: redirect_5xx_rate [critical]
14:45:33 探针 成立: Redis 不可达 实测=0.0000 (lt 0.05)
14:45:33 Root cause: Redis 不可达 (confidence 0.81) → [restart_redis, check_dependency]
14:45:33 DRY-RUN: docker restart shortlink-redis
14:45:39 执行结果: SUCCESS: shortlink-redis | 执行后验证: shortlink-redis 已恢复 (health=healthy)
14:45:39 Action: restart_redis [L0] blast_radius=0.25
14:45:39 ChangeAgent APPROVED: restart_redis risk=0.258 approver=auto-system
```

从告警到验证恢复 **6 秒**；业务侧跳转 500 → 302、`actuator/health` UP。注入的故障是真实 `docker stop`，恢复也是真实 `docker restart`——闭环里没有任何模拟。

测试 `tests/test_heal_docker.py` **11/11**：命令白名单、拒绝 `docker rm/rmi/system/volume`、无 shell 注入（断言注入串不会落地成文件）、人工动作不可执行且说明理由、动作映射到正确容器、只读诊断爆炸半径为零、动作选择尊重 RCA 排序、dry_run 不执行、真实只读命令可执行。

### 5.5 验证中发现的问题：审批发生在执行之后

日志里 `ChangeAgent APPROVED` 的时间戳（14:45:39.538）**晚于** `执行结果`（14:45:39.537）——因为编排器的 DAG 是 `monitor → rca → heal → change`，Heal 先执行、Change 后审批。

后果：**L1/L2 动作的审批形同橡皮图章**。它并不阻止执行，只是在执行后补一条审计记录；而 L1 动作当前本来就不会执行（只有 L0 会执行），于是出现了「审批通过但没有执行」的怪状态。

要真正让审批成为闸门，需要把 Heal 拆成「提议」与「执行」两段：`heal_propose`（产出 HealEvent，永不执行）→ `change`（审批）→ `heal_execute`（仅在审批通过后执行）。

### 5.6 阶段 5 修复：审批成为真正的闸门（commit `dd79d05`）

按 §5.5 的方案改：

- `HealAgent` 拆成 `HealProposeAgent`（只提议，永不执行）与 `HealExecuteAgent`（仅审批通过 + L0 才执行）
- DAG 改为 `monitor → rca → heal_propose → change → heal_execute`；`heal_execute` 的入边条件是「已有审批且通过」，未通过则节点 SKIPPED
- 执行 Agent 内部还会**再校验一次**审批状态与级别 —— 即使有人改了图，也绕不过闸门
- 提议与执行共享同一个 `CircuitBreaker`：否则两边各记一套失败计数，状态不一致
- 待执行的 argv 随 `HealEvent.action_params` 流转，执行阶段直接用，不重新推导（可审计）
- 事件流分开：提议发 `aiops.events`，只有真正要执行的命令才发 `aiops.commands`
- `ChangeAgent` 去掉「审批通过即 `resolved`」，改为 `approved`；`resolved` 只在执行成功并通过执行后验证后才置。新增 `awaiting_human` / `heal_failed` 状态，区分「等人工」与「执行失败」

端到端验证：

```
15:34:00.227 [HealProposeAgent] 提议: restart_redis [L0] blast_radius=0.25
15:34:00.227 [ChangeAgent]     APPROVED: restart_redis risk=0.258          ← 审批在执行之前
15:34:06.015 [HealExecuteAgent] 执行结果: SUCCESS | 执行后验证: 已恢复 (health=healthy)
15:34:06.015 Workflow completed: status=resolved                          ← 真修复才 resolved
```

测试 18/18（新增 8 条审批闸门用例：提议绝不执行、未审批不执行、审批被拒不执行、L1 审批通过也不自动执行、无命令动作不执行、审批通过+L0 才执行、执行失败不置 resolved、dry_run 不执行）。

### 5.6.1 一个设计选择：L0 也要走审批（为什么不「直接执行」）

原项目 README 写「L0（全自动）… 爆炸半径 < 5%，**直接执行**」，乍看与这里的
「L0 也要审批」矛盾。澄清两点：

**一、原项目 L0 也不是真的「直接执行」。**

改动前 `_execute_remediation` 的最后一行是：

```python
return f"SUCCESS: {playbook['description']} completed for {service}"
```

——没有任何子进程调用。所以原项目 L0 是「**假执行 + 事后 ChangeAgent 补审计**」，
两头都不真。本项改的是「**真执行 + 真审批（预先）**」。

**二、加了闸门，但「全自动」语义没破。**

L0 走 ChangeAgent 时 `risk=0.258 ≤ 0.3` → `approver=auto-system` 自动批，
**仍然不需要人介入**。「全自动」指「无人介入」，不等于「不过 ChangeAgent」。

让 L0 也过审批的三个理由：

1. **统一闸门**：若「L0 不审、L1/L2 审」，ChangeAgent 要按级别分支；
   统一过后只看风险分，逻辑更简单
2. **防止误判放大**：L0 听着安全（重启 Redis 爆炸半径 0.25），但**会中断在途
   请求**。RCA 误判时（探针给错值），ChangeAgent 是最后一道低成本关卡
3. **审计闭环**：即便 L0 也要留「when / who / risk / why」的记录，
   比「事后补日志」干净

**已知 trade-off**：L0 多走一个节点。若某天希望 L0 真的免审批，需改编排器 DAG
（跳过节点要小心环路），且应有「短链生产环境 RCA 误判率」的数据支撑——
目前没有该数据，故默认不放开。

> 决策已确认采纳（自动审批 + 统一处理）。

### 5.7 README 的「分级自愈 + 安全护栏」与原实现的逐条核对

README 第 348-357 行原文：

```
L0（全自动）：重启 Pod、扩容、限流  ← 爆炸半径 < 5%，直接执行
L1（半自动）：回滚版本、改配置      ← 需要 oncall 确认
L2（人工介入）：数据库操作、全量回滚 ← 需要 TL 审批

安全护栏链：
dry-run 模拟 → 爆炸半径 < 20%？→ 熔断器开着？→ 审批通过？→ 执行
```

把每一句当成「待证明的命题」，对照改动前的实现（`cde5327`）核一遍：

| README 说的 | 原实现 | 阶段 5 后 |
|---|---|---|
| L0 / L1 / L2 三级分级 | `playbook["level"]` 写死的静态字段，`restart_pod`/`scale_up`/`rate_limit` L0，`rollback`/`rollback_config` L1，`heap_dump` L2 | 沿用同一机制（短链动作改用自己的 playbook） |
| L2：数据库操作、全量回滚 | **这两个动作在 playbook 里根本不存在**；唯一标 L2 的是 `heap_dump` | 沿用这一缺口（待补） |
| L0 限流「爆炸半径 < 5%」 | **没有这个判定**。`circuit_breaker` 标 L0 但 `blast_radius_factor = 0.08 > 5%` | 未改（仍为静态级别） |
| 爆炸半径 = 受影响 Pod 数 / 总 Pod 数 | 是 playbook 里硬编码的常量，没从副本数算过 | 未改（`blast_radius_max_percent` 从 0.2 放宽到 0.5） |
| dry-run 模拟 | `_execute_dry_run` 只做字符串 `format()`，恒返回 `"DRY-RUN OK: command validated"`；且 `if playbook_level == L0_AUTO and self._dry_run:` 意味着 `dry_run=True` 才执行（语义反了） | ✅ 改为「True = 不执行」，校验走 DockerExecutor 的真实白名单 |
| 熔断器开着？ | 有 `allow_request()` 检查，但 `record_failure` 只在不返回 SUCCESS 时调用 —— 而 `_execute_remediation` 恒返回 `SUCCESS: ...`，**熔断器永远打不开** | ✅ Execute 阶段真记失败/成功，熔断器会真熔断 |
| 审批通过？→ 执行 | **顺序反了**：DAG 是 `monitor → rca → heal → change`，Heal 先执行、Change 后补审计 | ✅ 拆 `heal_propose` / `heal_execute`，审批夹中间 |
| 第 97-99 行叙事「第 6 步：执行回滚，5 分钟内恢复正常」 | 永不执行 L1/L2（只有 L0 走 `_execute_remediation`），且 `ChangeAgent` 审批通过后直接 `state.status = "resolved"`，**没有任何执行动作发生** | ✅ L1/L2 都不自动执行；`resolved` 只在 L0 + 审批通过 + 执行成功后才置 |

最后两行是这个核对里最要命的：原实现的 `state.status` 在「ChangeAgent 审批通过」那一刻就被标为 `resolved`，而那时**任何 docker / kubectl 都没跑过**。Demo 输出里看到的「已解决」是审批动作写上去的，不是执行成功的结果。

**还有两处没改，知道但刻意先不动**：
- 爆炸半径仍是常量，没有从 `docker ps` / K8s 副本数算；这对单实例的短链短链 compose 够用
- `dry_run` 现在的「校验」是真实白名单校验，没有更深入的「无副作用试跑」（如 `docker inspect` 检查容器存在、目标容器已存在等）

### 5.8 补 §4.7 缺口：依赖「挂起」检测

§4.7 记录的盲区：依赖挂起时请求**既不返回也不报错**，状态码类规则
完全失效。本轮把它补上。

#### 为什么状态码类规则抓不到

Micrometer 的 `http_server_requests_seconds_count` 只在**请求完成时**
递增。请求挂住 → 没完成 → 不计入任何 status：

| 故障形态 | 表现 | `redirect_5xx_rate` |
|---|---|---|
| 快速失败（连接被拒） | 立即 500 | ✅ 触发 |
| 挂起（连接池耗尽 / 对端无响应） | 卡住不返回 | ❌ 恒为 0，失效 |

危险在于：挂起比报错更隐蔽——Tomcat 线程被慢慢占满，而监控一片绿。

#### 新规则：改用「占用类」信号

既然完成计数抓不到，就改用**反映当前占用**的指标——这是请求"卡在半路"时
唯一仍有信号的维度。判据：

> **连接被占住不释放，同时成功跳转吞吐归零**

```promql
(
  (sum(tomcat_connections_current_connections{job="shortlink-app"}) or vector(0)) > 20
  and
  (sum(rate(shortlink_redirect_total{job="shortlink-app"}[2m])) or vector(0)) < 0.05
)
or vector(0)
```

#### 一个必须注意的坑：每个操作数都要 `or vector(0)`

`and` 要求两边**都非空**，任一边返回空则整个表达式返回空（漏报）。
而挂起场景下 `rate(shortlink_redirect_total[2m])` 极容易为空：

- 窗口内不足 2 个采样点 → `rate()` 返回空
- 若跳转从未成功过 → 该计数器序列根本不存在

因此两个操作数各自兜一层 `or vector(0)`，让「没观测到」等价于
「观测值为 0」。**这与 `rca_agent._verify()` 是同一个坑、同一个解法。**

#### RCA 侧：新增 `dependency_hang` 故障模式

| 候选根因 | 先验 | 探针 |
|---|---:|---|
| Redis 连接挂起（读路径阻塞在 Lettuce 连接池等待） | 0.55 | `tomcat_connections_current_connections > 20` |
| MySQL 连接挂起（缓存 miss 回源阻塞） | 0.25 | HikariCP 获取超时 > 0 |
| 真实流量激增导致线程池打满（非依赖故障） | 0.20 | 总请求速率 > 50/s |

### 5.9 阶段 5 修复的实测验证（挂起场景）

故障注入 `docker stop shortlink-redis`，**不重启应用**，让它自然挂起：

```
21:54:30.692 [RCAAgent]         Root cause: Redis 连接挂起 (confidence: 0.99)
21:54:30.693 [HealProposeAgent] 提议: restart_redis [L0] blast_radius=0.25
21:54:30.693 [ChangeAgent]      APPROVED: risk=0.218 approver=auto-system
21:54:36.181 [HealExecuteAgent] 执行结果: SUCCESS | 执行后验证: 已恢复 (healthy)
21:54:36.182 [Orchestrator]     Workflow completed: status=resolved
```

审批 → 执行完成 **5.5 秒**，业务跳转恢复 302。

**这条修复的价值**：此前该故障下监控完全无信号（`rca` 节点直接 skipped），
现在能被精确定位到「Redis 连接挂起」并自动恢复。

### 5.10 演练端点为什么必须自动补探测负载

上面这次能跑通，有个**容易被忽略的前提**：必须有请求进来，连接才会堆积。

第一次把 `dependency_hang` 接进 Dashboard 时，点「故障演练」**没有任何反应**。
排查后是两个叠加的原因：

**一、只停 Redis 不制造负载，故障等于没发生**

`dependency_hang` 的判据是 `(连接数 > 20) and (成功跳转速率 < 0.05/s)`。
实测空闲时 Tomcat 连接数只有 **2**，没人访问就永远跨不过 20 这个阈值：

```
注入后（无流量）：  连接数 = 2    → 规则不触发 → 不告警 → 不自愈 ❌
注入后（50 并发）：连接数 = 52   → 规则触发   → 自愈    ✅
```

**二、探测请求不能用任意 code**

第一版探测打的是 `/drill-probe` 这种不存在的短链，结果：

```
/drill-probe  → http=404  耗时=0.054s   ← 直接 404，根本不查 Redis
/{真实code}   → http=000  耗时=12s     ← 卡在 Redis 读路径上，挂住 ✅
```

请求不存在的短链时应用会走空值/404 分支，**碰不到 Redis**，自然造不出挂起连接。

**三、取 code 必须发生在注入之前**

`_resolve_probe_code()` 靠 `docker exec shortlink-redis redis-cli --scan` 反解真实
code —— 等 Redis 停了就扫不到了。所以顺序是固定的：

```
取 code（Redis 还活着） → docker stop → 用 code 并发探测
```

第一版把取 code 放在 stop 之后，结果 `probe: {requests: 0, skipped: true}`。

**四、并发本身也不能省**

串行请求同时只占 1 个连接，堆不出判据所需的连接数；timeout 还要跨过
Prometheus 的 15s 抓取周期，否则连接在采到之前就释放了。

> 优先级：显式配置 `AIOPS_DRILL_PROBE_CODE` → 自动从 Redis scan。

### 5.11 第二次演练无反应：告警去重窗口

现象：点「故障演练」→ 自愈成功；**紧接着再点一次** → 毫无反应。

根因不在规则，而在 **AlertFingerprint 的 5 分钟去重窗口**。时间线：

```
注入 23:07:52 → 告警 23:08:13 → 自愈 ✅
注入 23:08:55 → 无告警 ❌        ← 距上次告警仅 63 秒，被判定为重复告警
注入 23:23:40 → 告警 23:23:55 → 自愈 ✅   （间隔够）
注入 23:30:16 → 告警 23:30:34 → 自愈 ✅   （间隔 6.5 分钟）
```

去重本身是对的（避免告警风暴），但它假设「同一个 fingerprint 在窗口内
= 同一故障的持续告警」。而演练是**人为注入的新故障事件**，不该被上一次
（演练或真实故障）的窗口挡住。

修复：`Orchestrator.clear_monitor_fingerprints()` → 在 `drill_inject`
执行注入**之前**清空去重缓存。

修复后实测（间隔 2 分钟，远小于 5 分钟窗口）：

```
23:43:31  Alert fired: dependency_hang → 23:43:37 自愈 SUCCESS   第 1 次
23:44:35  告警去重缓存已清空（故障演练）
23:45:37  Alert fired: dependency_hang → 23:45:42 自愈 SUCCESS   第 2 次 ✅
```

### 5.12 流水线初始状态改为「空闲态」

原实现直接把 `get_workflow_status()` 的全局累积节点状态下发给前端，
导致用户一打开页面就看到：

```
●监控告警(已完成) → ○根因分析(已跳过) → ○自愈提议(已跳过) → …
```

这看起来像「刚自愈过一轮」，实际含义却是「本轮监控正常、没发现异常」——
误导。

修复：`build_state()` 先判断最近一轮是否真的触发了告警
（`latest.status != "open"`）：

- **无告警**：`monitor` 显示「监控中」（蓝），后续节点显示「待触发」（灰）
- **有告警**：照常展示真实进度

这样「还没跑过」与「跑过但被跳过」就区分开了。

### 5.13 流水线始终空闲：监控循环的闭环不回写事件历史

反复出现「点了故障演练毫无反应」。前面 §5.10、§5.11 分别在**探测负载**
和**告警去重**上找原因（那些确实是真问题且已修复），但修完仍不显示 ——
真正的断点在数据源：

```
MonitorLoop._run_cycle() 跑出完整闭环（告警 → RCA → 自愈 SUCCESS）
        ↓
incident_history  ← 只有手动端点 /incidents/trigger 会写入
        ↓
监控循环从不回写  →  /api/v1/incidents 恒为 0 条
        ↓
build_state() 判断「最近是否有告警」时永远读到空
        ↓
流水线始终渲染成空闲态
```

换言之：**告警一直在触发、自愈一直在成功**，只是这一切从 API 侧完全
看不见 —— 日志里有 `Alert fired` 和 `SUCCESS`，页面上却毫无动静。

三层修复：

1. `MonitorLoop` 新增 `on_cycle` 回调（可选构造参数），每轮跑完把
   incident 交给 API 层。
2. 回写时机改为**命中告警时立即取节点快照**，而非等循环结束。
   每轮会跑多次工作流（规则层 + 基线层各一遍），最后一遍通常无告警、
   会把 rca 之后的节点重新置为 `SKIPPED`，等循环结束再取快照等于把
   刚跑完的自愈结果冲掉。
3. `build_state()` 优先用闭环当次记录的 `workflow` 快照渲染，
   而不是 orchestrator 的当前状态。

`_snapshot_incident()` 抽为手动触发与监控循环共用。

实测：点一下按钮全程自动，流水线 5 个节点全部「已完成」，
70 秒后仍保持不丢失。

### 5.14 卡片字段全为空 + 演练前未清空上次流水线

**一、当前状态 / 历史事件字段为空**

前端 `renderCards()` / `renderHistory()` 读的是**原始 Agent 对象**的字段名，
但 API 下发的是 `_serialize_*` 简化后的字段，两边对不上，卡片全落成「—」：

| 前端读取 | API 实际下发 | 结果 |
|---|---|---|
| `a.alert_name` / `a.metric_name` | `a.name` / `a.metric` | 空 ❌ |
| `a.metric_value` | `a.value` | 空 ❌ |
| `h.action_type` / `h.heal_level` | `h.action` / `h.level` | 空 ❌ |
| `h.execution_result` | `h.result` | 空 ❌ |
| `c.approval_status` | `c.status` | 空 ❌ |

修复：前端字段名对齐 `_serialize_*` 的实际输出。

另：历史事件表的时间列也是空的 —— `_snapshot_incident()` 压根没下发
`created_at`。补上，并转成**本地时间**：

```
created_at 来自 datetime.utcnow()，是不带时区标记的 UTC。
直接展示会比系统时间早 8 小时（UTC+8），和日志对不上。
```

**二、点击「故障演练」时应先清空上一次的流水线**

每次演练都是一轮**全新**闭环。若不清空，上次的流水线结果会一直挂着，
和新一轮的进度混在一起，看不出这次跑到哪一步了。

修复：新增 `Orchestrator.reset_workflow()`，在 `drill_inject` 执行注入
**之前**把所有节点重置为 pending，并清空 `incident_history`。

实测：

```
点击前：事件数=1  已完成|已完成|已完成|已完成|已完成
点击后：事件数=0  监控中|待触发|待触发|待触发|待触发   ← 立即清空 ✅

闭环完成后卡片字段齐全：
    告警   dependency_hang   critical · 实测 52.0
    根因   Redis 连接挂起…    置信度 0.99
    动作   restart_redis     L0 · 爆炸半径 0.25
    审批   approved          风险 0.218 · auto-system
    执行   SUCCESS: shortlink-redis | 已恢复 (healthy)
时间列 00:56:57 与系统时间 00:58:51 一致（此前差 8 小时）
```

### 5.15 流水线各节点耗时 + 历史事件闭环耗时

**一、历史事件只展示 1 个告警（实际有 2 个）**

`_run_cycle()` 用 `last_state` 累积本轮最后一个「有告警」的 incident，
等循环跑完才统一回写。一轮里命中多个规则时，前面的就被覆盖了。

修复：抽出 `_emit_incident()`，命中告警时**立即**回写，不再攒到末尾。
注释里保留原有的时序修正说明（立刻取快照，避免被后续无告警轮次
覆盖成 `SKIPPED`）。

**二、新增耗时展示**

- 流水线每个节点显示自身耗时
- 事件列表新增「闭环耗时」列：监控告警开始 → 自愈执行结束

两个关键点：

1. **不能用 `agent.get_metrics()` 的 `avg_latency_ms`** —— 那是跨所有
历史调用的累计均值，无法反映本次耗时。改为在 `WorkflowNode` 上记
`started_at` / `ended_at`，由 `Orchestrator.run()` 在节点执行前后打点。
2. **端到端耗时用首尾时间戳相减**，而不是各节点耗时累加 —— 后者会漏掉
节点之间的调度与 I/O 间隔，不是用户感知的真实耗时。

展示上小于 1 秒用 ms、否则用 s：监控只有几十毫秒、自愈执行要好几秒，
量级差太多，统一单位反而不好读。

实测：

```
监控告警 1ms | 根因分析 17ms | 自愈提议 0ms | 变更审批 0ms |
自愈执行 5.58s  →  闭环总耗时 5.60s
```

自愈执行是大头（docker restart + 执行后验证），其余节点都在毫秒级。

### 5.16 演练不再清空历史 + 流水线实时逐个亮起

连续两次故障演练暴露两个问题：

| 问题 | 现象 | 根因 |
|---|---|---|
| ① 第二次演练把数据清零 | 一点「故障演练」，右上角告警数归 0、历史事件表被清空 | `drill_inject` 里 `incident_history.clear()`（此前修「两数对不上」时还误加了 `alerts_fired.clear()`） |
| ② 节点不逐个亮起 | 流水线初始化正常，之后长时间无变化，**等自愈执行跑完（约 5.6s）才一次性显示「已完成」和耗时** | 快照只在节点**结束**时更新；未执行节点残留上轮 `SKIPPED`；Dashboard 又以 `has_alert` 强制空闲态 |

**一、修复 ①：演练只重置流水线，不清历史/告警**

事件历史是「历史」，跨演练累积才能横向对比多次结果。右上角「告警数」
读的是 `monitor_loop.alerts_fired`，而 `alerts_fired` 与 `incident_history`
都在 `state.alert_event is not None` 时各自 +1，**天然 1:1**。清掉任一个，
两个数字就会对不上。因此 `drill_inject` 只保留：

```python
orchestrator.reset_workflow()              # 新一轮闭环从零开始
orchestrator.clear_monitor_fingerprints()  # 避免被 5 分钟去重窗口抑制
# 不再 incident_history.clear() / alerts_fired.clear()
```

**二、修复 ②：闭环执行期间逐节点发布快照**

- `Orchestrator` 新增 `_last_alert_workflow`：只在「有告警」的闭环里更新，
  且**节点开始执行（置 RUNNING）和节点结束（算完耗时）各发一次**。
  全局 `_nodes` 会被无告警的 `run()` 冲成 `SKIPPED`，这份快照不会。
- `run()` 开头先把所有节点重置为 `PENDING`：否则未轮到的节点会带着上一轮
  `SKIPPED`，前端看到「已跳过」而不是「待执行」。
- `build_state()` 取快照的优先级改为 `alert_workflow or active.workflow`——
  实时快照优先，历史事件快照兜底。顺序反了会一直挂着上一轮「已完成」。
  演练 `reset_workflow()` 时清空该快照，新一轮才能从头亮起。
- 空闲态判断由 `if not has_alert` 改为 `if not wf_source`。`has_alert` 取自
  事件历史，而事件要等 5 个节点全跑完才写入 —— 闭环跑的过程中它恒为假，
  进度就被强行覆盖成「监控中 / 待触发」，表现正是「节点不亮」。

实测（连续两次演练，每 3 秒采样 `/dashboard/state`）：

```
第一次  +12s 告警0 事件0 已完成|已完成|已完成|已完成|执行中  ← 自愈执行期间即可见
        +15s 告警0 事件0 已完成|已完成|已完成|已完成|执行中
        +18s 告警1 事件1 已完成|已完成|已完成|已完成|已完成 (5.56s)
第二次  点击前 告警1 事件1 已完成×5                        ← 未清零 ✅
        +15s 告警1 事件1 已完成|已完成|已完成|已完成|执行中
        +18s 告警2 事件2 已完成×5                          ← 1:1 累积 ✅
```

### 5.17 演练只报 anomaly_app_cpu_usage 且卡在根因分析（commit `ae13931`）

点故障演练后，页面冒出统计层告警 `anomaly_app_cpu_usage`，流水线停在
根因分析不再往下走。三个独立原因叠加：

| # | 现象 | 根因 | 修复 |
|---|---|---|---|
| ① | 真正该报的 `dependency_hang` 没触发 | 演练没拿到探测短链码 → 不发探测负载 → 判据（连接堆积 + 成功吞吐归零）不成立 | 取码前先 `_ensure_running()` 拉起 Redis 并等 healthcheck；缓存为空时改为经应用接口新建一条短链 |
| ② | 后面三个节点永远「待执行」 | 被闸门拦下的节点不写快照（SKIPPED 只在无告警轮次出现） | 跳过时也发快照 + 每节点带 `skip_reason`，前端显示原因；未执行节点耗时显示「—」 |
| ③ | 冒出无意义的 CPU 告警 | `process_cpu_usage` 常年 0.2%~0.6% 抖动，方差极小，3-sigma/EWMA 轻易凑够 2 票 | 基线指标加噪声地板 `min_delta`（CPU 5 个百分点 / 线程 5 / lag 500），振幅不足则抑制 |

**① 为什么取不到码**：探测码来自 `docker exec shortlink-redis redis-cli
--scan "sl:link:*"`。上一轮自愈若没跑到（比如只命中了低置信度告警），
Redis 就一直停着，scan 必然失败；而 Redis 被自愈重启后缓存是空的，
同样扫不到 —— 于是「演练点下去毫无动静」。

```
14:04:17 [DRILL] 探测短链码: (未拿到) → 跳过探测负载
14:05:14 Alert fired: anomaly_app_cpu_usage [medium]     ← 只有这个无关告警
14:05:14 Root cause: 消费端批量聚合占用 CPU (confidence: 0.15)
14:05:14 Skipping node: heal_propose / change / heal_execute
docker ps → shortlink-redis 不在（自愈没跑到，Redis 一直停着）
```

**③ 为什么统计层会误报**：统计检测只回答「是否偏离分布」，不回答
「偏离得有没有意义」。CPU 在 0.0017~0.0057 之间抖动也能凑够 2 票，
报出 `[critical/medium]`。故需在投票之外再补一道绝对振幅门槛，且以
近期**中位数**为基线（不会被异常样本自身拉高）。

**顺带修掉一个自己引入的副作用**：`_probe_load()` 在没拿到码时会回退
调用 `_resolve_probe_code()`，而后者会先拉起 Redis —— 故障已注入却把
容器偷偷恢复，探测请求根本不会挂起。改为直接跳过探测负载。

实测：

```
演练 → 探测短链码 9SjNQXLpjr → docker stop shortlink-redis
     → 50 并发探测负载（25s）→ 下一轮 dependency_hang 触发
监控告警 2ms | 根因分析 13ms | 自愈提议 1ms | 变更审批 1ms | 自愈执行 5.60s
告警数 1 = 事件数 1；Redis 恢复 healthy

跳过分支的展示（build_state 离线验证）：
自愈提议 已跳过 — | 根因置信度 0.15 < 0.3，不提议自愈
变更审批 已跳过 — | 没有待审批的自愈方案
自愈执行 已跳过 — | 变更审批未通过，闸门未放行不执行
```

### 5.18 立即巡检不计告警数 + 演练冒出 anomaly_tomcat_busy_threads（commit `81b6743`）

**一、立即巡检的告警不累加到右上角告警数**

按钮打的是 `/api/v1/incidents/trigger` 且**不带 metric_data**，
`MonitorAgent.process()` 于是走 `_generate_demo_alert()`，生成一条
**伪造的** `high_cpu_usage`：

- 它不是真实巡检结果（与 Prometheus、规则层、基线层全无关系）
- 只 `append` 进 `incident_history`，不进 `monitor_loop.alerts_fired`
  → 事件 +1、告警 +0（实测点两次巡检后：事件 7 / 告警 5）

修：

| 改动 | 说明 |
|---|---|
| `MonitorLoop.run_once()` | 手动跑一轮采集，与自动巡检同源（规则层 + 基线层） |
| `POST /api/v1/monitor/check` | 「立即巡检」改走这里，返回本轮告警条数 |
| `trigger` 端点补记 `alerts_fired` | 手动触发也记 `source=manual`，两处计数保持 1:1 |
| `_record_alert` → `record_alert` | 改为公开方法供 API 层调用 |

**二、演练冒出 `anomaly_tomcat_busy_threads`**

它是 `dependency_hang` 的**症状**而不是新故障：50 并发探测请求挂在
Redis 上，Tomcat 忙碌线程必然堆积，统计层把它当成「未知突变」又报一条，
于是同一次演练出现两条告警。

修：规则层告警按服务记时间戳（5 分钟窗口），统计层命中异常时先检查
「该服务是否刚被规则层点名」—— 是则抑制。根因已经在闭环里处理了，
症状不该再刷屏。时序上 `_run_cycle` 先跑规则层、再跑基线层，同轮内生效。

```
对照验证（离线，样本 ≥20）：
  有规则告警在窗口内 → 未告警（症状抑制）
  无规则告警（对照）  → anomaly_tomcat_busy_threads   ← 真实线程池异常仍能捕获

实测：注入 Redis 故障 → 立即巡检 → {"alerts_this_cycle": 1}
      告警数 2 = 事件数 2，告警名 dependency_hang（不再是 high_cpu_usage）
      演练后 60s 仍只有 dependency_hang，无症状噪音
```

### 5.19 点「立即巡检」没有反应（commit `601a47d`）

后端其实跑完了一整轮（规则 5 项 + 基线 3 项，耗时 17~28ms），但系统
健康时是 **0 告警**：历史事件不新增、告警数不变、流水线不变 —— 页面上
没有任何可见变化，只剩一句一闪而过的 toast，用户只会以为按钮坏了。

巡检「一切正常」本身就是结论，必须让用户看得见：

| 改动 | 说明 |
|---|---|
| `run_once()` 返回概况 | 检查的规则/基线项数、本轮告警数、耗时，而非仅一个计数 |
| 最近巡检常驻展示 | API 记下但不进事件历史（不破坏告警数与事件数的 1:1），`/dashboard/state` 下发 `last_check_text` |
| 按钮旁一行提示 | 「最近巡检 16:16:24 · 检查 8 项 · 一切正常 / 告警 N 条」 |
| toast 区分结论 | 「一切正常（无新增告警）」vs「告警 N 条」 |
| `Cache-Control: no-store` | 内联 JS 改过之后，浏览器若用缓存旧页面，按钮仍打旧端点，表现同样是「改了没反应」 |

实测：连点两次巡检 → 「最近巡检 16:16:24 · 检查 8 项 · 一切正常」，
告警数 / 事件数保持 0/0 未被污染。

### 5.20 第二次演练只报 5xx、归因失败、Redis 停机 25 分钟无人处理（commit `c246474`）

第二次点故障演练，只命中 `redirect_5xx_rate`，RCA 输出：

```
探针 不成立: Redis 不可达   实测=0.4762 (lt 0.05)
探针 不成立: MySQL 不可达   实测=0.0000 (gt 0.0)
探针 不成立: 流量激增       实测=1.0191 (gt 20.0)
Root cause: 近期部署引入缺陷 (confidence: 0.10) → 不提议自愈
```

结果 **Redis 真实停机 25 分钟无人处理** —— 告警在响却什么都不做，比多报
一条告警严重得多。三层原因：

| # | 原因 | 修复 |
|---|---|---|
| ① | 带探针的候选全被证伪后，静默落到没有探针、只靠 `base_probability=0.10` 的兜底猜测 | 输出「未能定位根因：所有证据探针均被真实指标证伪（转人工）」，confidence 0.0 |
| ② | `redirect_5xx_rate`（症状）与 `dependency_hang`（根因）平行各开一个闭环 | `AlertRule` 加 `kind`，5xx 标为 symptom，被同服务已有的根因类告警单向抑制 |
| ③ | 探针窗口 `[2m]` 把故障前的健康数据一起平均，「吞吐归零」永不成立 | 窗口收窄到 `[30s]`（与 15s 抓取间隔配合仍有 2 个采样点） |

**为什么单向**：若症状先到就把根因挡住，而症状的 RCA 又常定位不出根因，
故障就真的没人处理了。离线验证：

```
根因先到 → dependency_hang      症状后到 → 未告警（被抑制）
症状先到 → redirect_5xx_rate    根因后到 → dependency_hang（仍触发）
```

**为什么第二次演练换了一条规则命中**：第一次 stop 时连接池里是健康连接，
请求挂起 → `dependency_hang`；自愈重启 Redis 后连接重建，第二次 stop 时
客户端快速失败直接 500 → 连接不堆积 → 只剩 5xx。

**`redirect_5xx_rate` 的语义**：`status=~"5.."` 匹配所有 5 开头状态码，
且不限 uri（实际是全站 5xx，名字比语义窄）；`rate` 是**速率** req/s 不是
占比，阈值 0.5。

**其他组件演练会怎样**：MySQL 会命中（回源失败 500），但 RCA 有
`hikaricp_connections_timeout_total` 探针，大概率正确归因；Kafka **不会**
—— `ClickEventProducer` 是异步 `whenComplete`，失败只 `logger.warn`
（注释「统计丢失可容忍，不能影响跳转」），表现是 `consume_lag` 堆积。

**测试**：`StubProvider` 改为按归一化 PromQL（窗口 → `[W]`）匹配 —— 之前
按字面量匹配，收窄窗口后 6 个用例挂了 4 个；断言的是「给定实测值应得出
哪个根因」，不该被调参影响。D 场景（健康误报）现在正好走到新的转人工分支。

实测：连续两次演练均 `dependency_hang` (confidence 0.99) → `restart_redis`
成功 → Redis 恢复 healthy；告警 2 = 事件 2。

### 5.21 第二次点演练，前 4 个节点仍是上一轮的「已完成」（commit `4b9a425`）

现象：在已完成状态下再次点演练，前 4 个节点保持「已完成」不变，只有
最慢的自愈执行（5~6s）才看得出在动 —— 整体像「流水线显示有延迟」。

根因是快照回退，不是渲染延迟：

```python
# reset_workflow() 末尾
self._last_alert_workflow = []          # ← 置空

# build_state()
wf_source = alert_workflow or (active.workflow if has_alert else None)
```

快照一空就回退到 `active.workflow`（最近事件的快照）—— 那是上一轮
**已跑完**的流水线，于是旧状态一直挂着，直到新一轮闭环跑到某节点才被
覆盖；前 4 个节点是毫秒级，只有自愈执行跑得久，才看得出在动。

修：重置后发布一份**重置之后的初始快照**（全 PENDING）而不是置空，并
清掉 `skip_reason`（避免「根因置信度 0.15 < 0.3」这类备注残留到下一轮）。

实测（在已完成状态下再次演练）：

```
点击后 +3s   待执行|待执行|待执行|待执行|待执行   ← 立即回到初始态
+15s         仍待执行（探测负载期间，告警未触发）
+27s         全已完成（自愈执行 5.58s），Redis 恢复 healthy
```

### 5.22 requirements.txt 里那些包是干什么的，能用它们重写吗

`requirements.txt` 列了 langchain / langgraph / langchain-openai / confluent-kafka /
neo4j / chromadb / prophet / pandas，但当前 Python 实现**一个都没用到**。
它们的构成逻辑是照岗位 JD 列的技术栈（`docs/project-plan.md:47`：「核心要求：
Go/Python + LLM + Agent 框架 + RAG + K8s + 可观测性」），不是运行依赖。

| 包 | 原设计角色 | 实际替代 | 证据 |
|---|---|---|---|
| langgraph | Agent 编排状态机 | 自建 `WorkflowNode`（条件 + 重试 + 熔断 + 检查点） | `core/orchestrator.py:62` |
| langchain-openai | 调 LLM | **完全没接**；`settings.py` 的 `openai_api_key/model/base_url` 三个字段无人读取 | — |
| langchain | Prompt / 工具链封装 | 未使用 | — |
| confluent-kafka | Agent 间事件总线 | `InMemoryEventBus`（`KafkaEventBus` 只是备选） | `core/event_bus.py:46/93` |
| neo4j | 服务依赖拓扑知识图谱 | 硬编码 `SHORTLINK_TOPOLOGY` | `rca_agent.py:45` |
| chromadb | RAG 向量库（历史故障检索） | 未实现（代码无 embedding / 向量检索） | — |
| prophet | 时序预测 / 异常检测 | 自建 `AnomalyDetector`（3-sigma + EWMA + IsolationForest） | `models/time_series.py` |

**为什么不用它们也能跑通**：

1. 编排是线性的（`monitor → rca → heal_propose → change → heal_execute` + 条件
   闸门），没有循环、没有动态分支，用不上"图"。自建状态机 200 行，已覆盖
   LangGraph 的核心语义（节点 / 条件边 / 检查点 / 重试）。
2. 根因是**确定性**的贝叶斯更新 `posterior = likelihood × prior / evidence`，
   先验来自故障模式库、似然来自探针实测，可解释（页面上直接显示
   "探针 X 实测 0.4762 → 证伪"）。
3. 没有任何环节需要生成自然语言或理解非结构化文本，LLM 无处可用。

**能不能用它们重写（成本评估，规模参照：rca 589 行 / 编排 427 行）**：

| 改造 | 代码量 | 风险 | 2C2G 可行性 | 结论 |
|---|---|---|---|---|
| C. LLM 只做表达层（把结论写成人话） | ~50 行 | 几乎为零 | ✅ | 顺手做 |
| B. LLM 只补证据（补探针看不到的信息） | 100–150 行 | 低（失败可降级） | ✅ | **优先做** |
| 编排换 LangGraph | 200–300 行 | 中 | ✅ | 加「方案升级链」或面试需要时做 |
| A. 全 LLM 判根因 | 300–500 行 | 高（误自愈） | ⚠️ 依赖外网 | 不建议 |
| RAG（chromadb） | 300+ 行 | 中 | ❌ 内存不够 | 优先级最低 |

**前提要先说清楚：换成 LLM 并不会让根因判得更准。** 当前故障是**可枚举**的
（Redis / MySQL / 线程池 / 流量激增），每类都有**一手信号**（PromQL 探针），
LLM 的优势在非结构化、不可枚举的场景。前面 §5.20 修的是"探针窗口被稀释"
"判据太严导致证伪"这类**工程问题**，不是"推理不够聪明"。

**A 方案的四个坑**（为什么不建议把贝叶斯换成 LLM）：

| 坑 | 后果 |
|---|---|
| 置信度不可校准 | LLM 说 0.8 ≠ 它 80% 有把握；现有闸门 `confidence >= 0.3` 就自愈，直接塞模型的概率 = 误自愈 |
| 不可复现 | 同样输入两次结论可能不同，误自愈后无法复盘 |
| 延迟 | 现在 RCA 13–27ms，LLM 一次几秒到几十秒 |
| 外部依赖 | 服务器需出外网调 API；无 key 无法运行；每次告警都有成本 |

**B 方案为什么划算**：它正好补上现在的真实短板 ——「近期部署引入缺陷」
这条候选 `base_probability` 只有 0.10 且**没有探针**（只能靠
`has_recent_change` 一个粗糙信号），日志里的异常堆栈更是 PromQL 完全看不到的。

**设计要点**：LLM 输出必须是**结构化证据**（`{候选, 支持/反对, 强度}`），
当作一条 evidence 参与贝叶斯更新，**不能直接当最终概率**；且一手信号（探针）
优先，LLM 只在探针无数据 / 无探针时补位。

**RAG 在 2C2G 上不现实**：chromadb + onnxruntime 磁盘 1GB+，本地 embedding
模型另需几百 MB 内存。更划算的替代是历史故障入 SQLite + 简单统计（"该指标
组合历史上 7 次有 5 次是 Redis"），几十行、零额外内存。

> 另注：`README.md:511` 写「Agent 框架 = LangGraph 状态机」，但 Python 版实际
> 是自建状态机。建议改为「自建状态机（节点 / 条件边 / 检查点，语义对齐
> LangGraph StateGraph）」—— 更准确，且能讲清"为什么不用框架"反而更有说服力。

### 5.23 落地 B 方案：LLM 作为补充证据源（commit `88842ad`）

新增 `python/agents/llm_evidence.py`（232 行），按 §5.22 的 B 方案实现。
**默认关闭**，是可选增强。

**四条设计原则**（决定它不会把系统搞坏）：

| # | 原则 | 落地 |
|---|---|---|
| 1 | LLM 输出的是**结构化证据**，不是置信度 | `{候选: (支持/反对, 强度)}`，作为一条 evidence 参与贝叶斯更新 |
| 2 | **一手信号优先** | 探针已给出结论（成立/证伪）时 LLM 不参与，只在「无探针 / 探针无数据」时补位 |
| 3 | **默认关闭 + 失败静默降级** | 无 key、超时、非 JSON、网络不通 → 返回空证据，闭环完全不受影响 |
| 4 | **零新增依赖** | 标准库 `urllib` 调 OpenAI 兼容 `/chat/completions`，不引入 langchain |

**融合边界**（安全性的关键）：

```python
MAX_SHIFT = 0.4
likelihood = 0.5 ± 0.4 × strength，再夹到 [0.05, 0.9]
```

上限 0.9 与探针「成立」同档、下限 0.05 与「证伪」同档 —— **即便模型强度拉满，
也无法单独把置信度顶过 `>= 0.3` 的自愈闸门**。它只能在一手信号的留白处起作用。

**可解释性**：证据写入 `RCAEvent.evidence`（`type="llm_evidence"`，weight
0.3，刻意低于指标 0.8 与变更记录 0.9），页面能看到它到底影响了哪条候选：

```
[RCAAgent] LLM 证据: 近期部署引入缺陷… 支持(0.60) → 似然 0.50 → 0.74
```

**配置**：复用既有的 `openai_*` 字段（此前无人读取，现在终于有用），新增：

```bash
AIOPS_LLM_EVIDENCE_ENABLED=true      # 默认 false
AIOPS_OPENAI_API_KEY=sk-...
AIOPS_OPENAI_BASE_URL=https://...    # 任何 OpenAI 兼容端点
AIOPS_LLM_TIMEOUT_SECONDS=8          # RCA 在采集循环里同步调用，必须短
```

**调用时机**：每轮 RCA **只调一次**（一次覆盖全部候选，避免 N 次往返），
且只在「无探针 / 探针无数据」的候选上生效。健康巡检不会触发。

**测试 6/6**（离线，桩客户端，不打真实 API）：

```
A 默认关闭不介入        B 调用失败静默降级      C 非 JSON 返回被忽略
D 模型编的候选被挡掉    E 证据被采纳且有上下限
F 探针优先于 LLM（关键）← 探针证伪后，LLM 说支持也不能翻盘
```

F 是核心安全性质：否则模型可以靠一句话让系统去重启容器。

**实测**（启用 + 指向不可达端点，验证降级路径）：

```
[LLMEvidence] 调用失败，降级为无 LLM 证据: <urlopen error [Errno 61] Connection refused>
[RCAAgent] Root cause: Redis 连接挂起…（confidence: 0.99）
演练闭环照常：监控 1ms | 根因 11ms | 自愈执行 5.64s → Redis healthy
```

**当前局限（诚实说明）**：项目还没有日志采集源，所以 `context` 默认为空，
LLM 只能基于告警上下文与候选语义判断 —— 相当于一个语义化的软先验，不是
真正的外部证据。接入日志/变更系统后把文本传进 `context` 即为真实证据，
调用方无需改动。

### 5.24 故障模式库：把运维经验结构化

前几节反复提到「故障模式库 / 候选 / 先验 / 探针」，这里集中解释它是什么。

**一句话**：把老运维脑子里的经验写成数据结构——某个告警来了，通常有哪几种
可能的原因、每种多常见、怎么验证、怎么修。

代码位置 `SHORTLINK_FAULT_PATTERNS`（`rca_agent.py:113`），结构是一个字典：
**告警名 → 候选根因列表**。以 `redirect_5xx_rate` 为例，4 条候选：

| 候选根因 | 先验 | 探针（PromQL 实测） | 建议动作 |
|---|---|---|---|
| Redis 不可达：读路径 `sl:link:{code}` 直接抛异常，无降级逻辑 | **0.45** | 跳转吞吐 `< 0.05`（Redis 挂了跳转全失败） | restart_redis |
| MySQL 不可达：缓存 miss 回源失败（命中路径仍正常） | **0.25** | `hikaricp_connections_timeout_total > 0` | restart_mysql |
| 流量激增超出容量（Tomcat 线程池 / 连接打满） | **0.20** | 总请求量 `> 20/s` | scale_app / rate_limit |
| 近期部署引入缺陷 | **0.10** | **无探针** | rollback / review_change |

每条候选四件套：`root_cause`（人话描述）、`base_probability`（先验）、
`verify`（探针：PromQL + 比较符 + 阈值）、`suggested_actions`。
先验加起来为 1.0。

**一次真实演算**（即 §5.20 之后「置信度 0.42 → 0.81」的来源）：

```
后验 = 似然 × 先验 / 0.5

Redis：探针成立（跳转吞吐归零）      → 似然 0.9  → 0.9 × 0.45 / 0.5 = 0.81  ← 胜出
MySQL：探针不成立（无连接超时）      → 似然 0.05 → 0.05 × 0.25 / 0.5 = 0.025
```

0.81 是结论，但**过程完全可见**——页面上能看到「探针 Redis 不可达
实测=0.4762 (lt 0.05) → 不成立」。这是它跟「LLM 直接判根因」最大的区别。

#### 两个关键设计

**1. 先验只决定「谁更常见」，不决定「这次是谁」**

代码注释 18–19 行明说。0.45 表示「历史上 5xx 十次有四五次是 Redis」，
但**这次是不是**由探针说了算。即便 Redis 先验最高，只要它没挂就被证伪到
0.025。

**2. 探针必须能证伪**

`rca_agent.py:131` 那段注释值得单独摘出来：

> 用「命中请求仍成功」判 MySQL 是不行的：健康状态下它也成立，等于无法证伪，
> 会导致正常状态被误判为 MySQL 故障。连接池获取超时才是真信号。

**一个判据如果在健康时也成立，它就没有任何信息量。** 这是设计探针最容易
踩的坑——§5.20 里探针集体失效是另一个方向的坑（窗口太宽把故障信号平均没了）。

#### 局限（也是下一步的改进方向）

- **人工写死**：26 条候选靠经验沉淀，新故障类型需手动添加
- **先验是拍的**：0.45 / 0.25 是估的，不是从历史数据统计出来的。改进方向是
  让先验从历史故障中学习——即 §5.22 里 RAG 那条路线；但 chromadb 在 2C2G 上
  跑不动，更划算的是 SQLite + 简单统计（「该指标组合历史上 7 次有 5 次是
  Redis」），几十行、零额外内存
- **无探针的候选是盲区**：「近期部署引入缺陷」只能靠 `has_recent_change`
  一个粗糙信号 —— **这正是 §5.23 LLM 补证据要解决的**

#### 面试讲法

> 故障模式库是把运维经验结构化：告警名 → 候选根因 + 先验概率 + PromQL 探针
> + 修复动作。告警来了先按先验排出候选，再用真实指标逐条证实或证伪，最后
> 贝叶斯更新出后验。**先验决定谁更常见，探针决定这次是谁**，整个过程在
> 页面上可解释。

### 5.25 流水线支持起点 / 终点剪裁：这次为什么没上 LangGraph（commit `247879f`）

**起因**：想让流水线支持"指定从哪个节点开始、到哪个节点结束"，于是要能
跳过前面的 / 后面的节点 —— 看起来像 LangGraph 的条件边，于是重新评估了一遍
要不要上框架。

**结论：不上。** 自建状态机加两个参数即可（约 20 行），见
`core/orchestrator.py` 的 `run(start_node=..., end_node=...)`。

先看现状：节点跳过机制**本来就在跑**，`WorkflowNode` 带
`condition` + `skip_reason`，`run()` 里逐节点调 `node.should_execute(state)`。
已有 4 个节点在用：

| 节点 | 跳过条件 | 跳过原因 |
|---|---|---|
| rca | `alert_event is None` | 本轮监控未触发告警 |
| heal_propose | confidence < 0.3 | 置信度不足，不提议自愈 |
| change | `heal_event is None` | 没有待审批的方案 |
| heal_execute | 审批未通过 | 闸门未放行不执行 |

所以"跳跃节点"每天都在发生，只是它是**条件驱动**的。缺的只有"指定起终点"。

#### 实现

```python
async def run(self, initial_state=None, metadata=None,
              start_node: Optional[str] = None,   # 新增
              end_node: Optional[str] = None):    # 新增
```

三个点值得记：

1. **终点之后的节点也要标 SKIPPED**。只 `break` 的话它们停在 `PENDING`，
   Dashboard 上显示成一排"待执行"，看着像流水线卡在终点不动——与 §5.21 踩过的
   "跳过也要发快照"是同一类问题。
2. **节点名不存在时忽略并 warning**。否则拼错一个字就会**静默把整条流水线
   剪空**，表现为"点了没反应"，排查非常费劲。
3. **现有 `condition` 逻辑一行未动**，纯增量。

验证：`python/tests/test_orchestrator_range.py`，5 组用例（起点剪裁 / 终点剪裁
含"不是 PENDING"断言 / 非法节点名 / 不传参回归 / 起终点组合），节点用假 Agent
替换，不依赖 Prometheus 与 docker。

#### 什么时候才真的需要 LangGraph

| 需求 | 自建状态机 | LangGraph |
|---|---|---|
| `add_node` 节点 | ✅ `self._nodes` | 同 |
| 条件跳过 | ✅ `condition` + `skip_reason`，已在用 | `add_conditional_edges` |
| 指定起点 / 终点 | ✅ 本次加的，约 20 行 | `set_entry_point` |
| 重试 / 熔断 | ✅ `max_retries` / `CircuitBreaker` | 同 |
| 检查点 | ⚠️ `_checkpoints`（**内存 dict，重启即丢**） | checkpointer |
| **有环的图**（自愈失败 → 换方案 → 升级人工） | ❌ 线性遍历撑不住 | ✅ |
| **持久化中断恢复**（暂停等审批、重启后续跑） | ❌ | ✅ `interrupt` + checkpointer |

**分界线是最后两行**，不是"起终点"。循环和持久化中断恢复才是 LangGraph 真正
值回票价的地方——线性流水线用框架反而多一层抽象要调试。

#### 面试讲法

> 跳过节点这件事我的状态机本身就支持——每个节点带一个 condition，不满足就
> 标记 SKIPPED 并在 Dashboard 上显示跳过原因，比如"置信度不足，不提议自愈"。
> 至于指定起点和终点，遍历时加一个 started 标志、到终点 break 就行，二十行。
>
> **LangGraph 我评估过，它真正解决的是循环和持久化中断恢复**——如果以后要做
> "自愈失败→换方案→升级人工"的升级链，那是有环的图，我会换过去；现在这条
> 流水线是线性的，用了反而多一层抽象。

#### 一处没做的改动（诚实记录）

原本打算顺手让 `/api/v1/incidents/trigger` 跳过监控，读了代码后发现**不成立**：
该接口传的是 `metric_data`（指标值）而不是告警，`monitor` 节点负责把指标变成
`alert_event`，跳过它就没有告警、RCA 会因条件不满足直接跳过，整条流程空转。

要让 trigger 跳过监控，得先手工构造 `AlertEvent` 塞进 `initial_state`——
那是另一个改动，暂未做。

### 5.26 为什么只监控跳转链路？创建链路的观测缺口与补齐

**问题**：整套告警规则和故障模式库都围绕**跳转**——`redirect_5xx_rate`、
`dependency_hang`、探针查的是跳转吞吐和连接超时。创建链路呢？

#### 一、聚焦跳转是有正当理由的（三条）

**1. 流量差一个数量级**

```
压测 1,739,256 次请求：
  跳转   ≈ 1,525,510 次（1,525,481 hit + 29 miss）  → 约 88%
  创建等 ≈   213,746 次                             → 约 12%
```

跳转是核心读路径，挂掉影响全部用户。

**2. 依赖关系不对称，故障模式也不同**

| | 跳转 | 创建 |
|---|---|---|
| Redis | 读（命中则直接返回） | 写（回填缓存） |
| MySQL | 回源查询（miss 时） | **写**（主表） |
| Kafka | 异步发点击事件 | 不涉及 |
| 失败表现 | **可能挂起不返回** → 无 5xx、监控一片绿 | 同步返回 5xx，客户端立刻知道 |

`dependency_hang` 这条规则专门抓"请求挂住、既不成功也不报错"——这种故障在
创建链路上**根本不会发生**（写失败就是明确的错误响应）。跳转的故障模式更丰富
也更隐蔽，更需要探针去发现。

**3. 最关键：创建故障，自愈帮不上忙**

```
restart_redis      L0  ← 自动执行
restart_mysql      L1  ← 需 oncall 确认
scale_app/rate_limit L2 ← 只提建议
```

创建链路最常见的坏因是 MySQL 写失败，而 `restart_mysql` 是 **L1，系统本来就不
自动执行**。即使给创建做了完整告警和根因定位，最后还是要人处理——**自愈的价值
在这条链路上体现不出来**。而跳转故障对应 `restart_redis` 是 L0，能真正闭环。

#### 二、创建链路并不是裸奔（有间接覆盖）

`redirect_5xx_rate` 用的是：

```
sum(rate(http_server_requests_seconds_count{status=~"5.."}[1m]))
```

`http_server_requests_seconds_count` 是 Spring Boot 的**通用 HTTP 指标，涵盖所有
端点**，创建接口的 5xx 也在里面。而且阈值是**绝对速率 0.5 req/s，不是比率**——
创建接口全挂（比如 10 req/s 的 5xx）会触发，**不会被跳转的巨大流量稀释掉**。

#### 三、真实的缺口在"定位"，不在"发现"

```
创建接口 5xx 升高
  → 触发 redirect_5xx_rate
  → 按告警名匹配故障模式库 → 拿到的是「跳转」那 4 条候选
  → 探针查跳转吞吐、连接超时 → 创建挂了但跳转正常时全部"成立"
  → 落到无探针的兜底候选 → 转人工
```

**好消息**：不会乱重启（探针全证伪会转人工，闸门设计生效了）。

**坏消息**：定位不出真凶。创建链路特有的坏因——雪花 ID 时钟回拨、URL 校验失败、
MySQL 写冲突、Redis 写失败——故障模式库里一条都没有。

#### 四、补齐（已实现）

**① 短链侧加创建链路指标**

`ShortlinkMetrics.createOutcome(outcome)` → `shortlink_create_total{outcome}`，
与跳转同样保持 **outcome 互斥**：

| outcome | 含义 |
|---|---|
| `success` | 创建成功 |
| `invalid_request` | 入参校验失败（URL 前缀 / expireAt），400 |
| `db_error` | MySQL 写入失败，5xx |
| `cache_error` | Redis 回填失败 |

**② AIOps 侧加告警规则**

```
create_5xx_rate   uri="/api/v1/links" 的 5xx 速率 > 0.5/s
kind = root_cause（独立开闭环）   severity = HIGH
```

`redirect_5xx_rate` 保持 `symptom` 不变。

**③ 故障模式库加 4 条创建链路候选**（先验之和 1）

| 候选 | 先验 | 探针 |
|---|---|---|
| MySQL 写入失败（连接池打满） | 0.45 | `hikaricp_connections_pending > 5` |
| Redis 回填失败（创建事务含缓存写） | 0.30 | 命中吞吐 `< 0.05` |
| 创建限流阈值过低（5 次/60s） | 0.15 | `ratelimit_rejected{scene="create"} > 1/s` |
| 雪花 ID 时钟回拨导致主键冲突 | 0.10 | 无探针 |

#### 五、★ 实现时差点踩进去的坑

**故障模式库的 key 必须等于告警名。**

`_get_fault_candidates` 用**子串匹配**：

```python
if pattern_key in normalized or normalized in pattern_key:
```

我最初把 key 命名成语义化的 `create_failure`，而告警名是 `create_5xx_rate`——
两者**互不为子串**，匹配不上，会静默落到 `FALLBACK_FAULT_PATTERNS`。

症状极其隐蔽：**能正常告警，但永远定位不出根因**，看起来像"根因分析能力不行"，
实际是取错了名字。

现有 key 都遵循这个约定（`redirect_5xx_rate` / `shortlink_app_down` /
`consume_lag_backlog` / `ratelimit_reject_burst` 全部与告警名一致）。测试
`test_create_path_monitoring.py` 的 [1] 显式断言 `hits == [告警名]` 来锁死这点。

#### 六、顺带发现的一个设计问题（未改，记录在此）

创建方法带 `@Transactional`，而 **Redis 回填写在这个事务内**：

```java
@Transactional
public LinkResponse create(...) {
    linkRepository.save(link);        // MySQL
    redis.opsForValue().set(...);     // Redis —— 也在事务内
}
```

后果：**Redis 不可达会让整个创建失败并回滚**，哪怕 MySQL 已经写成功。

但从业务上看，创建只需要 MySQL 成功即可——缓存没回填，首次跳转时会 miss 并回源
回填，功能不受影响。**创建不应该把 Redis 当成强依赖。**

埋点如实记为 `cache_error` 而不是掩盖成 `success`。改法是把 Redis 写移出事务
或包 try-catch 降级（只 warn），但这属于行为变更，本次未做。

#### 面试讲法

> 创建链路我确实没做独立的根因定位，这是有意的取舍，也是当时的缺口。
>
> 理由三条：跳转占总请求约 88% 是核心读路径；创建依赖少、失败是同步 5xx，
> 不存在跳转那种"挂起但不报错"的隐蔽故障；最关键的是——**创建故障对应的修复
> 动作是重启 MySQL，那是 L1，系统本来就不自动执行，做了告警也闭环不了**。
>
> 现在创建接口的 5xx 会被通用 HTTP 指标间接捕获（阈值是绝对速率，不会被大流量
> 稀释），但定位会走跳转那套候选，最后大概率落到"未能定位根因"。所以补了三件事：
> 加 `shortlink_create_total{outcome}`、加一条 create 专属规则、给故障模式库补
> 创建特有的候选。
>
> 补的过程中踩了一个坑：故障模式库的 key 必须等于告警名，因为匹配用的是子串。
> 我一开始按语义取名 `create_failure`，结果匹配不上、静默落到兜底候选——
> **能告警但永远定位不出**，很难排查。

---

## 补充改动

### metadata 字段修复（commit `293c516`）

上游 `IncidentState` 未声明 `metadata` 字段，但 `Orchestrator.run(metadata=...)` 第一行就写它、`MonitorAgent.process()` 第一行就读它。实测：

```
赋值 → ValueError: "IncidentState" object has no field "metadata"
读取 → AttributeError: 'IncidentState' object has no attribute 'metadata'
```

**这意味着上游的 metadata 事件通道从未真正工作**——MonitorAgent 的任何调用都会因 AttributeError 进入重试并最终节点失败。阶段 3 的 `metric_data` / `rule_violation` 全部走此通道，故必须补齐。该 commit 的 message 中保留了完整复现证据。

### `.gitignore`（commit `dcc08a0`）

上游仓库无 `.gitignore`，导致 `.DS_Store`、`.idea/`、`java/target/` 等构建 / IDE / OS 杂项全部进入未跟踪列表。补齐：

- `java/target/`、`__pycache__/`、`*.pyc`、`*.class`
- `.idea/`、`*.iml`、`.vscode/`
- `.DS_Store`
- `.env`、`.env.local`、`*.log`

`python/start.sh`（本地启动入口）保持未跟踪，单独处理。

---

## 遗留问题（阶段 5 之后）

阶段 3~5 验证暴露的**逻辑错误**（都不是 bug 而是设计缺口）已全部修复：

| 问题 | 现象 | 根因 | 状态 |
|------|------|------|------|
| RCA 根因判错 | Redis 挂掉导致的 5xx，报「上游服务请求量激增导致 CPU 过载」 | `MOCK_SERVICE_TOPOLOGY` 无 `shortlink-app`；`redirect_5xx_rate` 不匹配任何 `FAULT_PATTERNS` 键 | ✅ 阶段 4 已修复 |
| Heal 动作既不真也不对 | 执行 `kubectl annotate ...`，短链无 K8s，且限流对 Redis 故障无效 | playbook 全为 K8s 命令，执行是假的（直接 return SUCCESS） | ✅ 阶段 5 已修复 |
| 审批不是闸门而是橡皮图章 | ChangeAgent 审批晚于 Heal 执行，且审批通过即置 `resolved` 而实际未执行 | DAG 顺序 heal → change；`ChangeAgent` 里 `approved` 直接写 `resolved` | ✅ 已修复（§5.6，commit `dd79d05`） |
| 依赖「挂起」检测不到 | 请求挂住不产生 5xx，`redirect_5xx_rate` 完全失效；挂起比快速失败更危险 | 规则集只有状态码维度，缺占用类信号 | ✅ 已修复（§5.8 / §5.9） |

### 仍待处理（均为验证中发现、尚未实施）

| 问题 | 影响 | 建议 | 状态 |
|------|------|------|------|
| 监控循环的事件不进 `incident_history` | `/api/v1/incidents` 返回 0 条，自愈做了什么只能翻日志 | MonitorLoop 每轮跑完把 incident 回写到 `incident_history` | ⏳ 未实施 |
| 爆炸半径是硬编码常量 | README 说是「受影响 Pod 数 / 总 Pod 数」，实际是 playbook 里的固定值（详见 §5.7） | 从 `docker ps` / K8s 副本数动态计算 | ⏳ 未实施 |
| L2 的示例动作不存在 | README 说 L2 是「数据库操作、全量回滚」，playbook 里没有这些动作（详见 §5.7） | 按 README 的描述补齐对应 playbook | ⏳ 未实施 |
| AIOps fork 的 README 与实现不符 | fork README 仍写「L0 限流（爆炸半径<5%）」「L2 数据库操作、全量回滚」，但我们已把限流/扩容从 L0 上调为 L2（风险等级提高，改人工处理）且 argv_template=None，爆炸半径仍是硬编码常量 | 更新 fork README 的「分级自愈 + 安全护栏」段，使其与实际实现（阶段 5 后）一致 | ⏳ 未实施（用户同意先放着） |

### 阶段 5 实施时的两个注意点

- 短链 compose 写死了 `container_name`（`shortlink-app` / `shortlink-mysql` / `shortlink-redis` / `shortlink-kafka`），执行器直接用这些名字
- 水平扩容时雪花 ID 的 `SHORTLINK_WORKER_ID` 必须每实例唯一，否则发号碰撞 → `scale_app` 已标 L2 人工，不让 Agent 自动扩

---

## 涉及文件清单（AIOps 侧）

| 路径 | 类型 | 本次改动量 |
|------|------|-----------|
| `python/config/prometheus.yml` | 修改 | +16 |
| `python/collectors/__init__.py` | 新增 | 1 行 |
| `python/collectors/prometheus_collector.py` | 新增 | ~250 行 |
| `python/agents/monitor_agent.py` | 修改 | +74 / -23（阶段 3） |
| `python/agents/rca_agent.py` | 修改 | 阶段 4：拓扑 + 故障模式库 + 证据探针重写；随后修 `or vector(0)` 兜底 |
| `python/agents/heal_agent.py` | 修改 | 阶段 5：重写为 docker 执行器 + 短链 playbook；随后拆 `HealProposeAgent` / `HealExecuteAgent` |
| `python/agents/change_agent.py` | 修改 | 阶段 5：风险模型换短链动作与服务；审批通过不再置 `resolved` |
| `python/core/orchestrator.py` | 修改 | 阶段 4 注入 `evidence_provider`；阶段 5 改为五节点 DAG + 共享熔断器 |
| `python/api/main.py` | 修改 | 阶段 3 接口 +47 / -1；阶段 4 注入；阶段 5 传自愈护栏参数 |
| `python/config/settings.py` | 修改 | `monitor_enabled`；`blast_radius_max_percent` 0.2 → 0.5 |
| `python/models/events.py` | 修改 | 补 `IncidentState.metadata`（阶段 3 硬前提） |
| `python/tests/test_rca_shortlink.py` | 新增 | ~130 行（阶段 4，6 场景离线验证） |
| `python/tests/test_heal_docker.py` | 新增 | ~330 行（阶段 5，18 条：安全护栏 + 动作映射 + 审批闸门） |
| `.gitignore` | 新增 | 12 行 |
