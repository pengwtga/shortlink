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
| fix | `IncidentState.metadata` 字段 | `293c516` | 修复上游 metadata 通道不可用的硬 bug |
| chore | `.gitignore` | `dcc08a0` | 排除构建产物 / IDE / OS 杂项 |

六个 commit 已推送到 fork：`git@github.com:pengwtga/multi-agent-aiops.git`；`upstream` 仍指向 `bcefghj/multi-agent-aiops`，便于后续同步上游。

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

这说明规则集缺一类**基于延迟 / 吞吐**的判据，例如：

- 成功吞吐骤降：`rate(shortlink_redirect_total[2m])` 长期为 0，而总请求速率不为 0
- p99 延迟：基于 `http_server_requests_seconds` histogram 的 quantile
- 线程池占用率持续高位：`tomcat_threads_busy_threads / tomcat_threads_config_max_threads`

此项留待后续阶段（不在阶段 4 范围内）。

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

## 遗留问题 → 阶段 5

阶段 3 验证暴露的两处**逻辑错误**（都不是 bug 而是设计缺口），RCA 那处已在阶段 4 修复，剩下 Heal 的执行问题：

| 问题 | 现象 | 根因 | 状态 |
|------|------|------|------|
| RCA 根因判错 | Redis 挂掉导致的 5xx，报「上游服务请求量激增导致 CPU 过载」 | `MOCK_SERVICE_TOPOLOGY` 无 `shortlink-app`；`redirect_5xx_rate` 不匹配任何 `FAULT_PATTERNS` 键 | ✅ 阶段 4 已修复 |
| Heal 动作既不真也不对 | 执行 `kubectl annotate svc/shortlink-app rate-limit=100 -n production`——短链是 docker compose 部署，没有 k8s；且根因若是 Redis 不可达，该拉起 Redis 而非限流 | playbook 全为 k8s 命令，执行是假的（直接 return SUCCESS） | ⏳ 阶段 5：换 docker 执行器 + 真实动作映射 |
| 依赖「挂起」检测不到 | 请求挂住不产生 5xx，`redirect_5xx_rate` 规则完全失效（详见 §4.7） | 规则集只有状态码维度，缺延迟 / 吞吐维度 | ⏳ 后续：补延迟或成功吞吐骤降类规则 |

### 一个预期内的中间状态

阶段 4 之后，RCA 会输出 `restart_redis` / `restart_mysql` / `check_dependency` / `restart_consumer` 等短链专属动作，而这些在 HealAgent 的 k8s playbook 里并不存在。此时 HealAgent 会**优雅降级**：记 `No playbook for action` 并跳过，不崩溃——表现为「根因判对了，但没有自愈动作」。这正是阶段 5 要补的部分：用 docker 执行器（`docker start shortlink-redis` 等）替换 k8s playbook。

阶段 5 需注意两点：短链 compose 写死了 `container_name`（`shortlink-app` 等）；以及水平扩容时雪花 ID 的 `SHORTLINK_WORKER_ID` 必须每实例唯一，否则发号碰撞——因此 `scale_app` 应标 L2 人工，不让 Agent 自动扩。

---

## 涉及文件清单（AIOps 侧）

| 路径 | 类型 | 本次改动量 |
|------|------|-----------|
| `python/config/prometheus.yml` | 修改 | +16 |
| `python/collectors/__init__.py` | 新增 | 1 行 |
| `python/collectors/prometheus_collector.py` | 新增 | ~250 行 |
| `python/agents/monitor_agent.py` | 修改 | +74 / -23（阶段 3） |
| `python/agents/rca_agent.py` | 修改 | 拓扑 + 故障模式库 + 证据探针重写（阶段 4） |
| `python/core/orchestrator.py` | 修改 | +3（注入 `evidence_provider`） |
| `python/api/main.py` | 修改 | +47 / -1（阶段 3）、+3（阶段 4 注入） |
| `python/config/settings.py` | 修改 | +1 |
| `python/models/events.py` | 修改 | +3 |
| `python/tests/test_rca_shortlink.py` | 新增 | ~120 行（6 场景离线验证） |
| `.gitignore` | 新增 | 12 行 |
