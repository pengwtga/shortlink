-- 短链映射表
CREATE TABLE IF NOT EXISTS t_link (
    id         BIGINT       NOT NULL COMMENT '雪花ID，同时是短码的来源',
    code       VARCHAR(16)  NOT NULL COMMENT 'Base62 短码',
    long_url   VARCHAR(2048) NOT NULL COMMENT '原始长链接',
    status     TINYINT      NOT NULL DEFAULT 1 COMMENT '1-有效 0-禁用',
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expire_at  DATETIME     NULL COMMENT '过期时间，NULL 表示永久',
    PRIMARY KEY (id),
    UNIQUE KEY uk_code (code),
    KEY idx_created_at (created_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '短链映射';

-- 点击日统计表（Kafka 消费端批量聚合写入）
CREATE TABLE IF NOT EXISTS t_link_stats_daily (
    id        BIGINT  NOT NULL AUTO_INCREMENT,
    code      VARCHAR(16) NOT NULL,
    stat_date DATE    NOT NULL,
    clicks    BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_code_date (code, stat_date),
    KEY idx_stat_date (stat_date)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '短链日统计';

-- Kafka 消费位点表：与统计累加在「同一事务」提交，实现不重复计数。
-- 为什么不用 Kafka 自身的位点？因为「写库成功」与「提交位点」之间存在窗口，
-- 崩溃/重平衡后该批次会被重放，而累加型 UPSERT 不具备幂等性。
CREATE TABLE IF NOT EXISTS t_consumer_offset (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    group_id     VARCHAR(128) NOT NULL COMMENT '消费组',
    topic        VARCHAR(128) NOT NULL,
    partition_no INT          NOT NULL COMMENT '分区号',
    next_offset  BIGINT       NOT NULL COMMENT '下一条待消费位点 = 已处理位点 + 1',
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_group_topic_partition (group_id, topic, partition_no)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT 'Kafka 消费位点';
