package com.shortlink.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/**
 * Kafka 消费位点（按 消费组 + topic + 分区 唯一）。
 * 存在 MySQL 而非依赖 Kafka 自身位点，是为了能和业务数据在同一个事务里提交。
 */
@Entity
@Table(name = "t_consumer_offset")
public class ConsumerOffset {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "group_id", nullable = false, length = 128)
    private String groupId;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Column(name = "partition_no", nullable = false)
    private int partitionNo;

    @Column(name = "next_offset", nullable = false)
    private long nextOffset;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt = LocalDateTime.now();

    public Long getId() {
        return id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    public void setPartitionNo(int partitionNo) {
        this.partitionNo = partitionNo;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
