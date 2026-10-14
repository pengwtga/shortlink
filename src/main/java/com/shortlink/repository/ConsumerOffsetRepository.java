package com.shortlink.repository;

import com.shortlink.entity.ConsumerOffset;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface ConsumerOffsetRepository extends JpaRepository<ConsumerOffset, Long> {

    /** 分区分配时用来自 DB 的位点 seek，覆盖 Kafka 自身的已提交位点 */
    Optional<ConsumerOffset> findByGroupIdAndTopicAndPartitionNo(String groupId, String topic, int partitionNo);

    /**
     * 推进位点。必须与业务数据写入处于同一事务：
     * 事务提交则「数据 + 位点」一起生效，事务回滚则一起撤销，从而不重复计数。
     */
    @Modifying
    @Query(value = """
            INSERT INTO t_consumer_offset (group_id, topic, partition_no, next_offset)
            VALUES (:groupId, :topic, :partitionNo, :nextOffset)
            ON DUPLICATE KEY UPDATE next_offset = VALUES(next_offset),
                                    updated_at = CURRENT_TIMESTAMP
            """, nativeQuery = true)
    void upsertOffset(@Param("groupId") String groupId,
                      @Param("topic") String topic,
                      @Param("partitionNo") int partitionNo,
                      @Param("nextOffset") long nextOffset);
}
