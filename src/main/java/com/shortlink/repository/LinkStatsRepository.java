package com.shortlink.repository;

import com.shortlink.entity.LinkStatsDaily;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;

public interface LinkStatsRepository extends JpaRepository<LinkStatsDaily, Long> {

    List<LinkStatsDaily> findByCodeAndStatDateBetweenOrderByStatDateAsc(
            String code, LocalDate start, LocalDate end);

    /**
     * 历史总点击。作为「总点击」的真相源，避免依赖 Redis 里永不过期的计数器
     * （那种 key 在内存压力下会被淘汰，导致总量静默归零）。
     */
    @Query("SELECT COALESCE(SUM(s.clicks), 0) FROM LinkStatsDaily s WHERE s.code = :code")
    long sumClicksByCode(@Param("code") String code);

    /** 原子累加，避免「查-改-写」的并发丢失更新 */
    @Modifying
    @Query(value = """
            INSERT INTO t_link_stats_daily (code, stat_date, clicks)
            VALUES (:code, :statDate, :clicks)
            ON DUPLICATE KEY UPDATE clicks = clicks + VALUES(clicks)
            """, nativeQuery = true)
    void upsertClicks(@Param("code") String code,
                      @Param("statDate") LocalDate statDate,
                      @Param("clicks") long clicks);
}
