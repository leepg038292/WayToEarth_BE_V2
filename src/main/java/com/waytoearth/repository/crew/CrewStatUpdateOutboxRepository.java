package com.waytoearth.repository.crew;

import com.waytoearth.entity.crew.CrewStatUpdateOutbox;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface CrewStatUpdateOutboxRepository extends JpaRepository<CrewStatUpdateOutbox, Long> {

    /**
     * 처리 대상 하나를 배타적으로 잠가서 가져옴 (여러 인스턴스가 동시에 스윕해도 같은 row를 중복 처리 안 함).
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM CrewStatUpdateOutbox o WHERE o.id = :id")
    Optional<CrewStatUpdateOutbox> findByIdForUpdate(@Param("id") Long id);

    /**
     * 안전망 스케줄러용 — 일정 시간 이상 PENDING 상태로 남아있는 것들(=빠른 경로에서 처리 안 된 것)
     */
    @Query("SELECT o.id FROM CrewStatUpdateOutbox o " +
           "WHERE o.status = :status " +
           "AND o.createdAt < :olderThan " +
           "AND o.attemptCount < :maxAttempts " +
           "ORDER BY o.createdAt ASC")
    List<Long> findStalePendingIds(@Param("status") CrewStatUpdateOutbox.OutboxStatus status,
                                    @Param("olderThan") LocalDateTime olderThan,
                                    @Param("maxAttempts") int maxAttempts,
                                    Pageable pageable);
}
