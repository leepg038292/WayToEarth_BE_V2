package com.waytoearth.entity.crew;

import com.waytoearth.entity.common.BaseTimeEntity;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;

/**
 * 크루 통계 반영 대기열(트랜잭셔널 아웃박스).
 * completeRunning의 핵심 트랜잭션과 함께 커밋되어, 앱이 죽어도 이 row는 반드시 남는다.
 * 별도의 비동기 리스너(빠른 경로)와 스케줄러(안전망)가 이 테이블을 소비한다.
 */
@Entity
@Table(name = "crew_stat_update_outbox")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CrewStatUpdateOutbox extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "crew_id", nullable = false)
    private Long crewId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "month", nullable = false, length = 6)
    private String month;

    @Column(name = "distance_km", nullable = false, precision = 10, scale = 2)
    private BigDecimal distanceKm;

    @Column(name = "duration_seconds", nullable = false)
    private Long durationSeconds;

    @Column(name = "is_new_active_member", nullable = false)
    private Boolean isNewActiveMember;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(name = "attempt_count", nullable = false)
    @Builder.Default
    private Integer attemptCount = 0;

    public enum OutboxStatus {
        PENDING, FAILED
    }
}
