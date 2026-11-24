package com.waytoearth.service.crew;

import com.waytoearth.entity.crew.CrewStatUpdateOutbox;
import com.waytoearth.repository.crew.CrewStatUpdateOutboxRepository;
import com.waytoearth.repository.crew.CrewStatisticsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

/**
 * 아웃박스 항목 하나를 실제로 크루 통계에 반영하는 공통 로직.
 * 빠른 경로(RunningCompletedEventListener, 비동기)와 안전망(CrewStatOutboxSweepJob, 스케줄러)이
 * 둘 다 이 컴포넌트를 공유한다 — 로직이 하나뿐이라 두 경로가 서로 다르게 동작할 위험이 없음.
 *
 * processOne()은 "통계 반영 + outbox 삭제"를 하나의 트랜잭션으로 묶음 — 중간에 실패하면
 * 전부 롤백되어(반쪽 상태 없음) outbox row가 그대로 PENDING으로 남고, 다음 시도 때 처음부터
 * 다시 적용됨(멱등). 실패 기록(재시도 횟수 증가)은 반드시 별도의 새 트랜잭션(recordFailure)에서
 * 하는데, 안 그러면 실패한 트랜잭션 안에서 같이 롤백돼서 "실패했다는 기록조차" 안 남기 때문.
 *
 * 두 메서드를 한 클래스 안에서 this.processOne() / this.recordFailure()로 서로 부르지 않는 이유:
 * 그러면 Spring AOP 프록시를 안 거쳐서 @Transactional이 무시됨(self-invocation 문제).
 * 그래서 호출 순서 조율은 반드시 이 클래스 "바깥"(리스너, 스케줄러)에서 함.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CrewStatOutboxProcessor {

    private static final int MAX_ATTEMPTS = 5;

    private final CrewStatUpdateOutboxRepository outboxRepository;
    private final CrewStatisticsRepository crewStatisticsRepository;
    private final CrewStatisticsService crewStatisticsService;

    /**
     * @return 이번 호출로 실제 처리가 일어났으면 true, 이미 다른 곳에서 먼저 처리해서 없어졌으면 false
     * @throws RuntimeException 반영 도중 실패 시 그대로 던짐(트랜잭션 전체 롤백 목적) — 호출자가 잡아서
     *         recordFailure()를 별도 트랜잭션으로 호출해야 함
     */
    @Transactional
    public boolean processOne(Long outboxId) {
        Optional<CrewStatUpdateOutbox> found = outboxRepository.findByIdForUpdate(outboxId);
        if (found.isEmpty()) {
            return false; // 이미 처리되어 삭제됨 (빠른 경로와 안전망이 동시에 집었을 때 나중 쪽)
        }
        CrewStatUpdateOutbox outbox = found.get();

        applyToStatistics(outbox);
        outboxRepository.delete(outbox);
        return true;
    }

    /**
     * processOne()이 실패했을 때 호출. 반드시 별도의 새 트랜잭션으로 실행되어야
     * 실패한 트랜잭션의 롤백에 이 기록마저 같이 휩쓸려가지 않음.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordFailure(Long outboxId, Exception cause) {
        outboxRepository.findById(outboxId).ifPresent(outbox -> {
            int attempts = outbox.getAttemptCount() + 1;
            outbox.setAttemptCount(attempts);
            if (attempts >= MAX_ATTEMPTS) {
                outbox.setStatus(CrewStatUpdateOutbox.OutboxStatus.FAILED);
                log.error("크루 통계 반영 최종 실패 ({}회 시도, outboxId={}): {}",
                        attempts, outboxId, cause.getMessage(), cause);
            } else {
                log.warn("크루 통계 반영 실패, 재시도 예정 (outboxId={}, attempt={}): {}",
                        outboxId, attempts, cause.getMessage());
            }
            outboxRepository.save(outbox);
        });
    }

    private void applyToStatistics(CrewStatUpdateOutbox outbox) {
        BigDecimal distanceKm = outbox.getDistanceKm();

        if (distanceKm.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("거리가 0 이하라 크루 통계 반영을 건너뜁니다. crewId={}, userId={}, distance={}",
                    outbox.getCrewId(), outbox.getUserId(), distanceKm);
            return;
        }

        BigDecimal paceSeconds = BigDecimal.valueOf(outbox.getDurationSeconds())
                .divide(distanceKm, 2, RoundingMode.HALF_UP);

        crewStatisticsService.getOrCreateMonthlyStatistics(outbox.getCrewId(), outbox.getMonth());

        crewStatisticsRepository.updateStatisticsAndPaceAtomically(
                outbox.getCrewId(), outbox.getMonth(), 1, distanceKm,
                paceSeconds, outbox.getIsNewActiveMember());

        crewStatisticsService.updateMvpForMonth(outbox.getCrewId(), outbox.getMonth());

        // Redis 랭킹 실시간 업데이트 — 외부(다른 빈)에서 호출하는 거라 @Retryable이 정상 작동함
        crewStatisticsService.updateRedisRankingAfterRun(outbox.getCrewId(), outbox.getUserId(), outbox.getMonth());

        log.info("크루 통계가 반영되었습니다. crewId={}, userId={}, month={}, distance={}",
                outbox.getCrewId(), outbox.getUserId(), outbox.getMonth(), distanceKm);
    }
}
