package com.waytoearth.service.crew;

import com.waytoearth.entity.crew.CrewStatUpdateOutbox;
import com.waytoearth.repository.crew.CrewStatUpdateOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 안전망: 빠른 경로(RunningCompletedEventListener)가 어떤 이유로든(앱 재시작, 스레드풀 소진,
 * 예외 등) 처리를 못 한 outbox 항목을 주기적으로 훑어서 대신 처리함.
 * 빠른 경로가 정상 작동하는 한 이 잡이 실제로 처리할 항목은 거의 없어야 정상 —
 * 여기서 계속 뭔가 처리된다면 빠른 경로에 문제가 있다는 신호로 보고 조사할 것.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CrewStatOutboxSweepJob {

    private static final int STALE_AFTER_SECONDS = 15;
    private static final int BATCH_SIZE = 50;
    private static final int MAX_ATTEMPTS = 5;

    private final CrewStatUpdateOutboxRepository outboxRepository;
    private final CrewStatOutboxProcessor outboxProcessor;

    @Scheduled(fixedDelay = 30_000)
    public void sweep() {
        LocalDateTime olderThan = LocalDateTime.now().minusSeconds(STALE_AFTER_SECONDS);

        List<Long> staleIds = outboxRepository.findStalePendingIds(
                CrewStatUpdateOutbox.OutboxStatus.PENDING, olderThan, MAX_ATTEMPTS,
                PageRequest.of(0, BATCH_SIZE));

        if (staleIds.isEmpty()) {
            return;
        }

        log.warn("크루 통계 안전망 스윕 — 빠른 경로에서 처리 안 된 항목 {}건 발견", staleIds.size());

        for (Long id : staleIds) {
            try {
                outboxProcessor.processOne(id);
            } catch (Exception e) {
                outboxProcessor.recordFailure(id, e);
            }
        }
    }
}
