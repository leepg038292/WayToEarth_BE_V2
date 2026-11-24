package com.waytoearth.event;

import com.waytoearth.config.async.AsyncConfig;
import com.waytoearth.service.crew.CrewStatOutboxProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 러닝 완료(핵심 저장 커밋) 직후, 크루 통계 반영을 시도하는 "빠른 경로".
 *
 * - AFTER_COMMIT: 핵심 트랜잭션이 커밋 성공했을 때만 실행됨(롤백되면 아예 실행 안 됨)
 * - @Async: 별도 스레드에서 실행되어, 이 처리를 기다리지 않고 completeRunning의 HTTP 응답이 먼저 나감
 * - 이게 실패해도(스레드/앱이 죽어도) CrewStatOutboxSweepJob이 안전망으로 나중에 다시 처리함 —
 *   그래서 여기서는 실패해도 재시도를 여러 번 붙이지 않고 딱 한 번만 시도하고 넘어감
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RunningCompletedEventListener {

    private final CrewStatOutboxProcessor outboxProcessor;

    @Async(AsyncConfig.CREW_STAT_EXECUTOR)
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onRunningCompleted(RunningCompletedEvent event) {
        try {
            boolean processed = outboxProcessor.processOne(event.outboxId());
            if (processed) {
                log.debug("크루 통계 빠른 경로 처리 완료: outboxId={}", event.outboxId());
            }
        } catch (Exception e) {
            // 여기서 실패해도 괜찮음 — CrewStatOutboxSweepJob이 안전망으로 나중에 재시도함
            outboxProcessor.recordFailure(event.outboxId(), e);
        }
    }
}
