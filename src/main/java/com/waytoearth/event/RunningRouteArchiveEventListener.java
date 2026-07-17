package com.waytoearth.event;

import com.waytoearth.config.async.AsyncConfig;
import com.waytoearth.service.running.RunningRouteArchiveWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 러닝 완료(핵심 저장 커밋) 직후, 좌표를 running_route_archive로 비동기 복제한다.
 *
 * - AFTER_COMMIT: 핵심 트랜잭션이 커밋 성공했을 때만 실행됨(롤백되면 아예 실행 안 됨)
 * - @Async: 별도 스레드에서 실행되어 completeRunning의 응답 지연에 영향을 주지 않음
 * - 크루 통계(아웃박스+스윕)와 달리 재시도 안전망이 없음 — 이 처리는 각 사용자의 독립적인 row를
 *   추가하는 것뿐이라 경합/유실 걱정이 없고, 실패해도 getDetail이 running_route로 폴백하므로
 *   1회 시도만 하고 실패 시 로그만 남긴다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RunningRouteArchiveEventListener {

    private final RunningRouteArchiveWriter archiveWriter;

    @Async(AsyncConfig.ROUTE_ARCHIVE_EXECUTOR)
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onRunningRouteArchiveRequested(RunningRouteArchiveRequestedEvent event) {
        try {
            archiveWriter.archive(event.runningRecordId(), event.points());
        } catch (Exception e) {
            log.warn("러닝 경로 아카이브 복제 실패 (running_route로 폴백됨): runningRecordId={}",
                    event.runningRecordId(), e);
        }
    }
}
