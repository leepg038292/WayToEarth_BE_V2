package com.waytoearth.service.crew;

import com.waytoearth.entity.crew.CrewMemberEntity;
import com.waytoearth.entity.crew.CrewStatUpdateOutbox;
import com.waytoearth.event.RunningCompletedEvent;
import com.waytoearth.repository.crew.CrewMemberRepository;
import com.waytoearth.repository.crew.CrewStatUpdateOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * 러닝 완료 시 사용자가 속한 크루마다 "통계 반영해야 할 일"을 아웃박스에 등록함.
 * 실제 반영(CrewStatOutboxProcessor)은 여기서 안 하고, 이 메서드가 하는 건 그냥 INSERT뿐이라
 * 경합/지연 위험이 없어서 completeRunning의 핵심 트랜잭션 안에 그대로 둬도 안전함.
 * (이게 바로 아웃박스 패턴의 핵심 — "할 일"을 핵심 저장과 같은 트랜잭션에 커밋해두면,
 *  그 이후 무슨 일이 생기든 이 일감 자체는 절대 사라지지 않음)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CrewStatisticsUpdater {

    private final CrewMemberRepository crewMemberRepository;
    private final CrewStatUpdateOutboxRepository outboxRepository;
    private final ApplicationEventPublisher eventPublisher;

    public void enqueueCrewStatisticsUpdate(Long userId, Double distanceKm, Integer durationSeconds) {
        List<CrewMemberEntity> memberships = crewMemberRepository.findByUserIdWithCrew(userId);
        if (memberships.isEmpty()) {
            return; // 크루 미가입 사용자는 스킵
        }

        String month = LocalDateTime.now(ZoneId.of("Asia/Seoul"))
                .format(DateTimeFormatter.ofPattern("yyyyMM"));

        for (CrewMemberEntity membership : memberships) {
            CrewStatUpdateOutbox outbox = outboxRepository.save(CrewStatUpdateOutbox.builder()
                    .crewId(membership.getCrew().getId())
                    .userId(userId)
                    .month(month)
                    .distanceKm(BigDecimal.valueOf(distanceKm))
                    .durationSeconds(durationSeconds.longValue())
                    .isNewActiveMember(false) // 기존 updateStatisticsAfterRun과 동일하게 유지
                    .build());

            // 커밋 이후(AFTER_COMMIT) 빠른 경로가 이 outbox를 바로 처리 시도함.
            // 실패해도 CrewStatOutboxSweepJob이 안전망으로 나중에 재시도하므로 데이터는 절대 안 사라짐.
            eventPublisher.publishEvent(new RunningCompletedEvent(outbox.getId()));

            log.debug("크루 통계 반영 요청을 등록했습니다. crewId={}, userId={}, month={}",
                    membership.getCrew().getId(), userId, month);
        }
    }
}
