package com.waytoearth.service.crew;

import com.waytoearth.dto.response.crew.CrewStatisticsSummaryDto;
import com.waytoearth.dto.response.crew.CrewMemberRankingDto;
import com.waytoearth.dto.response.crew.CrewRankingDto;
import com.waytoearth.entity.crew.CrewEntity;
import com.waytoearth.entity.crew.CrewStatisticsEntity;
import com.waytoearth.entity.user.User;
import com.waytoearth.repository.crew.CrewRepository;
import com.waytoearth.repository.crew.CrewStatisticsRepository;
import com.waytoearth.repository.crew.CrewMemberRepository;
import com.waytoearth.repository.user.UserRepository;
import com.waytoearth.service.ranking.CrewRankingService;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
@Slf4j
public class CrewStatisticsServiceImpl implements CrewStatisticsService {

    private final CrewStatisticsRepository statisticsRepository;
    private final CrewRepository crewRepository;
    private final CrewMemberRepository crewMemberRepository;
    private final UserRepository userRepository;
    private final CrewRankingService crewRankingService;
    private final EntityManager entityManager;

    /**
     * "조회해서 없으면 생성"은 동시에 여러 트랜잭션이 같은 크루의 첫 통계 row를 만들려고 하면
     * 전부 "없다"고 보고 각자 INSERT를 시도하는 경쟁 상태(TOCTOU race)가 생김 — 실측 부하테스트에서
     * "Query did not return a unique result: 2 results were returned" 예외로 실제 발생 확인됨.
     * (crew_id, month) 유니크 제약(DB 레벨)으로 중복 저장 자체를 막고, 그 제약 위반이 발생하면
     * "누군가 나보다 먼저 만들었다"는 뜻이므로 그 row를 다시 조회해서 반환함.
     */
    @Override
    @Transactional
    public CrewStatisticsEntity getOrCreateMonthlyStatistics(Long crewId, String month) {
        CrewEntity crew = getCrewEntity(crewId);

        Optional<CrewStatisticsEntity> existing = statisticsRepository.findByCrewAndMonth(crew, month);

        if (existing.isPresent()) {
            return existing.get();
        }

        long activeMemberCount = crewMemberRepository.countByCrew(crew);

        CrewStatisticsEntity newStats = CrewStatisticsEntity.builder()
                .crew(crew)
                .month(month)
                .activeMembers((int) activeMemberCount)
                .build();

        try {
            CrewStatisticsEntity saved = statisticsRepository.saveAndFlush(newStats);
            log.info("새로운 월간 통계가 생성되었습니다. crewId: {}, month: {}", crewId, month);
            return saved;
        } catch (DataIntegrityViolationException e) {
            // 유니크 제약 위반 = 동시에 다른 트랜잭션이 먼저 만듦. 실패한 엔티티를 영속성 컨텍스트에서
            // 떼어내고, 먼저 만들어진 진짜 row를 다시 조회해서 반환.
            entityManager.detach(newStats);
            log.debug("월간 통계 생성 경합 발생, 기존 row 재조회. crewId: {}, month: {}", crewId, month);
            return statisticsRepository.findByCrewAndMonth(crew, month)
                    .orElseThrow(() -> e);
        }
    }

    @Override
    public Optional<CrewStatisticsEntity> getMonthlyStatistics(Long crewId, String month) {
        CrewEntity crew = getCrewEntity(crewId);
        return statisticsRepository.findByCrewAndMonth(crew, month);
    }

    @Override
    public List<CrewStatisticsEntity> getCrewMonthlyStatistics(Long crewId) {
        CrewEntity crew = getCrewEntity(crewId);
        return statisticsRepository.findByCrewOrderByMonthDesc(crew);
    }

    @Override
    public List<CrewStatisticsEntity> getCrewStatisticsByPeriod(Long crewId, String startMonth, String endMonth) {
        CrewEntity crew = getCrewEntity(crewId);
        return statisticsRepository.findByCrewAndMonthBetweenOrderByMonth(crew, startMonth, endMonth);
    }

    @Override
    @Transactional
    public void updateMvpForMonth(Long crewId, String month) {
        // 현재 MVP 조회
        CrewMemberRankingDto currentMvp = statisticsRepository.findMvpInCrew(crewId, month);

        if (currentMvp != null) {
            User mvpUser = userRepository.findById(currentMvp.getUserId())
                    .orElseThrow(() -> new RuntimeException("MVP 사용자를 찾을 수 없습니다. userId: " + currentMvp.getUserId()));

            // 원자적 UPDATE — JPA 엔티티를 읽고-mutate-flush 하지 않고 직접 갱신 (동시 처리 중인
            // 다른 outbox 항목의 통계 UPDATE와 서로 낙관적 락으로 충돌하지 않도록)
            statisticsRepository.updateMvpUser(crewId, month, mvpUser, currentMvp.getTotalDistance());

            log.info("월간 MVP가 갱신되었습니다. crewId: {}, month: {}, mvpUserId: {}",
                    crewId, month, currentMvp.getUserId());
        }
    }

    @Override
    public List<CrewRankingDto> getCrewRankingByDistance(String month, int limit) {
        // Redis 우선 조회! 고성능 랭킹 시스템
        return crewRankingService.getCrewRanking(month, limit);
    }

    @Override
    public List<CrewStatisticsSummaryDto> getCrewRankingByRunCount(String month, int limit) {
        List<CrewStatisticsEntity> topCrews = statisticsRepository.findTopCrewsByRunCount(month, limit);
        return topCrews.stream()
                .map(stats -> new CrewStatisticsSummaryDto(
                        stats.getMonth(),
                        1, // 크루 수
                        stats.getTotalDistance().doubleValue(),
                        stats.getActiveMembers(),
                        stats.getAvgPaceSeconds() != null ? stats.getAvgPaceSeconds().doubleValue() : 0.0
                ))
                .toList();
    }

    @Override
    public List<CrewStatisticsSummaryDto> getCrewRankingByGrowth(String currentMonth, String previousMonth, int limit) {
        // 현재는 월간 누적 거리 랭킹만 사용, 성장률 랭킹은 향후 필요시 구현
        return Collections.emptyList();
    }

    @Override
    public Optional<CrewStatisticsSummaryDto> getCrewMonthlySummary(Long crewId, String month) {
        Optional<CrewStatisticsEntity> statsOpt = getMonthlyStatistics(crewId, month);

        if (statsOpt.isPresent()) {
            CrewStatisticsEntity stats = statsOpt.get();
            return Optional.of(new CrewStatisticsSummaryDto(
                    stats.getMonth(),
                    1, // 크루 수
                    stats.getTotalDistance().doubleValue(),
                    stats.getActiveMembers(),
                    stats.getAvgPaceSeconds() != null ? stats.getAvgPaceSeconds().doubleValue() : 0.0
            ));
        }

        return Optional.empty();
    }

    @Override
    @Transactional
    public void resetStatisticsForNewMonth(Long crewId, String newMonth) {
        CrewEntity crew = getCrewEntity(crewId);

        // 기존 통계가 있는지 확인
        if (!statisticsRepository.existsByCrewAndMonth(crew, newMonth)) {
            // 새 달 통계 생성
            getOrCreateMonthlyStatistics(crewId, newMonth);

            log.info("새 달 통계가 초기화되었습니다. crewId: {}, month: {}", crewId, newMonth);
        }
    }

    @Override
    @Transactional
    public void cleanupStatisticsForCrew(Long crewId) {
        statisticsRepository.deleteAllByCrewId(crewId);
        log.info("크루 삭제에 따른 통계 데이터가 물리 삭제되었습니다. crewId: {}", crewId);
    }

    /**
     * 동시성 안전한 크루 멤버 수 증가/감소
     */
    @Override
    @Transactional
    @Retryable(value = {OptimisticLockingFailureException.class}, maxAttempts = 3, backoff = @Backoff(delay = 50))
    public boolean updateCrewMemberCountSafe(Long crewId, int delta) {
        int updateCount = statisticsRepository.updateCurrentMembersAtomically(crewId, delta);

        if (updateCount == 0) {
            log.warn("크루 멤버 수 원자적 업데이트 실패 - crewId: {}, delta: {}", crewId, delta);
            return false; // 정원 초과나 0 미만이 되려고 했음
        }

        log.debug("크루 멤버 수 안전 업데이트 완료 - crewId: {}, delta: {}", crewId, delta);
        return true;
    }

    // Private helper methods
    private CrewEntity getCrewEntity(Long crewId) {
        return crewRepository.findById(crewId)
                .orElseThrow(() -> new RuntimeException("크루를 찾을 수 없습니다. crewId: " + crewId));
    }

    @Override
    public List<CrewMemberRankingDto> getMemberRankingInCrew(Long crewId, String month, int limit) {
        // Redis 우선 조회! 고성능 랭킹 시스템
        return crewRankingService.getMemberRankingInCrew(crewId, month, limit);
    }

    @Override
    public Optional<CrewMemberRankingDto> getMvpInCrew(Long crewId, String month) {
        CrewMemberRankingDto mvp = statisticsRepository.findMvpInCrew(crewId, month);
        return Optional.ofNullable(mvp);
    }

    /**
     * 러닝 완료 후 Redis 랭킹 실시간 업데이트
     * 동시성 문제 해결: DB 조회 대신 Redis ZINCRBY 사용하여 원자적 증가
     * 재시도 로직: Redis 실패 시 최대 3번 재시도
     * public + 외부(CrewStatOutboxProcessor)에서 호출 — 같은 클래스 내부 호출(self-invocation)이면
     * @Retryable이 프록시를 못 거쳐 무시되므로, 반드시 다른 빈에서 호출해야 재시도가 실제로 동작함
     */
    @Override
    @Retryable(
        value = {Exception.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 200, multiplier = 2)
    )
    public void updateRedisRankingAfterRun(Long crewId, Long userId, String month) {
        try {
            // 1. DB에서 최신 거리 조회 (Redis와 동기화)
            BigDecimal userTotalDistance = getUserMonthlyTotalDistance(crewId, userId, month);
            BigDecimal crewTotalDistance = getCrewMonthlyTotalDistance(crewId, month);

            // 2. Redis에 거리 업데이트 (ZADD)
            // 동시성 이슈가 있지만, DB가 source of truth이므로 주기적으로 동기화
            crewRankingService.updateMemberRanking(crewId, userId, month, userTotalDistance.doubleValue());
            crewRankingService.updateCrewRanking(crewId, month, crewTotalDistance.doubleValue());

            // 3. Redis에 러닝 횟수 증가 (원자적 증가)
            crewRankingService.incrementMemberRunCount(crewId, userId, month);
            crewRankingService.incrementCrewRunCount(crewId, month);

            log.debug("Redis 랭킹 업데이트 완료. crewId: {}, userId: {}, month: {}, userDistance: {}, crewDistance: {}",
                    crewId, userId, month, userTotalDistance, crewTotalDistance);

        } catch (Exception e) {
            log.error("Redis 랭킹 업데이트 실패 (재시도 후에도 실패). crewId: {}, userId: {}, month: {}, error: {}",
                    crewId, userId, month, e.getMessage(), e);
            // Redis 오류가 메인 로직을 방해하지 않도록 예외를 먹음
            // 실패해도 DB에는 저장되어 있으므로, 다음 배치 동기화나 랭킹 조회 시 DB에서 캐싱됨
        }
    }

    /**
     * 사용자의 월간 누적 거리 조회 (N+1 쿼리 최적화)
     */
    private BigDecimal getUserMonthlyTotalDistance(Long crewId, Long userId, String month) {
        return statisticsRepository.findUserMonthlyDistanceInCrew(crewId, userId, month);
    }

    /**
     * 크루의 월간 전체 거리 조회
     */
    private BigDecimal getCrewMonthlyTotalDistance(Long crewId, String month) {
        CrewStatisticsEntity stats = getMonthlyStatistics(crewId, month).orElse(null);
        return stats != null ? stats.getTotalDistance() : BigDecimal.ZERO;
    }
}