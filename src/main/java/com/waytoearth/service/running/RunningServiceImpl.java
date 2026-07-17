package com.waytoearth.service.running;

import com.waytoearth.dto.request.running.*;
import com.waytoearth.dto.response.common.CursorPageResponse;
import com.waytoearth.dto.response.running.RunningCompleteResponse;
import com.waytoearth.dto.response.running.RunningRecordSummaryResponse;
import com.waytoearth.dto.response.running.RunningStartResponse;
import com.waytoearth.entity.enums.RunningStatus;
import com.waytoearth.entity.enums.RunningType;
import com.waytoearth.entity.running.RunningRecord;
import com.waytoearth.entity.user.User;
import com.waytoearth.exception.*;
import com.waytoearth.repository.running.RunningRecordRepository;
import com.waytoearth.repository.running.RunningRouteRepository;
import com.waytoearth.repository.user.UserRepository;
import com.waytoearth.security.AuthenticatedUser;
import com.waytoearth.service.emblem.EmblemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class RunningServiceImpl implements RunningService {

    private final RunningRecordRepository runningRecordRepository;
    private final RunningRouteRepository runningRouteRepository;
    private final UserRepository userRepository;
    private final EmblemService emblemService;  // 엠블럼 자동 지급
    private final com.waytoearth.service.crew.CrewStatisticsUpdater crewStatisticsUpdater;  // 크루 통계 반영 아웃박스 등록
    private final RedisTemplate<String, Object> redisTemplate;  // 완료된 러닝 상세 캐시 (write-through)

    private static final String RUNNING_DETAIL_CACHE_KEY_PREFIX = "running:detail:";
    private static final Duration RUNNING_DETAIL_CACHE_TTL = Duration.ofDays(60);

    /**
     * 캐시 히트 시 DB를 아예 안 타고 소유자 검증까지 끝내야 하므로, 응답과 함께 소유자 userId를 같이 저장.
     */
    private record RunningDetailCacheEntry(Long userId, RunningCompleteResponse response) {}

    /**
     * 완료된 러닝의 경로 데이터는 이후 절대 수정되지 않으므로(수정 API 없음) 캐시 무효화가 필요 없음.
     * 실패해도 DB가 source of truth이므로 예외를 삼켜 응답 흐름을 막지 않음.
     */
    private void cacheRunningDetail(Long recordId, Long userId, RunningCompleteResponse response) {
        try {
            redisTemplate.opsForValue().set(
                    RUNNING_DETAIL_CACHE_KEY_PREFIX + recordId,
                    new RunningDetailCacheEntry(userId, response),
                    RUNNING_DETAIL_CACHE_TTL
            );
        } catch (Exception e) {
            log.warn("러닝 상세 캐시 적재 실패 (DB는 정상 저장됨): recordId={}", recordId, e);
        }
    }

    private RunningDetailCacheEntry getCachedRunningDetail(Long recordId) {
        try {
            Object cached = redisTemplate.opsForValue().get(RUNNING_DETAIL_CACHE_KEY_PREFIX + recordId);
            return cached instanceof RunningDetailCacheEntry entry ? entry : null;
        } catch (Exception e) {
            log.warn("러닝 상세 캐시 조회 실패, DB로 폴백: recordId={}", recordId, e);
            return null;
        }
    }

    @Override
    public RunningStartResponse startRunning(AuthenticatedUser authUser, RunningStartRequest request) {
        User runner = userRepository.findById(authUser.getUserId())
                .orElseThrow(() -> new UserNotFoundException(authUser.getUserId()));

        RunningRecord record = RunningRecord.builder()
                .sessionId(request.getSessionId())
                .user(runner)
                .runningType(request.getRunningType() != null ? request.getRunningType() : RunningType.SINGLE)
                .status(RunningStatus.RUNNING)
                .startedAt(LocalDateTime.now(ZoneId.of("Asia/Seoul")))
                .isCompleted(false)
                .build();

        runningRecordRepository.save(record);
        return new RunningStartResponse(record.getSessionId(), record.getStartedAt());
    }

    @Override
    public void updateRunning(AuthenticatedUser authUser, RunningUpdateRequest request) {
        if (request.getSessionId() == null || request.getSessionId().isBlank()) {
            throw new InvalidParameterException("sessionId는 필수입니다.");
        }

        RunningRecord record = runningRecordRepository.findBySessionId(request.getSessionId())
                .orElseThrow(() -> new RunningSessionNotFoundException("세션을 찾을 수 없습니다."));

        if (!record.getUser().getId().equals(authUser.getUserId())) {
            throw new UnauthorizedAccessException("해당 세션에 대한 권한이 없습니다.");
        }

        record.setDistance(BigDecimal.valueOf(request.getDistanceMeters() / 1000.0));
        record.setDuration(request.getDurationSeconds());
        record.setAveragePaceSeconds(request.getAveragePaceSeconds());
        record.setCalories(request.getCalories());

        if (request.getCurrentPoint() != null) {
            record.addRoutePoint(
                    request.getCurrentPoint().getLatitude(),
                    request.getCurrentPoint().getLongitude(),
                    request.getCurrentPoint().getSequence()
            );
        }

        runningRecordRepository.save(record);
    }

    @Override
    public void pauseRunning(AuthenticatedUser authUser, RunningPauseResumeRequest request) {
        if (request.getSessionId() == null || request.getSessionId().isBlank()) {
            throw new InvalidParameterException("sessionId는 필수입니다.");
        }

        RunningRecord record = runningRecordRepository.findBySessionIdWithLock(request.getSessionId())
                .orElseThrow(() -> new RunningSessionNotFoundException("세션을 찾을 수 없습니다."));

        if (!record.getUser().getId().equals(authUser.getUserId())) {
            throw new UnauthorizedAccessException("해당 세션에 대한 권한이 없습니다.");
        }

        record.setStatus(RunningStatus.PAUSED);
        runningRecordRepository.save(record);
    }

    @Override
    public void resumeRunning(AuthenticatedUser authUser, RunningPauseResumeRequest request) {
        if (request.getSessionId() == null || request.getSessionId().isBlank()) {
            throw new InvalidParameterException("sessionId는 필수입니다.");
        }

        RunningRecord record = runningRecordRepository.findBySessionIdWithLock(request.getSessionId())
                .orElseThrow(() -> new RunningSessionNotFoundException("세션을 찾을 수 없습니다."));

        if (!record.getUser().getId().equals(authUser.getUserId())) {
            throw new UnauthorizedAccessException("해당 세션에 대한 권한이 없습니다.");
        }

        record.setStatus(RunningStatus.RUNNING);
        runningRecordRepository.save(record);
    }

    /**
     * 러닝 완료
     *
     * 캐시 무효화: 러닝 완료 시 사용자의 통계가 업데이트되므로 캐시 삭제
     * - userInfo: 총 거리, 러닝 횟수 변경
     * - userSummary: 통계 정보 변경
     */
    @Override
    @CacheEvict(value = {"userInfo", "userSummary"}, key = "#authUser.userId")
    public RunningCompleteResponse completeRunning(AuthenticatedUser authUser, RunningCompleteRequest request) {
        if (request.getSessionId() == null || request.getSessionId().isBlank()) {
            throw new InvalidParameterException("sessionId는 필수입니다.");
        }

        RunningRecord record = runningRecordRepository.findBySessionIdWithLock(request.getSessionId())
                .orElseThrow(() -> new RunningSessionNotFoundException("세션을 찾을 수 없습니다."));

        if (!record.getUser().getId().equals(authUser.getUserId())) {
            throw new UnauthorizedAccessException("해당 세션에 대한 권한이 없습니다.");
        }

        BigDecimal distanceKm = BigDecimal.valueOf(request.getDistanceMeters() / 1000.0);

        record.complete(
                distanceKm,
                request.getDurationSeconds(),
                request.getAveragePaceSeconds(),
                request.getCalories(),
                request.getAverageHeartRate(),
                request.getMaxHeartRate(),
                LocalDateTime.now(ZoneId.of("Asia/Seoul"))
        );

        if (request.getRoutePoints() != null) {
            request.getRoutePoints().forEach(p ->
                    record.addRoutePoint(
                            p.getLatitude(),
                            p.getLongitude(),
                            p.getSequence(),
                            p.getTimestampSeconds(),
                            p.getHeartRate(),
                            p.getPaceSeconds(),
                            p.getAltitude(),
                            p.getAccuracy(),
                            p.getCumulativeDistanceMeters()
                    )
            );
        }

        RunningRecord savedRecord = runningRecordRepository.save(record);
        runningRecordRepository.flush();

        // 동시성 안전한 원자적 통계 업데이트
        userRepository.updateRunningStatsAtomic(authUser.getUserId(), distanceKm);

        User user = record.getUser();

        // 크루 통계 반영은 아웃박스에 등록만 하고 실제 반영은 트랜잭션 밖(AFTER_COMMIT)에서 처리.
        // 등록(INSERT)만 하는 거라 경합이 없어 핵심 트랜잭션 안에 있어도 안전함.
        try {
            crewStatisticsUpdater.enqueueCrewStatisticsUpdate(
                    user.getId(),
                    distanceKm.doubleValue(),
                    request.getDurationSeconds()
            );
        } catch (Exception e) {
            // 아웃박스 등록 자체가 실패해도 로깅만 하고 러닝 완료는 유지
            log.error("크루 통계 반영 요청 등록 실패: userId={}, error={}",
                    user.getId(), e.getMessage());
        }

        var awardResult = emblemService.scanAndAward(user.getId(), "DISTANCE");

        RunningCompleteResponse response = RunningCompleteResponse.builder()
                .runningRecordId(savedRecord.getId())
                .title(savedRecord.getTitle())
                .totalDistanceKm(savedRecord.getDistance() != null ? savedRecord.getDistance().doubleValue() : 0.0)
                .averagePace(formatPace(savedRecord.getAveragePaceSeconds()))
                .durationSeconds(savedRecord.getDuration())
                .calories(savedRecord.getCalories())
                .averageHeartRate(savedRecord.getAverageHeartRate())
                .maxHeartRate(savedRecord.getMaxHeartRate())
                .startedAt(savedRecord.getStartedAt() != null ? savedRecord.getStartedAt().toString() : null)
                .endedAt(savedRecord.getEndedAt() != null ? savedRecord.getEndedAt().toString() : null)
                .routePoints(savedRecord.getRoutes().stream()
                        .map(rt -> RunningCompleteResponse.RoutePoint.builder()
                                .latitude(rt.getLatitude())
                                .longitude(rt.getLongitude())
                                .sequence(rt.getSequence())
                                .timestampSeconds(rt.getTimestampSeconds())
                                .heartRate(rt.getHeartRate())
                                .paceSeconds(rt.getPaceSeconds())
                                .altitude(rt.getAltitude())
                                .accuracy(rt.getAccuracy())
                                .cumulativeDistanceMeters(rt.getCumulativeDistanceMeters())
                                .build()
                        )
                        .collect(Collectors.toList()))
                .routePointsCount(savedRecord.getRoutes().size())
                .emblemAwardResult(awardResult)
                .runningType(savedRecord.getRunningType().name())
                .build();

        // 완료 시점에 이미 메모리에 있는 응답을 그대로 캐시에 얹음 (추가 DB 조회 없음, D 단계)
        cacheRunningDetail(savedRecord.getId(), user.getId(), response);

        return response;
    }

    @Override
    public void updateTitle(AuthenticatedUser authUser, Long recordId, RunningTitleUpdateRequest request) {
        RunningRecord record = runningRecordRepository.findById(recordId)
                .orElseThrow(() -> new RunningRecordNotFoundException(recordId));

        if (!record.getUser().getId().equals(authUser.getUserId())) {
            throw new UnauthorizedAccessException("해당 기록에 대한 권한이 없습니다.");
        }

        record.setTitle(request.getTitle());
        runningRecordRepository.save(record);
    }

    @Override
    @Transactional(readOnly = true)
    public RunningCompleteResponse getDetail(AuthenticatedUser authUser, Long recordId) {
        RunningDetailCacheEntry cached = getCachedRunningDetail(recordId);
        if (cached != null) {
            if (!cached.userId().equals(authUser.getUserId())) {
                throw new UnauthorizedAccessException("해당 기록에 대한 권한이 없습니다.");
            }
            return cached.response();
        }

        RunningRecord r = runningRecordRepository.findWithRoutesById(recordId)
                .orElseThrow(() -> new RunningRecordNotFoundException(recordId));

        if (!r.getUser().getId().equals(authUser.getUserId())) {
            throw new UnauthorizedAccessException("해당 기록에 대한 권한이 없습니다.");
        }

        RunningCompleteResponse response = RunningCompleteResponse.builder()
                .runningRecordId(r.getId())
                .title(r.getTitle())
                .totalDistanceKm(r.getDistance() != null ? r.getDistance().doubleValue() : 0.0)
                .averagePace(formatPace(r.getAveragePaceSeconds()))
                .durationSeconds(r.getDuration())
                .calories(r.getCalories())
                .averageHeartRate(r.getAverageHeartRate())
                .maxHeartRate(r.getMaxHeartRate())
                .startedAt(r.getStartedAt() != null ? r.getStartedAt().toString() : null)
                .endedAt(r.getEndedAt() != null ? r.getEndedAt().toString() : null)
                .routePoints(
                        r.getRoutes().stream()
                                .map(rt -> RunningCompleteResponse.RoutePoint.builder()
                                        .latitude(rt.getLatitude())
                                        .longitude(rt.getLongitude())
                                        .sequence(rt.getSequence())
                                        .timestampSeconds(rt.getTimestampSeconds())
                                        .heartRate(rt.getHeartRate())
                                        .paceSeconds(rt.getPaceSeconds())
                                        .altitude(rt.getAltitude())
                                        .accuracy(rt.getAccuracy())
                                        .cumulativeDistanceMeters(rt.getCumulativeDistanceMeters())
                                        .build()
                                )
                                .collect(Collectors.toList())
                )
                .routePointsCount(r.getRoutes().size())
                .emblemAwardResult(null)
                .runningType(r.getRunningType().name())
                .build();

        cacheRunningDetail(r.getId(), r.getUser().getId(), response);

        return response;
    }

    private String formatPace(Integer paceSeconds) {
        if (paceSeconds == null) return "00:00";
        int m = paceSeconds / 60;
        int s = paceSeconds % 60;
        return String.format("%02d:%02d", m, s);
    }

    @Transactional(readOnly = true)
    @Override
    public Page<RunningRecordSummaryResponse> getRecords(AuthenticatedUser authUser, Pageable pageable) {
        User user = userRepository.findById(authUser.getUserId())
                .orElseThrow(() -> new UserNotFoundException(authUser.getUserId()));

        Page<RunningRecord> pageResult = runningRecordRepository
                .findByUserAndIsCompletedTrueOrderByStartedAtDesc(user, pageable);

        return pageResult.map(r ->
                new RunningRecordSummaryResponse(
                        r.getId(),
                        r.getTitle(),
                        r.getDistance() != null ? r.getDistance().doubleValue() : 0.0,
                        r.getDuration() != null ? r.getDuration() : 0,
                        formatPace(r.getAveragePaceSeconds()),
                        r.getCalories() != null ? r.getCalories() : 0,
                        r.getStartedAt() != null ? r.getStartedAt().toString() : null,
                        r.getRunningType().name()
                )
        );
    }

    @Override
    public CursorPageResponse<RunningRecordSummaryResponse> getRecordsByCursor(
            AuthenticatedUser authUser, Long cursor, int size) {

        User user = userRepository.findById(authUser.getUserId())
                .orElseThrow(() -> new UserNotFoundException(authUser.getUserId()));

        // size + 1개 조회하여 hasNext 판단
        Pageable pageable = PageRequest.of(0, size + 1);
        List<RunningRecord> records;

        if (cursor == null) {
            // 첫 페이지: 최신 데이터부터
            records = runningRecordRepository.findTopNByUserOrderByIdDesc(user, pageable);
        } else {
            // 다음 페이지: cursor 이후 데이터
            records = runningRecordRepository.findNextPageByUserAndCursor(user, cursor, pageable);
        }

        // hasNext 판단
        boolean hasNext = records.size() > size;
        if (hasNext) {
            records = records.subList(0, size);
        }

        // nextCursor 계산
        Long nextCursor = hasNext && !records.isEmpty()
                ? records.get(records.size() - 1).getId()
                : null;

        // DTO 변환
        List<RunningRecordSummaryResponse> content = records.stream()
                .map(r -> new RunningRecordSummaryResponse(
                        r.getId(),
                        r.getTitle(),
                        r.getDistance() != null ? r.getDistance().doubleValue() : 0.0,
                        r.getDuration() != null ? r.getDuration() : 0,
                        formatPace(r.getAveragePaceSeconds()),
                        r.getCalories() != null ? r.getCalories() : 0,
                        r.getStartedAt() != null ? r.getStartedAt().toString() : null,
                        r.getRunningType().name()
                ))
                .collect(Collectors.toList());

        return CursorPageResponse.of(content, nextCursor, hasNext);
    }

}

