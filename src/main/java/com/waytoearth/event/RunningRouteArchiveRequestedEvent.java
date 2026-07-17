package com.waytoearth.event;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 러닝 완료(핵심 저장 커밋) 후, 좌표를 running_route_archive에 복제 요청하기 위한 이벤트.
 * JPA 엔티티를 그대로 실으면 영속성 컨텍스트/트랜잭션 경계를 넘나드는 문제가 생길 수 있어,
 * 필요한 스칼라 값만 미리 꺼낸 순수 데이터(PointSnapshot)로 전달한다.
 */
public record RunningRouteArchiveRequestedEvent(Long runningRecordId, List<PointSnapshot> points) {

    public record PointSnapshot(
            Integer sequence,
            Double latitude,
            Double longitude,
            Integer timestampSeconds,
            Integer heartRate,
            Integer paceSeconds,
            Double altitude,
            Double accuracy,
            Integer cumulativeDistanceMeters,
            LocalDateTime createdAt,
            LocalDateTime updatedAt
    ) {
    }
}
